# pyre-strict
import logging
import os
import tempfile
import json
import copy
import docker
from datetime import datetime
from requests.exceptions import ReadTimeout
from abc import ABC, abstractmethod
from typing import NamedTuple, Tuple, List, Dict, Optional, Iterable
import WDL

"""
Task lifecycle

Preamble: runtime attributes will have been evaluated and implemented somehow

Load AST and inputs/outputs
Provision execution working directory, and 'localize' input files
- Calculate 1:1 mapping for the outside-docker:inside-docker path of each input file
- Usually project each input file into one directory inside docker
- If basenames collide, error out for now; in the future we can put one or the other
  into a temp subdirectory, with a heuristic that any other file with the same path
  plus suffix is also placed in that subdirectory.
- Modularize this container logic so one could theoretically implement on Singularity etc.

Evaluate pre-command declaration DAG
- File-to-String coercions will yield inside-docker paths
- Invocations of size(), read_* are permitted only on File inputs (no String coercions pls)
- Invocations of write_* will have something added to the mapping
- glob is forbidden/undefined?
Evaluate command interpolations to formulate command script
Execute script with all necessary instrumentation, manipulating docker-py as needed
Evaluate output declarations
Upload output files if necessary (note, files may be embedded inside compound-type outputs)
Transmit outputs


How to handle task termination? https://stackoverflow.com/questions/18499497/how-to-process-sigterm-signal-gracefully
"docker run" seems to ignore sigterm. if you sigkill it, container stays alive. 
http://blog.bordage.pro/avoid-docker-py/
docker-py Container.wait has a timeout. so, we could trap sigint and sigterm and use docker-py to clean up
Orrrr we could expose our own wait & timeout...pros&cons there.
"""


# InputError
# CommandError
# Terminated
# OutputError


class CommandError(WDL.Error.RuntimeError):
    pass


class Terminated(WDL.Error.RuntimeError):
    pass


class OutputError(WDL.Error.RuntimeError):
    pass


class TaskContainer(ABC):
    """
    Base class for task containers, subclassed by runtime-specific
    implementations (e.g. Docker).
    """

    task_id: str

    host_dir: str
    """
    :type: str

    The host path to the scratch directory that will be mounted inside the
    container.
    """

    container_dir: str
    """
    :type: str

    The scratch directory's mounted path inside the container. The task
    command's working directory will be ``{container_dir}/work/``.
    """

    input_file_map: Dict[str, str]
    """
    :type: Dict[str,str]

    A mapping of host input file paths to in-container mounted paths,
    maintained by ``add_files``.
    """

    _running: bool
    _terminate: bool

    def __init__(self, task_id: str, host_dir: str) -> None:
        self.task_id = task_id
        self.host_dir = host_dir
        self.container_dir = "/mnt/miniwdl_task_run"
        self.input_file_map = {}
        self._running = False
        self._terminate = False

    def add_files(self, host_files: List[str]) -> None:
        """
        Use before running the container to add a list of host files to mount
        inside the container as inputs. The host-to-container path mapping is
        maintained in ``input_file_map``.

        Although ``add_files`` can be used multiple times, files should be
        added together where possible, as this allows heuristics for dealing
        with any name collisions among them.
        """
        assert not self._running
        ans = {}
        basenames = {}
        for fn in host_files:
            if fn not in self.input_file_map:
                bn = os.path.basename(fn)
                basenames[bn] = 1 + basenames.get(bn, 0)
                ans[fn] = os.path.join(self.container_dir, "inputs", bn)

        # Error out if any input filenames collide.
        # TODO: assort them into separate subdirectories, also with a heuristic
        # grouping together files that come from the same host directory.
        collisions = [bn for bn, ct in basenames.items() if ct > 1]
        if collisions:
            raise WDL.Error.InputError("input filename collision(s): " + " ".join(collisions))

        for k, v in ans.items():
            assert k not in self.input_file_map
            self.input_file_map[k] = v

    def run(self, logger: logging.Logger, command: str) -> None:
        """
        1. Container is instantiated
        2. Command is executed in ``{host_dir}/work/`` (where {host_dir} is mounted to {container_dir} inside the container)
        3. Standard output is written to ``{host_dir}/stdout.txt``
        4. Standard error is written to ``{host_dir}/stderr.txt`` and logged at INFO level
        5. Raises exception for nonzero exit code or any other error

        The container is torn down in any case, including SIGTERM/SIGHUP signal which is trapped.
        """
        assert not self._running
        self._running = True
        # container-specific logic should be in _run(). this wrapper traps SIGTERM/SIGHUP
        # and sets self._terminate
        return self._run(logger, command)

    @abstractmethod
    def _run(self, logger: logging.Logger, command: str) -> None:
        raise NotImplementedError()

    def host_file(self, container_file: str) -> str:
        """
        Map an in-container path under ``container_dir`` to a host path.
        """
        if os.path.isabs(container_file):
            dpfx = os.path.join(self.container_dir, "work") + "/"
            if not container_file.startswith(dpfx):
                raise OutputError("task attempted to output a file outside its working directory")
            container_file = container_file[len(dpfx) :]
        return os.path.join(self.host_dir, "work", container_file)


class TaskDockerContainer(TaskContainer):
    image_tag: str

    def __init__(self, task_id: str, host_dir: str) -> None:
        super().__init__(task_id, host_dir)
        self.image_tag = "ubuntu:18.04"

    def _run(self, logger: logging.Logger, command: str) -> None:
        with open(os.path.join(self.host_dir, "command"), "x") as outfile:
            outfile.write(command)
        pipe_files = ["stdout.txt", "stderr.txt"]
        for touch_file in pipe_files:
            with open(os.path.join(self.host_dir, touch_file), "x") as outfile:
                pass

        volumes = {}
        # mount input files and command read-only
        for host_path, container_path in self.input_file_map.items():
            volumes[host_path] = {"bind": container_path, "mode": "ro"}
        volumes[os.path.join(self.host_dir, "command")] = {
            "bind": os.path.join(self.container_dir, "command"),
            "mode": "ro",
        }
        # mount stdout, stderr, and working directory read/write
        for pipe_file in pipe_files:
            volumes[os.path.join(self.host_dir, pipe_file)] = {
                "bind": os.path.join(self.container_dir, pipe_file),
                "mode": "rw",
            }
        volumes[os.path.join(self.host_dir, "work")] = {
            "bind": os.path.join(self.container_dir, "work"),
            "mode": "rw",
        }
        logger.debug("docker volume map: " + str(volumes))

        # connect to dockerd
        client = docker.from_env()
        try:
            container = None
            exit_info = None

            try:
                # run container
                logger.info("docker starting image {}".format(self.image_tag))
                container = client.containers.run(
                    self.image_tag,
                    command=[
                        "/bin/bash",
                        "-c",
                        "/bin/bash ../command >> ../stdout.txt 2>> ../stderr.txt",
                    ],
                    detach=True,
                    auto_remove=True,
                    working_dir=os.path.join(self.container_dir, "work"),
                    volumes=volumes,
                )
                logger.debug(
                    "docker container name = {}, id = {}".format(container.name, container.id)
                )

                # long-poll for container exit
                while exit_info is None:
                    try:
                        exit_info = container.wait(timeout=1)
                    except Exception as exn:
                        # TODO: tail stderr.txt into logger
                        if self._terminate:
                            raise Terminated() from None
                        # workaround for docker-py not throwing the exception class
                        # it's supposed to
                        s_exn = str(exn)
                        if "Read timed out" not in s_exn and "Timeout" not in s_exn:
                            raise
                logger.info("container exit info = " + str(exit_info))
            except:
                # make sure to stop & clean up the container if we're stopping due
                # to SIGTERM or something. Most other cases should be handled by
                # auto_remove.
                if container:
                    try:
                        container.remove(force=True)
                    except Exception as exn:
                        logger.error("failed to remove docker container: " + str(exn))
                raise

            # retrieve and check container exit status
            assert exit_info
            if "StatusCode" not in exit_info:
                raise CommandError(
                    "docker finished without reporting exit status in: " + str(exit_info)
                )
            if exit_info["StatusCode"] != 0:
                raise CommandError("command exit status = " + str(exit_info["StatusCode"]))
        finally:
            try:
                client.close()
            except:
                logger.error("failed to close docker-py client")


def run_local_task(
    task: WDL.Task,
    posix_inputs: WDL.Env.Values,
    task_id: Optional[str] = None,
    parent_dir: Optional[str] = None,
) -> Tuple[str, WDL.Env.Values]:
    """
    Execute a task locally.

    Inputs shall have been typechecked already.

    File inputs are presumed to be locally-accessible POSIX file paths that can be mounted into a container
    """

    parent_dir = parent_dir or os.getcwd()

    # formulate task ID & provision local directory
    if task_id:
        run_dir = os.path.join(parent_dir, task_id)
        os.makedirs(run_dir, exist_ok=False)
    else:
        now = datetime.today()
        task_id = now.strftime("%Y%m%d_%H%M%S") + "_" + task.name
        try:
            run_dir = os.path.join(parent_dir, task_id)
            os.makedirs(run_dir, exist_ok=False)
        except FileExistsError:
            task_id = now.strftime("%Y%m%d_%H%M%S_") + str(now.microsecond) + "_" + task.name
            run_dir = os.path.join(parent_dir, task_id)
            os.makedirs(run_dir, exist_ok=False)

    # provision logger
    logger = logging.getLogger("miniwdl_task:" + task_id)
    logger.info("starting task")
    logger.debug("task run directory " + run_dir)

    # create appropriate TaskContainer
    container = TaskDockerContainer(task_id, run_dir)

    # evaluate input/postinput declarations, including mapping from host to
    # in-container file paths
    container_env = _eval_task_inputs(logger, task, posix_inputs, container)

    # evaluate runtime.docker
    image_tag_expr = task.runtime.get("docker", None)
    if image_tag_expr:
        if isinstance(image_tag_expr, str):
            container.image_tag = image_tag_expr
        elif isinstance(image_tag_expr, WDL.Expr.Base):
            container.image_tag = image_tag_expr.eval(posix_inputs).value
        else:
            assert False

    # interpolate command
    command = _strip_leading_whitespace(task.command.eval(container_env).value)[1]

    # run container
    container.run(logger, command)

    # evaluate output declarations
    outputs = _eval_task_outputs(logger, task, container_env, container)

    logging.info("done")
    return (run_dir, outputs)


def _eval_task_inputs(
    logger: logging.Logger, task: WDL.Task, posix_inputs: WDL.Env.Values, container: TaskContainer
) -> WDL.Env.Values:
    # Map all the provided input Files to in-container paths
    # First make a pass to collect all the host paths and pass them to the
    # container as a group (so that it can deal with any basename collisions)
    host_files = []

    def collect_host_files(v: WDL.Value.Base) -> None:
        if isinstance(v, WDL.Value.File):
            host_files.append(v.value)
        for ch in v.children:
            collect_host_files(ch)

    WDL.Env.map(posix_inputs, lambda namespace, binding: collect_host_files(binding.rhs))
    container.add_files(host_files)

    # copy posix_inputs with all Files mapped to their in-container paths
    def map_files(v: WDL.Value.Base) -> WDL.Value.Base:
        if isinstance(v, WDL.Value.File):
            v.value = container.input_file_map[v.value]
        for ch in v.children:
            map_files(ch)
        return v

    container_inputs = WDL.Env.map(
        posix_inputs, lambda namespace, binding: map_files(copy.deepcopy(binding.rhs))
    )

    # Collect task declarations requiring evaluation. This includes input
    # declarations that are optional and/or have default values, and are not
    # explicitly provided by posix_inputs; and all post-input declarations.
    decls_to_eval = {}
    for inp in task.available_inputs:
        assert isinstance(inp, WDL.Env.Binding)
        try:
            WDL.Env.resolve(container_inputs, [], inp.name)
        except KeyError:
            decls_to_eval[inp.name] = inp.rhs
    for inp in task.postinputs:
        assert isinstance(inp, WDL.Env.Binding)
        try:
            WDL.Env.resolve(container_inputs, [], inp.name)
            assert False
        except KeyError:
            pass
        decls_to_eval[inp.name] = inp.rhs

    # TODO: topsort decls_to_eval according to internal dependencies

    container_env = container_inputs
    for b in container_env:
        assert isinstance(b, WDL.Env.Binding)
        logger.info("input {} -> {}".format(b.name, json.dumps(b.rhs.json)))

    # evaluate each declaration in order
    # TODO: stdlib speccializations:
    # - Invocations of size(), read_* are permitted only on File inputs (no String coercions pls)
    # - Invocations of write_* will add something to the mapping
    # - forbidden/undefined: stdout, stderr, glob
    for decl in decls_to_eval.values():
        v = WDL.Value.Null()
        if decl.expr:
            v = decl.expr.eval(container_env)
        logger.info("eval {} -> {}".format(decl.name, json.dumps(v.json)))
        container_env = WDL.Env.bind(container_env, [], decl.name, v)

    return container_env


def _eval_task_outputs(
    logger: logging.Logger, task: WDL.Task, env: WDL.Env.Values, container: TaskContainer
) -> WDL.Env.Values:
    # TODO: stdlib specializations,
    # - size(), read_* and glob are permitted only on paths in or under the container directory (cdup from working directory)
    # - their argument has to be translated from container to host path to actually execute

    outputs = []
    for decl in task.outputs:
        assert decl.expr
        v = WDL.Value.from_json(
            decl.type, decl.expr.eval(env).json
        )  # TODO: are we happy with this coercion approach?
        logger.info("output {} -> {}".format(decl.name, json.dumps(v.json)))
        outputs = WDL.Env.bind(outputs, [], decl.name, v)

    # map Files from in-container paths to host paths
    # TODO: coerce WDL.Value.String to WDL.Value.File when appropriate
    def map_files(v: WDL.Value.Base) -> WDL.Value.Base:
        if isinstance(v, WDL.Value.File):
            host_file = container.host_file(v.value)
            logger.debug("File {} -> {}".format(v.value, host_file))
            v.value = host_file
        for ch in v.children:
            map_files(ch)
        return v

    return WDL.Env.map(outputs, lambda namespace, binding: map_files(copy.deepcopy(binding.rhs)))


def _strip_leading_whitespace(txt: str) -> Tuple[int, str]:
    lines = txt.split("\n")

    to_strip = None
    for line in lines:
        lsl = len(line.lstrip())
        if lsl:
            c = len(line) - lsl
            assert c >= 0
            if to_strip is None or to_strip > c:
                to_strip = c
            # TODO: do something about mixed tabs & spaces

    if not to_strip:
        return (0, txt)

    for i, line_i in enumerate(lines):
        if line_i.lstrip():
            lines[i] = line_i[to_strip:]

    return (to_strip, "\n".join(lines))