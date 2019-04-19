import unittest
import logging
import tempfile
from .context import WDL

# TODO: subclass unittest.TestCase for a task with source, inputs, & expected outputs/error

class TestTaskRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_taskrun_")

    def test_hello(self):
        wdl = """
        version 1.0
        task hello {
            command <<<
                apt-get update
                apt-get install -y lsb-release
                lsb_release -c
            >>>
        }

        task hello10 {
            command <<<
                apt-get update
                apt-get install -y lsb-release
                lsb_release -c
            >>>

            runtime {
                docker: "ubuntu:18.10"
            }
        }

        task hello_blank {
            input {
                String who
            }
            command <<<
                echo "Hello, ~{who}!"
            >>>
        }
        """
        doc = WDL.parse_document(wdl)
        doc.typecheck()
        WDL.runner.run_local_task(doc.tasks[0], [], parent_dir=self._dir)
        WDL.runner.run_local_task(doc.tasks[1], [], parent_dir=self._dir)
        WDL.runner.run_local_task(doc.tasks[2], WDL.Env.bind([], [], "who", WDL.Value.String("Alyssa")), parent_dir=self._dir)

