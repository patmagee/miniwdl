import unittest
import logging
import tempfile
import os
from .context import WDL

class TestTaskRunner(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
        self._dir = tempfile.mkdtemp(prefix="miniwdl_test_taskrun_")

    def _test_task(self, wdl:str, inputs: WDL.Env.Values = None, expected_outputs: WDL.Env.Values = None):
        # TODO: expected exceptions
        doc = WDL.parse_document(wdl)
        assert len(doc.tasks) == 1
        doc.typecheck()
        rundir, outputs = WDL.runner.run_local_task(doc.tasks[0], (inputs or []), parent_dir=self._dir)
        if expected_outputs is not None:
            self.assertEqual(outputs, expected_outputs)
        return outputs

    def test_basic_docker(self):
        self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                cat /etc/issue
            >>>
        }
        """)

        self._test_task(R"""
        version 1.0
        task hello {
            command <<<
                cat /etc/issue
            >>>
            runtime {
                docker: "ubuntu:18.10"
            }
        }
        """)

    def test_hello_blank(self):
        self._test_task(R"""
        version 1.0
        task hello_blank {
            input {
                String who
            }
            command <<<
                echo "Hello, ~{who}!"
            >>>
        }
        """,
        WDL.Env.bind([], [], "who", WDL.Value.String("Alyssa")))

    def test_hello_file(self):
        with open(os.path.join(self._dir, "alyssa.txt"), "w") as outfile:
            outfile.write("Alyssa")
        outputs = self._test_task(R"""
            version 1.0
            task hello_file {
                input {
                    File who
                }
                command <<<
                    echo -n "Hello, $(cat ~{who})!" > message.txt
                >>>
                output {
                    File message = "message.txt"
                }
            }
            """,
            WDL.Env.bind([], [], "who", WDL.Value.File(os.path.join(self._dir, "alyssa.txt"))))
        with open(WDL.Env.resolve(outputs, [], "message").value) as infile:
            self.assertEqual(infile.read(), "Hello, Alyssa!")

        # output an input file
        outputs = self._test_task(R"""
            version 1.0
            task hello_file {
                input {
                    File who
                }
                command <<<
                    echo -n "Hello, $(cat ~{who})!"
                >>>
                output {
                    File who2 = who
                }
            }
            """,
            WDL.Env.bind([], [], "who", WDL.Value.File(os.path.join(self._dir, "alyssa.txt"))))
        self.assertEqual(WDL.Env.resolve(outputs, [], "who2").value, os.path.join(self._dir, "alyssa.txt"))
