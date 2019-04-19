import unittest
import logging
import tempfile
from .context import WDL

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
        """
        doc = WDL.parse_document(wdl)
        doc.typecheck()
        WDL.runner.run_local_task(doc.tasks[0], [], parent_dir=self._dir)
        WDL.runner.run_local_task(doc.tasks[1], [], parent_dir=self._dir)

