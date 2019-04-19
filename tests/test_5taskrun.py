import unittest
from .context import WDL

class TestTaskRunner(unittest.TestCase):

    def test_hello(self):
        wdl = """
        version 1.0
        task hello {
            input {
                File file1
                File file2
            }
            command <<<
                echo "Hello, world!"
            >>>
        }
        """

        doc = WDL.parse_document(wdl)
        doc.typecheck()
        WDL.runner.run_local_task(doc.tasks[0], [])

