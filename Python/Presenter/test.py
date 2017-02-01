import unittest
import Presenter.processor as processor
import dp_util.db_util

class TestDB(unittest.TestCase):
    def test_makeconnection(self):
        pass


class TestProcessor(unittest.TestCase):
    def test_start(self):
        pass

    def test_makeconnection(self):
        self.processor.make_connection()
        self.assertTrue(self.processor.validate_connection())

    def test_runbatch_badconnection(self):
        with self.assertRaises(Exception):
            self.processor.pull_batch(batch_size=20)

    def test_runbatch(self):
        self.processor.make_connection()
        # TODO: figure how to assert lack of exception or make meaningful return code
        self.processor.pull_batch(batch_size=20)

    def setUp(self):
        self.processor = processor.Processor()
        self.processor._process_state = processor.ProcessState.debug

    def tearDown(self):
        self.processor.stop_process()
        self.processor = None



if __name__ == '__main__':
    unittest.main()
