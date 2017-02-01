from enum import Enum

class ProcessState(Enum):
    """
    If in starting state, transition to running state when possible
    If in stopping state, transition to stopped state when possible
    If in running state, kick off batch processing
    If in stopped state, do not kick off batch processing
    If in uninitialized state, treat as stopped plus further restrictions tbd
    If in debug state, run on command, and treat as test mode
    """
    starting = 0
    stopping = 1
    running = 2
    stopped = 3
    uninitialized = 4
    debug = 5


class Processor:
    def start_process(self, test_mode = False):
        """
        Put in request to start processing
        :param test_mode: If set, do not commit nontrivial changes to database
        :return: None
        """
        pass

    def stop_process(self):
        """
        Put in request to stop processing
        :return:
        """
        pass

    def get_process_state(self):
        return self._process_state

    def pull_batch(self, batch_size):
        """

        :param batch_size: number of elements in batch
        :return: success code
        """
        pass

    def run_process(self):
        pass

    def validate_connection(self):
        """

        :return: True if connection valid, false if not
        """
        return False

    def make_connection(self):
        """
        TBD
        :return:
        """
        pass

    def __init__(self):
        self._process_state = ProcessState.uninitialized
