import logging
import sys

from datetime import datetime

from volttron.utils.math_utils import mean, stdev


_log = logging.getLogger(__name__)


# TODO: Does this design still work for the new driver?
class ScalabilityTester:
    def __init__(self, required_iterations):
        self.required_iterations = required_iterations

        self.waiting_to_finish = set()
        self.test_iterations = 0
        self.test_results = []
        self.current_test_start = None

    def poll_starting(self, topic):
        if not self.waiting_to_finish:
            # Start a new measurement
            self.current_test_start = datetime.now()
            # TODO: What should be in this set for the new polling architecture?
            self.waiting_to_finish = set(self.instances.keys())

        if topic not in self.waiting_to_finish:
            _log.warning(
                f"{topic} started twice before test finished, increase the length of scrape interval and rerun test"
            )

    def poll_ending(self, topic):
        try:
            self.waiting_to_finish.remove(topic)
        except KeyError:
            _log.warning(
                f"{topic} published twice before test finished, increase the length of scrape interval and rerun test"
            )

        if not self.waiting_to_finish:
            end = datetime.now()
            delta = end - self.current_test_start
            delta = delta.total_seconds()
            self.test_results.append(delta)

            self.test_iterations += 1

            _log.info("publish {} took {} seconds".format(self.test_iterations, delta))

            if self.test_iterations >= self.required_iterations:
                # Test is now over. Button it up and shutdown.
                mean_t = mean(self.test_results)
                stdev_t = stdev(self.test_results)
                _log.info("Mean total publish time: " + str(mean_t))
                _log.info("Std dev publish time: " + str(stdev_t))
                sys.exit(0)
