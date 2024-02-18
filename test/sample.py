# define tasks
from logging import getLogger

import gokart
import luigi

logger = getLogger(__name__)


class TestCaseTask(gokart.TaskOnKart):
    task_namespace = 'test'
    param = luigi.Parameter()
    number = luigi.IntParameter()

    def require(self):
        return

    def output(self):
        return self.make_target(f'{self.param}/test_case.pkl')

    def run(self):
        self.dump(f'test number: {self.number}')


gokart.run()
