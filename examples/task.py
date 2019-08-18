# define tasks
import gokart
import luigi
from luigi.util import requires
from logging import getLogger

logger = getLogger(__name__)


class SampleTask(gokart.TaskOnKart):
    task_namespace = 'sample'
    name = luigi.Parameter()
    number = luigi.IntParameter()
    
    def require(self):
        return

    def output(self):
        return self.make_target(f'{self.name}/sample.pkl')

    def run(self):
        self.dump(f'this is sample output. model number: {self.number}')

        
@requires(SampleTask)
class SecondTask(gokart.TaskOnKart):
    task_namespace = 'sample'
    param = luigi.Parameter()

    def output(self):
        return self.make_target(f'SECOND_TASK/task.pkl')

    def run(self):
        sample = self.load()
        self.dump(sample + f'add task: {self.param}')
        
gokart.run()