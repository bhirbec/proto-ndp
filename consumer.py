import random
import signal
import time
from optparse import OptionParser
from Queue import Empty, Queue
from threading import Event, Thread

from sqlalchemy import create_engine


parser = OptionParser()
parser.add_option("-w", "--worker-name", dest="worker_name")
parser.add_option("-n", "--worker-count", type=int, dest="worker_count", default=10)


def main(opt):
  signal.signal(signal.SIGINT, _shutdown)
  signal.signal(signal.SIGTERM, _shutdown)

  engine = create_engine('mysql://ndp:password@localhost:3306/ndp', pool_size=100)
  workers = []
  taskQ = Queue()
  doneQ = Queue()

  for i in range(opt.worker_count):
    worker_id = '{0}-{1}'.format(opt.worker_name, i)
    w = Worker(engine, worker_id, taskQ, doneQ)
    w.start()
    workers.append(w)

  reserve_tasks(engine, opt.worker_name, taskQ, doneQ, opt.worker_count)

  for w in workers:
    w.join()

  print('Bye...')


def reserve_tasks(engine, worker_name, taskQ, doneQ, n):
  reserve_interval = 1
  start = time.time()
  done_ids = []

  while not _STOP.is_set():
    if (time.time() - start) > reserve_interval:
      str_ids = ','.join(str(s) for s in done_ids)
      number_done = len(done_ids)
      done_ids = []

      start = time.time()
      with engine.begin() as con:
        tasks = con.execute('call reserve_tasks("{0}", "{1}", {2})'.format(worker_name, str_ids, n))
        tasks = list(tasks)

        duration = time.time() - start
        msg = 'Fetched {0} tasks over {1} requested ({1})'
        print(msg.format(len(tasks), number_done, duration))

        for task in tasks:
          taskQ.put(task)

    try:
      done_id = doneQ.get(block=False)
    except Empty:
      pass
    else:
      done_ids.append(done_id)

    time.sleep(0.1)


class Worker(Thread):
  def __init__(self, engine, worker_id, taskQ, doneQ):
    self.engine = engine
    self.worker_id = worker_id
    self.taskQ = taskQ
    self.doneQ = doneQ
    Thread.__init__(self)

  def run(self):
    print('starting worker {0}'.format(self.worker_id))

    while not _STOP.is_set():
      task = self.taskQ.get(block=True)
      start = time.time()

      task_id, host = task['task_id'], task['host']
      print('received task_id {0} ({1})'.format(task_id, host))

      # simlutated work
      time.sleep(random.randint(0, 3))

      print('task {0} is done {1}'.format(task_id, time.time() - start))
      self.doneQ.put(task_id, block=False)


_STOP = Event()
def _shutdown(signum, frame):
  _STOP.set()


if __name__ == '__main__':
  options, args = parser.parse_args()
  main(options)
