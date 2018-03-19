import random
import signal
import time
from optparse import OptionParser
from Queue import Queue
from threading import Event, Thread

from sqlalchemy import create_engine

parser = OptionParser()
parser.add_option("-w", "--worker-name", dest="worker_name")
parser.add_option("-n", "--worker-count", type=int, dest="worker_count", default=10)
parser.add_option("-t", "--timeout", dest="timeout", default=None)


def main(opt):
  signal.signal(signal.SIGINT, _shutdown)
  signal.signal(signal.SIGTERM, _shutdown)

  engine = create_engine('mysql://ndp:password@localhost:3306/ndp', pool_size=100)
  workers = []
  taskQ = Queue()
  requestQ = Queue()

  for i in range(opt.worker_count):
    worker_id = '{0}-{1}'.format(opt.worker_name, i)
    w = Worker(engine, worker_id, requestQ, taskQ)
    w.start()
    workers.append(w)

  reserve_tasks(engine, requestQ, taskQ)

  for w in workers:
    w.join()

  print('Bye...')


def reserve_tasks(engine, requestQ, taskQ):
  # begin = time.time()
  # last = time.time()

  found = 0
  not_found = 0

  while not _STOP.is_set():
    worker_id = requestQ.get(block=True)
    with engine.begin() as con:
      start = time.time()
      res = con.execute('call reserve_task("{0}")'.format(worker_id))
      if res.returns_rows:
        found += 1
        task_id, host = res.first()
        print(task_id, host, time.time() - start)
        taskQ.put((task_id, host))
        # last = time.time()
      else:
        not_found += 1
        task_id = None

      # if timeout_threshold:
      #   inactive_duration = time.time() - last
      #   if inactive_duration > timeout_threshold:
      #     duration = time.time() - begin
      #     print('Waiting... Task found: {0} / Not found: {1} ({2})'.format(found, not_found, duration))


      # print('Worker {0} timed out'.format(self.worker_id))

    time.sleep(0.1)


class Worker(Thread):
  def __init__(self, engine, worker_id, requestQ, taskQ):
    self.engine = engine
    self.worker_id = worker_id
    self.requestQ = requestQ
    self.taskQ = taskQ
    Thread.__init__(self)

  def run(self):
    print('starting worker {0}'.format(self.worker_id))
    self.requestQ.put(self.worker_id)

    while not _STOP.is_set():
      task_id, host = self.taskQ.get(block=True)
      # simlutated work
      time.sleep(random.randint(0, 3))

      with self.engine.begin() as con:
        con.execute('delete from worker where task_id = {0}'.format(task_id))

      print('task {0} is done'.format(task_id))
      self.requestQ.put(self.worker_id)


_STOP = Event()
def _shutdown(signum, frame):
  _STOP.set()


if __name__ == '__main__':
  options, args = parser.parse_args()
  main(options)
