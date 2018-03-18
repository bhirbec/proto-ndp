import random
import signal
import time
import threading
from optparse import OptionParser

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

  for i in range(opt.worker_count):
    worker_id = '{0}-{1}'.format(opt.worker_name, i)
    w = threading.Thread(target=work, args=(engine, worker_id, opt.timeout))
    w.start()
    workers.append(w)

  for w in workers:
    w.join()

  print('Bye...')


def work(engine, worker_id, timeout_threshold=None):
  print('starting worker {0}'.format(worker_id))
  last = time.time()

  while not _STOP.is_set():
    # print('reserving work (worker {0})...'.format(worker_id))
    with engine.begin() as con:
      start = time.time()
      res = con.execute('call reserve_task("{0}")'.format(worker_id))
      if res.returns_rows:
        last = time.time()
        task_id, host = res.first()
        print(worker_id, task_id, host, time.time() - start)
        time.sleep(random.randint(0, 3))
      else:
        task_id = None

    if task_id:
      with engine.begin() as con:
        con.execute('delete from worker where task_id = {0}'.format(task_id))

    if timeout_threshold:
      inactive_duration = time.time() - last
      if inactive_duration > timeout:
        break
    time.sleep(1)

  print('Worker {0} timed out'.format(worker_id))


_STOP = threading.Event()
def _shutdown(signum, frame):
  _STOP.set()


if __name__ == '__main__':
  options, args = parser.parse_args()
  main(options)
