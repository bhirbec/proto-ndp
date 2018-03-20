import time
from optparse import OptionParser

from sqlalchemy import create_engine


parser = OptionParser()
parser.add_option("-n", "--number-task", type=int, dest="number_tasks", default=10)


def main(options):
  engine = create_engine('mysql://ndp:password@localhost:3306/ndp')

  values = ',\n'.join(
    "('smf1-cs-{0}', 'show hardware')".format(i)
    for i in range(300)
  )

  sql = 'insert into task(host, commands) values {0}'.format(values)

  for i in range(options.number_tasks):
    with engine.begin() as con:
      print 'Inserting tasks...'
      con.execute(sql.format(i))
      time.sleep(1)


if __name__ == '__main__':
  options, args = parser.parse_args()
  main(options)
