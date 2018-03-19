import time
from optparse import OptionParser

from sqlalchemy import create_engine


parser = OptionParser()
parser.add_option("-n", "--number-task", type=int, dest="number_tasks", default=10)


def main(options):
  engine = create_engine('mysql://ndp:password@localhost:3306/ndp')

  sql = '''
    insert into task
        (host, commands)
    values
        ('smf1-cs{0}-12', 'show hardware'),
        ('smf1-cs{0}-13', 'show hardware'),
        ('smf1-cs{0}-14', 'show hardware'),
        ('smf1-cs{0}-15', 'show hardware')
  '''
  for i in range(options.number_tasks):
    with engine.begin() as con:
      print 'Inserting tasks...'
      con.execute(sql.format(i))
      time.sleep(0.01)


if __name__ == '__main__':
  options, args = parser.parse_args()
  main(options)
