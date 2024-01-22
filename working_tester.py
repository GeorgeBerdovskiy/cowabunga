from lstore.db import Database
from lstore.query import Query

from lstore.wrappers.table_wrapper import WrappedTable

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

print(WrappedTable.__dict__)