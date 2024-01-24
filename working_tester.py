from lstore.db import Database
from lstore.query import Query

from lstore.wrappers.table_wrapper import WrappedTable

from ecs_165_database import table_module

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

rust_table = table_module.Table("Grades", 5)
rust_table.insert([192, 232, 232, 232, 90])