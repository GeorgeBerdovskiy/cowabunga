from lstore.db import Database
from lstore.query import Query

from lstore.wrappers.table_wrapper import WrappedTable

from ecs_165_database import table_module

'''db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)'''

rust_table = table_module.Table("Grades", 5)
rust_table.insert([10110, 95, 96, 97, 98])
rust_table.insert([90210, 93, 95, 94, 96])
print(rust_table)

rust_table.update([90210, None, None, 100, None])
rust_table.update([90210, None, None, 100, 101])

result = rust_table.select(90210)
print(result)