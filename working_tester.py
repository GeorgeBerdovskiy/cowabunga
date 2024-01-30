from lstore.db import Database
from lstore.query import Query

from lstore.wrappers.table_wrapper import WrappedTable

from ecs_165_database import table_module
from time import process_time
from random import choice, randrange

'''db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)'''

rust_table = table_module.Table("Grades", 5)

insert_time_0 = process_time()
for i in range(0, 10000):
    rust_table.insert([906659671 + i, 93, 94, 95, 96])
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

'''rust_table.insert([10110, 95, 96, 97, 98])
rust_table.insert([90210, 93, 95, 94, 96])
print(rust_table)

rust_table.update([90210, None, 99, 100, None])
rust_table.update([90210, None, None, 105, 101])

result = rust_table.select(90210)
print(result)'''