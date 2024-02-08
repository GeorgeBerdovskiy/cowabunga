from lstore.db import Database
from lstore.query import Query

from cowabunga_rs import table_module, buffer_pool_module
from time import process_time
from random import choice, randrange

bpm = buffer_pool_module.BufferPool()
grades_table = table_module.Table("Grades", 5, 0, bpm)
keys = []

'''### DELETE BLOCK ###
grades_table.insert((90210, 93, 94, 95, 96))
# print(grades_table.select(90210, 0, []))
# Expected result - [90210, 93, 94, 95, 96]

grades_table.update(90210, [None, 100, None, None, 100])
# print(grades_table.select(90210, 0, []))
# Expected result - [90210, 100, 94, 95, 100]

grades_table.update(90210, [None, 334, None, None, 95])
print(grades_table.select(90210, 0, []))
# Expected result - [90210, 105, 94, 95, 95]

### END DELETE BLOCK ###'''

insert_time_0 = process_time()
for i in range(0, 10000):
    grades_table.insert((906659671 + i, 93, 0, 0, 0))
    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    grades_table.update(choice(keys), choice(update_cols))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    result = grades_table.select(choice(keys), 0 , [1, 1, 1, 1, 1])
    print(result)
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
