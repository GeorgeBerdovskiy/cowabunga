from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

import sys, os
from pathlib import Path
import shutil

sys.path.append(str(Path(__file__).resolve().parent.parent))

from lstore.db import Database
from lstore.query import Query
from random import choice

# Delete the old database files
try:
    shutil.rmtree("./evals")
    print("Deleted CORRECTNESS_M2!")
except:
    print("Didn't need to delete CORRECTNESS_M2 because it doesn't exist")
from time import process_time
import random

db = Database()
db.open("./evals")

# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []
q = 20000

insert_time_0 = process_time()
for i in range(0, q):
    query.insert(906659671 + i, 93, 0, 0, 0)
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
for i in range(0, q):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, q):
    query.select(choice(keys),0 , [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, q, 100):
    start_value = 906659671 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, q):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)
total_time = delete_time_1 - delete_time_0 + agg_time_1 - agg_time_0 + select_time_1 - select_time_0 + update_time_1 - update_time_0 + insert_time_1 - insert_time_0

print("total time: \t", total_time)