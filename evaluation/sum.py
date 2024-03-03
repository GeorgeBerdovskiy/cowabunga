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
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

next_primary_key = 900000000
num_queries = 100

for i in range(num_queries):
    # Perform an insertion
    col_two = random.randint(0, 1000000)
    col_three = random.randint(0, 1000000)
    col_four = random.randint(0, 1000000)
    col_five = random.randint(0, 1000000)

    query.insert(next_primary_key, col_two, col_three, col_four, col_five)
    next_primary_key += 1

total_time_0 = process_time()
for i in range(num_queries):
    # Perform a sum
    col_index = random.randrange(0, 5)

    range_start = random.randrange(900000000, 900000000 + 999799)
    range_end = range_start + 100

    if col_index != 0:
        range_start = random.randint(0, 999900)
        range_end = range_start + 100
    
    query.sum(range_start, range_end, col_index)
total_time_1 = process_time()
print(f"Success! Finished {num_queries} randomized sums in... \t\t{total_time_1 - total_time_0}s")