import sys, os
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from cowabunga.db import Database
from cowabunga.query import Query
from time import process_time
from random import choice
import random

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

next_primary_key = 900000000
num_queries = 10000000

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
    # Perform a selection
    col_index = random.randrange(0, 5)

    search_key = random.randrange(900000000, 900000000 + 1000000)
    if col_index != 0:
        search_key = random.randint(0, 1000000)

    proj_one = choice([0, 1])
    proj_two = choice([0, 1])
    proj_three = choice([0, 1])
    proj_four = choice([0, 1])
    proj_five = choice([0, 1])

    query.select(search_key, col_index, [proj_one, proj_two, proj_three, proj_four, proj_five])
total_time_1 = process_time()
print(f"Success! Finished {num_queries} randomized selects in... \t\t{total_time_1 - total_time_0}s")