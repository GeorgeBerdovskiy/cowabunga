import sys, os
from pathlib import Path
import shutil

sys.path.append(str(Path(__file__).resolve().parent.parent))

from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice
import random

# Delete the old database files
try:
    shutil.rmtree("./LOAD_M2")
    print("Deleted LOAD_M2!")
except:
    print("Didn't need to delete LOAD_M2 because it doesn't exist")

# Student Id and 4 grades
db = Database()
db.open("./LOAD_M2")
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

next_primary_key = 900000000
num_queries = 1000000

total_time_0 = process_time()
for i in range(num_queries):
    print(f"> Insert #{i}")
    # Perform an insertion
    col_two = random.randint(0, 1000000)
    col_three = random.randint(0, 1000000)
    col_four = random.randint(0, 1000000)
    col_five = random.randint(0, 1000000)

    query.insert(next_primary_key, col_two, col_three, col_four, col_five)
    next_primary_key += 1
total_time_1 = process_time()
print(f"Success! Finished {num_queries} randomized insertions in... \t\t{total_time_1 - total_time_0}s")

for i in range(num_queries):
    print(f"> Query #{i}")
    query_choice = random.randint(0,3)

    if query_choice == 0:
        # Perform an update
        primary_key = random.randrange(900000000, 900000000 + num_queries)
        col_two = choice([None, random.randint(0, 1000000)])
        col_three = choice([None, random.randint(0, 1000000)])
        col_four = choice([None, random.randint(0, 1000000)])
        col_five = choice([None, random.randint(0, 1000000)])

        query.update(primary_key, *[None, col_two, col_three, col_four, col_five])
    elif query_choice == 1:
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
    elif query_choice == 2:
        # Perform a selection by version
        col_index = random.randrange(0, 5)

        search_key = random.randrange(900000000, 900000000 + 1000000)
        if col_index != 0:
            search_key = random.randint(0, 1000000)
        
        proj_one = choice([0, 1])
        proj_two = choice([0, 1])
        proj_three = choice([0, 1])
        proj_four = choice([0, 1])
        proj_five = choice([0, 1])

        version = -1 * random.randint(0, 100000)

        query.select_version(search_key, col_index, [proj_one, proj_two, proj_three, proj_four, proj_five], version)
    elif query_choice == 3:
        # Perform a sum
        col_index = random.randrange(0, 5)

        range_start = random.randrange(900000000, 900000000 + 999799)
        range_end = range_start + 100

        if col_index != 0:
            range_start = random.randint(0, 999900)
            range_end = range_start + 100
        
        query.sum(range_start, range_end, col_index)
    else:
        print("[ERROR] Query choice selection out of range.")
        exit()

total_time_1 = process_time()
print(f"Success! Finished {num_queries * 2} randomized queries in... \t\t{total_time_1 - total_time_0}s")

db.close()