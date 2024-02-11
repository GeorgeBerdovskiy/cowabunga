from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

query.insert(90210, 93, 94, 95, 96)
result = query.select(90210, 0, [1, 0, 1, 0, 1])
print(result[0].columns)


query.insert(90211, 93, 100, 200 ,300)
results = query.select(93, 1, [1, 0, 1, 0, 1])
for result in results:
    print(result.columns)

query.update(90211, *[None, 100, 100, 100, None])
query.update(90211, *[None, 101, 100, 100, None])
query.update(90211, *[None, 101, 102, 100, None])
query.update(90211, *[None, 101, 103, 100, None])

query.update(90210, *[None, 100, 100, 100, 300])
query.update(90210, *[None, 101, 100, 100, None])
query.update(90210, *[None, 101, 102, 100, None])
query.update(90210, *[None, 101, 103, 100, None])

print("VERSION ZERO...")
for result in query.select_version(300, 4, [0, 1, 1, 1, 1], 0):
    print(result.columns)

print("VERSION -1...")
for result in query.select_version(300, 4, [0, 1, 1, 1, 1], -1):
    print(result.columns)

print("VERSION -2...")
for result in query.select_version(300, 4, [0, 1, 1, 1, 1], -2):
    print(result.columns)

print("VERSION -3...")
for result in query.select_version(300, 4, [0, 1, 1, 1, 1], -3):
    print(result.columns)

print("VERSION -4 (BASE)...")
for result in query.select_version(300, 4, [0, 1, 1, 1, 1], -4):
    print(result.columns)

print("VERSION -5 (STILL BASE)...")
for result in query.select_version(300, 4, [0, 1, 1, 1, 1], -5):
    print(result.columns)