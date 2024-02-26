from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

print("Preparing to create grades table")

grades_table = db.create_table('Grades', 5, 0)

print("Done with the script")

query = Query(grades_table)

query.insert(9020, 95, 95, 95, 93)
query.insert(9021, 96, 94, 95, 92)
query.insert(9022, 97, 93, 95, 91)
print(query.select(9020, 0, [1, 1, 1, 1, 1])[0].columns)

print("---")
for res in query.select(95, 3, [1, 1, 1, 1, 1]):
    print(res.columns)