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