from cowabunga_rs import database_module
import shutil
from lstore.query import Query

# Delete the old database files
try:
    shutil.rmtree("./DB_TESTER")
    shutil.rmtree("./DB_TESTER_2")
    print("Deleted DB_TESTERs!")
except:
    print("Didn't need to delete DB_TESTER because it doesn't exist")

db = database_module.Database()
db.open("./DB_TESTER")
table = db.create_table("Grades", 5, 0)

query = Query(table)
query.insert(0, 1, 2, 3, 4)
query.update(0, *[None, 5, 4, 3, 2])
results = query.select(0, 0, [1, 1, 0, 1, 1])

for result in results:
    print(result.columns)

table_2 = db.create_table("Students", 3, 1)
query_2 = Query(table_2)
query_2.insert(0, 1, 2)
query_2.update(1, *[None, 1, 4])
results_2 = query_2.select(0, 0, [1, 1, 0])

for result in results_2:
    print(result.columns)

db.close()

db = database_module.Database()
db.open("./DB_TESTER_2")
table_3 = db.create_table("Grades_2", 5, 0)

query_3 = Query(table_3)
query_3.insert(0, 1, 2, 3, 4)
query_3.update(0, *[None, 5, 4, 3, 2])
results_3 = query_3.select(0, 0, [1, 1, 0, 1, 1])

for result in results_3:
    print(result.columns)

db.close()