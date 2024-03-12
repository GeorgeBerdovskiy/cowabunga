from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

import shutil
# Delete the old database files
try:
    shutil.rmtree("./DB_TESTER")
    shutil.rmtree("./DB_TESTER_2")
    print("Deleted DB_TESTERs!")
except:
    print("Didn't need to delete DB_TESTER because it doesn't exist")

db = Database()
db.open("./DB_TESTER")
table = db.create_table("Grades", 5, 0)

query = Query(db, table)

transact = Transaction()
transact.add_query(query.insert, table, *[1, 2, 3, 4, 5])

transact.add_query(query.update, table, 1, *[2, None, 3, 4, None])

transact.add_query(query.select, table, 0, 0, [1, 0, 1, 0, 1])

transact.add_query(query.select_version, table, 0, 0, [1, 0, 1, 0, 1], -5)

transact.add_query(query.sum, table, 0, 10, 2)

transact.add_query(query.sum_version, table, 0, 10, 2, -10)

transact.add_query(query.delete, table, 0)

worker = TransactionWorker(db, [transact])
worker.run()