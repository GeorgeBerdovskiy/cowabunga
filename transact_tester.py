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
# transact.add_query(query.delete, table, 2)

transact_2 = Transaction()
transact_2.add_query(query.insert, table, *[11, 12, 13, 14, 15])
transact_2.add_query(query.delete, table, 2)

worker = TransactionWorker(db, [transact, transact_2])
worker_2 = TransactionWorker(db, [transact_2])
worker.run()
worker_2.run()

print("This is immediately after the worker begins running....")

worker.join()
worker_2.join()

print("...and this is ONLY after the worker is done.")