from lstore.table import Table
from lstore.index import Index

import copy

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, db, transactions = []):
        #print("[DEBUG] Creating new transaction worker")
        self.stats = []
        self.transactions = copy.deepcopy(transactions)
        self.result = 0
        self.db = db
        self.worker_id = 0
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        #print(f"[PYTHON] Adding transaction {t}")
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        total_queries = 0

        for transact in self.transactions:
            total_queries += transact.query_count

        #print(f"[PYTHON] Preparing to start worker with {len(self.transactions)} transactions and {total_queries} TOTAL queries!")

        self.worker_id = self.db.db.run_worker(list(map(lambda transact: transact.transaction, self.transactions)))
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.db.db.join_worker(self.worker_id)

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

