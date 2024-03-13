from lstore.table import Table
from lstore.index import Index

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, db, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.db = db
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
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

