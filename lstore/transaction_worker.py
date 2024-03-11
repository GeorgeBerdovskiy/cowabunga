from lstore.table import Table #, Record
from lstore.index import Index

from cowabunga_rs import xact_worker_module as xw

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.rust_xw = xw.TransactionWorker(transactions)

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.rust_xw.add_transaction(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        self.rust_xw.run()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.rust_xw.join()

