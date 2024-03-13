from lstore.table import Table
from lstore.index import Index

from cowabunga_rs import transaction_module

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        # self.queries = []
        self.transaction = transaction_module.Transaction()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        args = list(args)
        query_name = query.__name__

        if query_name == "insert":
            self.transaction.add_insert(table.id, table.primary_key_index, list(args))
        elif query_name == "update":
            self.transaction.add_update(table.id, table.primary_key_index, args[0], list(args[1:]))
        elif query_name == "select":
            self.transaction.add_select(table.id, table.primary_key_index, args[0], args[1], args[2:][0])
        elif query_name == "sum":
            self.transaction.add_sum(table.id, table.primary_key_index, args[0], args[1], args[2])
        elif query_name == "select_version":
            self.transaction.add_select_version(table.id, table.primary_key_index, args[0], args[1], args[2:-1][0], args[-1])
        elif query_name == "sum_verstion":
            self.transaction.add_sum_version(table.id, table.primary_key_index, args[0], args[1], args[2], args[3])
        elif query_name == "delete":
            self.transaction.add_delete(table.id, table.primary_key_index, args[0])
        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        # TODO: commit to database
        return True

