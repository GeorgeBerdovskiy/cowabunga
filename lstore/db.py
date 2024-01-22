from lstore.table import Table
from lstore.logger import Logger, LogType

class Database():

    def __init__(self):
        self.logger = Logger()
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        self.logger.logt(LogType.INFO, __file__, f'Creating table "{name}" with {num_columns} columns and key index {key_index}...')
        table = Table(name, num_columns, key_index)

        self.logger.logt(LogType.INFO, __file__, f'Returning new table "{name}"')
        return table


    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
