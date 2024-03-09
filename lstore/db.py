from cowabunga_rs import table_module, buffer_pool_module
import shutil

class Database():
    def __init__(self):
        self.directory = None
        self.tables = []
        self.loaded = False

        try:
            shutil.rmtree("./COWDAT")
        except:
            pass

        self.open("COWDAT")

    # Not required for milestone1
    def open(self, path):
        self.directory = path

    def close(self):
        for table in self.tables:
            table.persist()
        
        table_module.persist_bpm()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if not self.loaded:
            table = table_module.Table(self.directory, name, num_columns, key_index, True)
            self.loaded = True
        else:
            table = table_module.Table(self.directory, name, num_columns, key_index, False)
        
        self.tables.append(table)
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
        table = table_module.Table(self.directory, name, 0, 0, self.bpm)
        table.start_merge_thread()
        self.tables.append(table)
        return table