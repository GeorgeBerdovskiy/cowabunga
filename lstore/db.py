from cowabunga_rs import database_module

# Wraps the database object written in Rust
class Database():
    def __init__(self):
        self.db = database_module.Database()

    def open(self, path):
        self.db.open(path)

    def close(self):
        self.db.close()

    def create_table(self, name, num_columns, key_index):
        return self.db.create_table(name, num_columns, key_index)

    def drop_table(self, name):
        pass

    def get_table(self, name):
        return self.db.get_table(name)
