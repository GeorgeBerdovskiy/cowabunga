import ecs_165_database

class WrappedTable:
    def __init__(self):
        self.table = ecs_165_database.Table()

    def insert(self):
        return self.table.insert()