from ecs_165_database import ecs_165_database

class WrappedTable:
    def __init__(self):
        self.table = ecs_165_database.Table.new()

    def get_indirection(self):
        return self.table.indirection