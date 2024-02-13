from lstore.db import Database
from lstore.query import Query

from cowabunga_rs import table_module, buffer_pool_module
from time import process_time
from random import choice, randrange

# Define "constants"
NUM_COLUMNS = 5
NUM_INSERTIONS = 100000
VALUE_MIN = -1000
VALUE_MAX = 1000

totals = [ [] for _ in range(NUM_COLUMNS) ]
record_mapping = {}
key = []

primary_key_index = choice(range(NUM_COLUMNS))

db = Database()
grades_table = db.create_table('Grades', NUM_COLUMNS, primary_key_index)
query = Query(grades_table)

f = open("correctness.log", "w")
fp = open("correctness.log.py", "w")

f.write(f"TABLE: Grades\nNUM COLUMNS: {NUM_COLUMNS}\nPRIM KEY IND: {primary_key_index}\n---------\n")

fp.write(f"""from lstore.db import Database
from lstore.query import Query

from cowabunga_rs import table_module, buffer_pool_module
from time import process_time
from random import choice, randrange

db = Database()
grades_table = db.create_table('Grades', {NUM_COLUMNS}, {primary_key_index})
query = Query(grades_table)

""")

for _ in range(NUM_INSERTIONS):
    query_choice = choice(range(4))

    if query_choice == 0:
        # Insert a record
        record = [choice(range(VALUE_MIN, VALUE_MAX)) for _ in range(NUM_COLUMNS)]
        
        # If some other unexpected error occurs, we'll see it since it isn't handled
        try:
            query.insert(*record)
        except ValueError:
            if record[primary_key_index] not in record_mapping:
                print("[ERROR] Failed to insert because primary key has already been used, but that's not true.")
                exit(1)
            else:
                continue

        f.write(f"INSERT {record[primary_key_index]} {record}\n")
        fp.write(f"query.insert(*{record})\n")

        if record[primary_key_index] not in record_mapping:
            # New record - add it to the totals and mapping
            i = 0
            for i in range(NUM_COLUMNS):
                totals[i].append([record[i]])

            record_mapping[record[primary_key_index]] = len(totals[0]) - 1
        else:
            print("[WARNING] Insertion not recorded because key is duplicate.")
    elif query_choice == 1:
        # Update a record
        keys = list(record_mapping.keys())
        if len(keys) == 0:
            print("[WARNING] Couldn't update because no insertions. Moving on...")
            continue

        primary_key = choice(keys)

        updates = [choice([None, choice(range(VALUE_MIN, VALUE_MAX))]) for _ in range(NUM_COLUMNS)]
        updates[primary_key_index] = primary_key

        query.update(primary_key, *updates)
        
        f.write(f"UPDATE {primary_key} {updates}\n")
        fp.write(f"query.update({primary_key}, *{updates})\n")

        totals_index = record_mapping[primary_key]
        i = 0
        for i in range(NUM_COLUMNS):
            if updates[i] is None:
                totals[i][totals_index].append(totals[i][totals_index][-1])
            else:
                totals[i][totals_index].append(updates[i])
    elif query_choice == 2:
        # Perform a select on the primary key (should return only one record)
        keys = list(record_mapping.keys())
        if len(keys) == 0:
            print("[WARNING] Couldn't select because no insertions. Moving on...")
            continue
    
        primary_key = choice(keys)
        projection = [choice([0, 1]) for _ in range(NUM_COLUMNS)]
        result = query.select(primary_key, primary_key_index, projection)

        f.write(f"SELECT PRIM {primary_key} {projection}\n")
        fp.write(f"query.select({primary_key}, {primary_key_index}, {projection})\n")

        if len(result) != 1:
            print(f"[ERROR] Expected one result, got {len(result)}.")
            exit(1)
        
        # We got only one result as expected, but is it correct?
        record_index = record_mapping[primary_key]
        all_columns = []
        for i in range(NUM_COLUMNS):
            all_columns.append(totals[i][record_index][-1])
        
        projected_columns = []
        for i in range(len(all_columns)):
            if projection[i] == 1:
                projected_columns.append(all_columns[i])
        
        if len(projected_columns) != len(result[0].columns):
            print(f"[ERROR] Expected select to return {projected_columns}, found {result[0].columns}.")
            exit(1)

        for i in range(len(projected_columns)):
            if projected_columns[i] != result[0].columns[i]:
                print(f"[ERROR] Expected select to return {projected_columns}, found {result[0].columns}")
                
                fp.write(f"# [ERROR] Expected select to return {projected_columns}, got {result[0].columns} instead")

                print(record_mapping)
                print(totals)
                
                exit(1)
    elif query_choice == 3:
        # Perform a select on another key (may return several records)
        if len(list(record_mapping.keys())) == 0:
            print("[WARNING] Couldn't select because no insertions. Moving on...")
            continue
            
        search_key = choice(range(VALUE_MIN, VALUE_MAX))

        search_key_index = choice(range(NUM_COLUMNS))
        while search_key_index == primary_key_index:
            search_key_index = choice(range(NUM_COLUMNS))

        # NOTE - We'll always include the projection index for easier correctness testing
        projection = [choice([0, 1]) for _ in range(NUM_COLUMNS)]
        projection[primary_key_index] = 1

        results = query.select(search_key, search_key_index, projection)
        f.write(f"SELECT ANY {search_key} @ {search_key_index} {projection}\n")
        fp.write(f"query.select({search_key}, {search_key_index}, {projection})\n")

        # We need "reconstruct" the records we expect from `totals`
        # Find every entry in the totals column that has the search key
        found_indices = []

        for i in range(len(totals[search_key_index])):
            if totals[search_key_index][i][-1] == search_key:
                found_indices.append(i)
        
        # Find all the primary keys that match the found indices
        matches = []
        for key in record_mapping:
            if record_mapping[key] in found_indices:
                matches.append(key)
        
        if len(matches) != len(results):
            print(f"[ERROR] Expected SELECT to return {len(matches)} records but got {len(results)}.")
            exit(1)
        
        if len(matches) == 0:
            continue # No need to check for correctness since there's nothing to check

        # We have the correct number of records, but are they the _right_ records?
        # What is the index of the primary key according to the projection?
        i, j = 0, 0
        while i < len(projection):
            if i == primary_key_index:
                break

            if projection[i] == 1:
                j += 1
            
            i += 1

        returned_prim_keys = list(map(lambda record: record.columns[j], results))

        for prim_key in returned_prim_keys:
            if prim_key not in matches:
                print(f"[ERROR] Select returned record w/primary key {prim_key}, but it shouldn't have. Returned primary keys are {returned_prim_keys} and the expected ones are {matches}")
                
                fp.write(f"# [ERROR] Last select returned primary key {prim_key} but it shouldn't have.")

                exit(1)

print(f"[INFO] Success! Ran {NUM_INSERTIONS} random queries without errors or mismatches in behavior.")
f.close()
