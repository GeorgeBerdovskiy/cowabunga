import sys, os
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from cowabunga.db import Database
from cowabunga.query import Query
from random import choice

# Define "constants"
NUM_COLUMNS = 2
NUM_INSERTIONS = 100000
VALUE_MIN = -1000
VALUE_MAX = 1000
WRITE_SCRIPT = False

# This will be used to store values
totals = [ [] for _ in range(NUM_COLUMNS) ]

# This maps primary keys to their columns in `totals`
record_mapping = {}

# This contains the primary keys
key = []

# Randomly select which column is the primary key
primary_key_index = choice(range(NUM_COLUMNS))

# Spin up the database and create a table
db = Database()
grades_table = db.create_table('Grades', NUM_COLUMNS, primary_key_index)
query = Query(grades_table)

# Open the script to be generated and write prologue
if not os.path.exists("tests/generated_scripts"):
    os.makedirs("tests/generated_scripts")

fp = open("tests/generated_scripts/correctness.log.py", "w")
fp.write(f"""from cowabunga.db import Database
from cowabunga.query import Query

from cowabunga_rs import table_module, buffer_pool_module
from time import process_time
from random import choice, randrange

db = Database()
grades_table = db.create_table('Grades', {NUM_COLUMNS}, {primary_key_index})
query = Query(grades_table)

""")

# Write to script _if_ configuration specifies that we should
def write_script(input: str):
    if not WRITE_SCRIPT:
        return

    fp.write(f"{input}\n")

for q in range(NUM_INSERTIONS):
    print(f"[INFO] QUERY {q + 1} / {NUM_INSERTIONS}")
    query_choice = choice(range(7))

    if query_choice == 0:
        # Insert a record and write to script
        record = [choice(range(VALUE_MIN, VALUE_MAX)) for _ in range(NUM_COLUMNS)]
        write_script(f"query.insert(*{record})")

        # If some other unexpected error occurs, we'll see it since it isn't handled
        try:
            query.insert(*record)
        except ValueError:
            if record[primary_key_index] not in record_mapping:
                print("[ERROR] Failed to insert because primary key has already been used, but that's not true.")
                exit(1)
            else:
                continue

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

        # Update record and write to script
        write_script(f"query.update({primary_key}, *{updates})")

        try:
            query.update(primary_key, *updates)
        except ValueError:
            if record[primary_key_index] in record_mapping:
                print("[ERROR] Failed to update because primary key hasn't already been used, but that's not true.")
                exit(1)
            else:
                continue

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

        # Select on primary key and write to script
        result = query.select(primary_key, primary_key_index, projection)
        write_script(f"query.select({primary_key}, {primary_key_index}, {projection})")

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
            print(f"[ERROR] Expected SELECT to return {projected_columns}, found {result[0].columns}.")
            exit(1)

        for i in range(len(projected_columns)):
            if projected_columns[i] != result[0].columns[i]:
                print(f"[ERROR] Expected SELECT to return {projected_columns}, found {result[0].columns}")
                write_script(f"# [ERROR] Expected SELECT to return {projected_columns}, got {result[0].columns} instead")
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

        # Select on the search key and write to script
        results = query.select(search_key, search_key_index, projection)
        write_script(f"query.select({search_key}, {search_key_index}, {projection})")

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
                print(f"[ERROR] SELECT returned record with primary key {prim_key}, but it shouldn't have. Returned primary keys are {returned_prim_keys} and expected keys are {matches}")
                write_script(f"# [ERROR] SELECT returned primary key {prim_key} but it shouldn't have.")
                exit(1)
    elif query_choice == 4:
        # Perform a sum
        if len(list(record_mapping.keys())) == 0:
            print("[WARNING] Couldn't sum because no insertions. Moving on...")
            continue
            
        search_key_low = choice(range(VALUE_MIN, VALUE_MAX))
        search_key_high = choice(range(search_key_low, VALUE_MAX))

        aggregate_col_index = choice(range(NUM_COLUMNS))

        # Perform sum and write to script
        result = query.sum(search_key_low, search_key_high, aggregate_col_index)
        write_script(f"query.sum({search_key_low}, {search_key_high}, {aggregate_col_index})")

        # First, find all the primary keys within the range
        matched_primary_keys = []
        for entry in totals[primary_key_index]:
            if entry[0] >= search_key_low and entry[0] <= search_key_high:
                matched_primary_keys.append(entry[0])

        # Now, get all the slots to sum in the `totals` dictionary
        column_indices = []
        for key in matched_primary_keys:
            if key not in record_mapping:
                # Entry must have been deleted - moving on
                continue

            column_indices.append(record_mapping[key])

        # Finally, calculate the expected sum
        row = totals[aggregate_col_index]
        expected_sum = 0

        i = 0
        for i in range(len(row)):
            if i in column_indices:
                expected_sum += row[i][-1]
        
        if result != expected_sum:
            print(f"[ERROR] Expected SUM to return {expected_sum} but got {result} instead.")
            write_script(f"# [ERROR] Expected SUM to return {expected_sum} but got {result} instead.")
            exit(1)
    elif query_choice == 5:
        # Perform a select on any key that ISN'T the primary key WITH VERSION
        # We'll choose a range between -10 and 0, inclusive
        version = choice(range(-10, 1))
                # Perform a select on another key (may return several records)
        if len(list(record_mapping.keys())) == 0:
            print("[WARNING] Couldn't select by version because no insertions. Moving on...")
            continue
            
        search_key = choice(range(VALUE_MIN, VALUE_MAX))

        search_key_index = choice(range(NUM_COLUMNS))
        while search_key_index == primary_key_index:
            search_key_index = choice(range(NUM_COLUMNS))

        # NOTE - We'll always include the projection index for easier correctness testing
        projection = [choice([0, 1]) for _ in range(NUM_COLUMNS)]
        projection[primary_key_index] = 1

        # Select version and write to script
        results = query.select_version(search_key, search_key_index, projection, version)
        write_script(f"query.select_version({search_key}, {search_key_index}, {projection}, {version})")

        # We need "reconstruct" the records we expect from `totals`
        # Find every entry in the totals column that has the search key
        found_indices = []

        # We need to get all the indices of records with matching fields at the
        # MOST RECENT VERSION
        for i in range(len(totals[search_key_index])):
            if totals[search_key_index][i][-1] == search_key and i in record_mapping.values():
                found_indices.append(i)

        # Now, we also need to get the values they have at the `version` version
        expected_records = [[] for _ in range(len(found_indices))]
        for i in range(NUM_COLUMNS):
            if projection[i] == 0:
                # Skip this column
                continue

            col = totals[i]
            k = 0
            for j in range(len(col)):
                if j in found_indices:
                    entry = col[j]

                    # Subtract one from version because index '0' is really index '-1', '-1' is '-2', and so on
                    adjusted_version = version - 1
                    if abs(adjusted_version) >= len(entry):
                        adjusted_version = 0
                    
                    expected_records[k].append(entry[adjusted_version])
                    k += 1
        
        if len(expected_records) != len(results):
            print(f"[ERROR] Expected SELECT VERSION {version} to return {len(expected_records)} records but got {len(results)}.")
            exit(1)
        
        if len(expected_records) == 0:
            continue # No need to check for correctness since there's nothing to check

        # We have the correct number of records, but are they the _right_ records?
        result_columns = list(map(lambda record: record.columns, results))

        for res in result_columns:
            if res not in expected_records:
                print(f"[ERROR] SELECT VERSION returned {res}, but it was not expected. The expected records are...")
                for exp in expected_records:
                    print(f"- {exp}")
                exit(1)
    elif query_choice == 6:
        # Perform DELETE
        if len(list(record_mapping.keys())) == 0:
            print("[WARNING] Cannot delete because no records exist. Moving on...")
            continue
        
        primary_key = choice(list(record_mapping.keys()))

        # Perform delete and write to log
        query.delete(primary_key)
        write_script(f"query.delete({primary_key})")

        del record_mapping[primary_key]

print(f"[INFO] Success! Ran {NUM_INSERTIONS} random queries without errors or mismatches in behavior.")

# Close the generated script!
fp.close()
