import json


def del_attr(item, attr_name):
    item.pop(attr_name, None)
    for output_item in item.get('output', []):
        del_attr(output_item, attr_name)


def get_output_rows(item, path, rows):
    path = list(path)
    path.append(item)
    if not item.get('output'):
        item.pop('output', None)
        rows.append(path)
    else:
        for i in item.pop('output'):
            get_output_rows(i, path, rows)


def process_item(item):
    for attr in ('runs', 'raw_x_dcs_runs', 'aod_whitelist_raw_runs', 'aod_whitelist_x_dcs_raw_runs', 'twiki_runs'):
        if item.get(attr) is not None:
            item[attr] = len(item[attr])

    for output_item in item.get('output', []):
        process_item(output_item)


with open('data.json', 'r') as data_file:
    items = json.load(data_file)

print('Read %s items from data.json' % (len(items)))

results = []
for item in items:
    # Show only lengths of run lists
    process_item(item)
    # Rows of table
    output_rows = []
    get_output_rows(item, [], output_rows)
    results.extend(output_rows)

with open('data_full_table.json', 'w') as output_file:
    json.dump(results, output_file, indent=2, sort_keys=True)
