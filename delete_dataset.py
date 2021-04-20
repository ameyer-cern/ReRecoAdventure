import json
import sys

dataset = sys.argv[1]
if dataset:
    with open('data.json', 'r') as input_file:
        data = json.load(input_file)

    data = [d for d in data if d['dataset'] != dataset]
    with open('data.json', 'w') as output_file:
        json.dump(data, output_file, indent=1, sort_keys=True)
