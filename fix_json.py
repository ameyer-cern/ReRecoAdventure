import json

with open('data.json') as json_file:
    rows = json.load(json_file)

for row in rows:
    if not row['aod_dataset']:
        row['aod_missing_runs'] = None
        row['aod_surplus_runs'] = None
        row['aod_runs_diff'] = None
        row['aod_ratio'] = None
    # MiniAOD v1
    if not row['miniaod_v1_dataset']:
        row['miniaod_v1_missing_runs'] = None
        row['miniaod_v1_surplus_runs'] = None
        row['miniaod_v1_runs_diff'] = None
        row['miniaod_v1_ratio'] = None
    # MiniAOD v1 NanoAOD
    if not row['miniaod_v1_nanoaod_dataset']:
        row['miniaod_v1_nanoaod_missing_runs'] = None
        row['miniaod_v1_nanoaod_surplus_runs'] = None
        row['miniaod_v1_nanoaod_runs_diff'] = None
        row['miniaod_v1_nanoaod_ratio'] = None
    # MiniAOD v2
    if not row['miniaod_v2_dataset']:
        row['miniaod_v2_missing_runs'] = None
        row['miniaod_v2_surplus_runs'] = None
        row['miniaod_v2_runs_diff'] = None
        row['miniaod_v2_ratio'] = None
    # MiniAOD v2 NanoAOD
    if not row['miniaod_v2_nanoaod_dataset']:
        row['miniaod_v2_nanoaod_missing_runs'] = None
        row['miniaod_v2_nanoaod_surplus_runs'] = None
        row['miniaod_v2_nanoaod_runs_diff'] = None
        row['miniaod_v2_nanoaod_ratio'] = None

with open('data2.json', 'w') as output_file:
   json.dump(rows, output_file, indent=2, sort_keys=True)
