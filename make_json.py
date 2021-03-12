import sys
import csv
import json
import os
import random
from stats_rest import Stats2
from connection_wrapper import ConnectionWrapper
from json import dumps


stats = Stats2(cookie='stats-cookie.txt')
cmsweb = ConnectionWrapper('cmsweb.cern.ch', keep_open=True)


def chunkify(items, chunk_size):
    """
    Yield fixed size chunks of given list
    """
    start = 0
    chunk_size = max(chunk_size, 1)
    while start < len(items):
        yield items[start: start + chunk_size]
        start += chunk_size


def das_get_events(dataset):
    if not dataset:
        return 0

    return int(os.popen('dasgoclient --query="dataset=' + dataset + ' | grep dataset.nevents"').read().strip())


def das_get_events_of_runs(dataset, runs, try_to_chunkify=True):
    if not dataset or not runs:
        return 0

    runs = sorted(list(runs))
    try:
        print('  Getting events of %s runs of %s' % (len(runs), dataset))
        command = 'dasgoclient --query="file run in ' + str(list(runs)).replace(' ', '') + ' dataset=' + dataset + ' | sum(file.nevents)"'
        events = os.popen(command).read()
        events = int(float(events.split(' ')[-1]))
        print('  Got %s events' % (events))
        return events
    except:
        print('Error while getting events for %s with %s runs, trying to chunkify' % (dataset, len(runs)))
        if try_to_chunkify:
            events = 0
            for chunk in chunkify(runs, 50):
                events += das_get_events_of_runs(dataset, chunk, False)

            return events
    
    return 0

def das_get_runs(dataset):
    if not dataset:
        return []

    stream = os.popen('dasgoclient --query="run dataset=' + dataset + '"')
    return set([int(r.strip()) for r in stream.read().split('\n') if r.strip()])


def get_twiki_file(file_name):
    if not file_name:
        return []

    rows = []
    with open(file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            rows.append(row)
    return rows


def get_dcs_json_runs(file_name):
    if not file_name:
        return []

    with open(file_name) as dcs_file:
        runs = [int(x) for x in json.load(dcs_file).keys()]

    return runs


def get_workflows_for_input(input_dataset):
    workflows = stats.get_input_dataset(input_dataset)
    workflows = [w for w in workflows if w['RequestType'].lower() not in ('resubmission', 'dqmharvest')]
    return workflows


def get_workflow(workflow_name):
    workflow = cmsweb.api('GET', '/couchdb/reqmgr_workload_cache/%s' % (workflow_name))
    workflow = json.loads(workflow)
    return workflow


def get_prepid_and_dataset(workflows, datatiers):
    if not datatiers:
       return []

    results = []
    for workflow in workflows:
        latest_info = workflow['EventNumberHistory'][-1]
        for dataset_name, info in latest_info['Datasets'].items():
            if info['Type'] in ('PRODUCTION', 'VALID'):
                ds_datatier = dataset_name.split('/')[-1]
                if ds_datatier == datatiers[0]:
                    prepid = workflow['PrepID']
                    dataset = dataset_name
                    dataset_type = info['Type']
                    events = info['Events']
                    processing_string = workflow['ProcessingString']
                    for res in results:
                        if res['dataset'] == dataset:
                            break
                    else:
                        runs = das_get_runs(dataset)
                        item = {'dataset': dataset,
                                'type': dataset_type,
                                'prepid': prepid,
                                'runs': list(runs),
                                'events': events,
                                'output': get_prepid_and_dataset([workflow], datatiers[1:]),
                                'workflow': workflow['RequestName'],
                                'processing_string': processing_string}
                        item['output'].extend(get_prepid_and_dataset(get_workflows_for_input(dataset), datatiers[1:]))
                        results.append(item)

    return results


with open('datasets.txt') as datasets_file:
    datasets = list(set([d.strip() for d in datasets_file.read().split('\n') if d.strip()]))


print('Read %s datasets from file' % (len(datasets)))
if '--debug' in sys.argv:
    random.shuffle(datasets)
    datasets = datasets[:10]
    print('Picking random 10 datasets because debug')

datasets = sorted(datasets)


years = {'2016': {'twiki_file_name': '2016ULdataFromTwiki.txt',
                  'aod_tag': ['21Feb2020_UL2016', '21Feb2020_ver1_UL2016_HIPM', '21Feb2020_ver2_UL2016_HIPM', '21Feb2020_UL2016_HIPM_rsb'],
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/DCSOnly/json_DCSONLY.txt'},
         '2017': {'twiki_file_name': '2017ULdataFromTwiki.txt',
                  'aod_tag': ['09Aug2019_UL2017'],
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/DCSOnly/json_DCSONLY.txt'},
         '2018': {'twiki_file_name': '2018ULdataFromTwiki.txt',
                  'aod_tag': ['12Nov2019_UL2018'],
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/DCSOnly/json_DCSONLY.txt'}}


for year, year_info in years.items():
    year_info['twiki_file'] = get_twiki_file(year_info['twiki_file_name'])
    year_info['dcs_json'] = get_dcs_json_runs(year_info['dcs_json_path'])


results = []
for index, raw_dataset in enumerate(datasets):
    print('%s/%s. Dataset is %s' % (index + 1, len(datasets), raw_dataset))
    for year, year_info in years.items():
        if '/Run%s' % (year) in raw_dataset:
            break
    else:
        print('  ***Could not find year info for %s ***' % (raw_dataset))
        continue

    # Year
    print('  Year is %s' % (year))
    aod_tags = year_info['aod_tag']
    dcs_runs = year_info['dcs_json']
    # TWiki row
    twiki_runs = set()
    for row in year_info['twiki_file']:
        if row[0] == raw_dataset:
           twiki_runs = set([int(x.strip()) for x in row[2].strip('[]').split(',') if x.strip()])
           break

    print('  Twiki runs: %s' % (len(twiki_runs)))
    # RAW dataset
    raw_input_workflows = get_workflows_for_input(raw_dataset)
    raw_input_workflows = [w for w in raw_input_workflows if [tag for tag in aod_tags if tag in w['ProcessingString']]]
    raw_events = das_get_events(raw_dataset)
    raw_runs = das_get_runs(raw_dataset)
    raw_dcs_matched_runs = set(raw_runs).intersection(set(dcs_runs))
    print('  RAW run intersection with DC JSON: %s runs' % (len(raw_dcs_matched_runs)))
    # RAW x DC intersection events
    if raw_dcs_matched_runs:
        raw_dcs_matched_runs_events = das_get_events_of_runs(raw_dataset, raw_dcs_matched_runs)
        print('  Number of RAW events after run intersection = %s' % (raw_dcs_matched_runs_events))
    else:
        raw_dcs_matched_runs_events = 0

    # AOD, MiniAOD, NanoAOD
    aod_workflows = get_prepid_and_dataset(raw_input_workflows, ['AOD', 'MINIAOD', 'NANOAOD'])
    item = {'dataset': raw_dataset,
            'output': aod_workflows,
            'events': raw_events,
            'twiki_runs': list(twiki_runs),
            'year': year,
            'runs': list(raw_runs),
            'raw_x_dcs_runs': list(raw_dcs_matched_runs),
            'raw_x_dcs_events': raw_dcs_matched_runs_events}

    for aod_item in aod_workflows:
        whitelist_runs = set(get_workflow(aod_item['workflow'])['RunWhitelist'])
        print('  Whitelist runs for %s: %s' % (aod_item['dataset'], len(whitelist_runs)))
        aod_item['aod_whitelist_raw_runs'] = list(whitelist_runs)
        aod_item['aod_whitelist_raw_events'] = das_get_events_of_runs(raw_dataset, aod_item['aod_whitelist_raw_runs'])
        aod_item['aod_whitelist_x_dcs_raw_runs'] = list(whitelist_runs.intersection(set(dcs_runs)))
        aod_item['aod_whitelist_x_dcs_raw_events'] = das_get_events_of_runs(raw_dataset, aod_item['aod_whitelist_x_dcs_raw_runs'])

    results.append(item)


with open('data.json', 'w') as output_file:
    json.dump(results, output_file, indent=1, sort_keys=True)
