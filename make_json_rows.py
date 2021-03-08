'''
- program that takes a RAW DATASET and checks consistency among child datasets (AOD and MINIAOD)
- need to work on the duplicate AOD or MINIAOD blocks

'''
import sys
import csv
import json
import os
import random
from stats_rest import Stats2
from json import dumps


stats = Stats2(cookie='stats-cookie.txt')


def das_get_events(dataset):
   if not dataset:
      return 0

   return int(os.popen('dasgoclient --query="dataset=' + dataset + ' | grep dataset.nevents"').read().strip())


def das_get_runs(dataset):
   if not dataset:
      return []

   stream = os.popen('dasgoclient --query="run dataset=' + dataset + '"')
   return set([int(r.strip()) for r in stream.read().split('\n') if r.strip()])


def get_source_raw_file(file_name):
   rows = []
   if not file_name:
      return rows

   with open(file_name) as csv_file:
      csv_reader = csv.reader(csv_file, delimiter="\t")
      for row in csv_reader:
         rows.append(row)
   return rows


def get_workflows_for_input(input_dataset):
   workflows = stats.get_input_dataset(input_dataset)
   workflows = [w for w in workflows if w['RequestType'].lower() not in ('resubmission', 'dqmharvest')]
   return workflows


def get_output_rows(item, path, rows):
   path = list(path)
   path.append(item)
   if not item.get('output'):
      item.pop('output', None)
      rows.append(path)
   else:
      for i in item.pop('output'):
         get_output_rows(i, path, rows)


def get_prepid_and_dataset(workflows, datatiers, raw_x_dcs_runs, raw_x_dcs_events):
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
                          'total_events': events,
                          'prepid': prepid,
                          'height': 1,
                          'runs': len(runs),
                          'missing_runs': len(raw_x_dcs_runs - runs),
                          'surplus_runs': len(runs - raw_x_dcs_runs),
                          'events': events,
                          'events_difference': events - raw_x_dcs_events,
                          'fraction': (events / float(raw_x_dcs_events)) if raw_x_dcs_events else 0,
                          'output': get_prepid_and_dataset([workflow],
                                                           datatiers[1:],
                                                           raw_x_dcs_runs,
                                                           raw_x_dcs_events),
                          'processing_string': processing_string}
                  item['output'].extend(get_prepid_and_dataset(get_workflows_for_input(dataset),
                                                               datatiers[1:],
                                                               raw_x_dcs_runs,
                                                               raw_x_dcs_events))
                  results.append(item)

   return results

with open('datasets.txt') as datasets_file:
   datasets = [d.strip() for d in datasets_file.read().split('\n') if d.strip()]
   datasets = list(set(datasets))

print('Read %s datasets from file' % (len(datasets)))
# high_priority_pd = ['BTagMu','Charmonium','DisplacedJet','DoubleMuon','DoubleMuonLowMass','DoubleMuonLowPU','EGamma','JetHT','MET','MuonEG','MuOnia','SingleMuon','Tau','FSQJet1','FSQJet2','MinimumBias','ZeroBias']
# datasets = [d for d in datasets if d.split('/')[1] in high_priority_pd]
print('Left %s datasets after high priority PD filter' % (len(datasets)))
# random.shuffle(datasets)
# datasets = datasets[:15]
datasets = sorted(datasets)

years = {'2016': {'twiki_file_name': '2016ULdataFromTwiki.txt',
                  'aod_tag': '21Feb2020_UL2016',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/DCSOnly/json_DCSONLY.txt'},
         '2017': {'twiki_file_name': '2017ULdataFromTwiki.txt',
                  'aod_tag': '09Aug2019_UL2017',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/DCSOnly/json_DCSONLY.txt'},
         '2018': {'twiki_file_name': '2018ULdataFromTwiki.txt',
                  'aod_tag': '12Nov2019_UL2018',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/DCSOnly/json_DCSONLY.txt'}}

for year, year_info in years.items():
   year_info['twiki_file'] = get_source_raw_file(year_info['twiki_file_name'])
   with open(year_info['dcs_json_path']) as dcs_file:
      year_info['dcs_json'] = [int(x) for x in json.load(dcs_file).keys()]


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
   tag = year_info['aod_tag']
   dcs_runs = year_info['dcs_json']
   # TWiki
   twiki_runs = set()
   for row in year_info['twiki_file']:
      if row[0] == raw_dataset:
         twiki_runs = set([int(x.strip()) for x in row[2].strip('[]').split(',') if x.strip()])

   print('  Twiki runs: %s' % (len(twiki_runs)))
   # RAW dataset
   raw_input_workflows = get_workflows_for_input(raw_dataset)
   raw_input_workflows = [w for w in raw_input_workflows if tag in w['ProcessingString']]
   raw_events = das_get_events(raw_dataset)
   raw_runs = das_get_runs(raw_dataset)
   raw_dcs_matched_runs = set(raw_runs).intersection(set(dcs_runs))   
   print('  RAW run intersection with DC JSON: %s runs' % (len(raw_dcs_matched_runs)))
   # RAW x DC intersection events
   if raw_dcs_matched_runs:
      raw_dcs_matched_runs_events = os.popen('dasgoclient --query="file run in ' + str(list(raw_dcs_matched_runs)) + ' dataset=' + raw_dataset + ' | sum(file.nevents)"').read()
      raw_dcs_matched_runs_events = int(float(raw_dcs_matched_runs_events.split(' ')[-1]))
      print('  Number of RAW events after run intersection = %s' % (raw_dcs_matched_runs_events))   
   else:
      raw_dcs_matched_runs_events = 0

   # AOD, MiniAOD, NanoAOD
   aod_workflows = get_prepid_and_dataset(raw_input_workflows, ['AOD', 'MINIAOD', 'NANOAOD'], raw_dcs_matched_runs, raw_dcs_matched_runs_events)
   item = {'dataset': raw_dataset,
           'output': aod_workflows,
           'events': raw_events,
           'twiki_missing_runs': len(raw_dcs_matched_runs - twiki_runs),
           'twiki_surplus_runs': len(twiki_runs - raw_dcs_matched_runs),
           'twiki_runs': len(twiki_runs),
           'year': year,
           'runs': len(raw_runs),
           'raw_x_dcs_runs': len(raw_dcs_matched_runs),
           'raw_x_dcs_events': raw_dcs_matched_runs_events,
           'height': 1}

   # Rows of table
   output_rows = []
   get_output_rows(item, [], output_rows)
   # output_rows = json.loads(json.dumps(output_rows)) # Because deepcopy is useless heap of shit
   # Merge cells of table 
   # for i in range(len(output_rows) - 1, 0, -1):
   #    this_row = output_rows[i]
   #    other_row = output_rows[i - 1]
   #    for j in range(min(len(this_row), len(other_row))):
   #       if this_row[j]['dataset'] != other_row[j]['dataset']:
   #          break

   #       other_row[j]['height'] += this_row[j]['height']
   #       this_row[j]['height'] = 0

   # for row in output_rows:
   #    while row[0]['height'] == 0:
   #       row.pop(0)

   results.extend(output_rows)

with open('data.json', 'w') as output_file:
   json.dump(results, output_file, indent=2, sort_keys=True)
