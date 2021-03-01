'''
- program that takes a RAW DATASET and checks consistency among child datasets (AOD and MINIAOD)
- need to work on the duplicate AOD or MINIAOD blocks

'''
import sys
import csv
import json
import os
from stats_rest import Stats2
from json import dumps


stats = Stats2(cookie='stats-cookie.txt')


def das_get_events(dataset):
   if not dataset:
      return 0

   return int(os.popen('dasgoclient --query="dataset=' + dataset + ' | grep dataset.nevents"').read().strip())


def das_get_runs(dataset):
   if not dataset:
      return set()

   stream = os.popen('dasgoclient --query="run dataset=' + dataset + '"')
   return set([int(r.strip()) for r in stream.read().split('\n') if r.strip()])


def get_source_raw_file(file_name):
   rows = []
   with open(file_name) as csv_file:
      csv_reader = csv.reader(csv_file, delimiter="\t")
      for row in csv_reader:
         rows.append(row)
   return rows


def get_workflows_for_input(input_dataset):
   workflows = stats.get_input_dataset(input_dataset)
   workflows = [w for w in workflows if w['RequestType'].lower() not in ('resubmission', 'dqmharvest')]
   return workflows


def get_prepid_and_dataset(workflows, wanted_type, datatier):
   prepid = None
   dataset = None
   dataset_type = None
   events = 0
   for workflow in workflows:
      # print('Workflow: %s' % (workflow['RequestName']))
      latest_info = workflow['EventNumberHistory'][-1]
      for dataset_name, info in latest_info['Datasets'].items():
         # print('Looking at dataset %s, status %s' % (dataset_name, info['Type']))
         if info['Type'] == wanted_type:
            if dataset_name.endswith(datatier):
               prepid = workflow['PrepID']
               dataset = dataset_name
               dataset_type = wanted_type
               events = info['Events']

   return prepid, dataset, dataset_type, events

with open('datasets.txt') as datasets_file:
   datasets = [d.strip() for d in datasets_file.read().split('\n') if d.strip()]

print('Read %s datasets from file' % (len(datasets)))
# high_priority_pd = ['BTagMu','Charmonium','DisplacedJet','DoubleMuon','DoubleMuonLowMass','DoubleMuonLowPU','EGamma','JetHT','MET','MuonEG','MuOnia','SingleMuon','Tau','FSQJet1','FSQJet2','MinimumBias','ZeroBias']
# datasets = [d for d in datasets if d.split('/')[1] in high_priority_pd]
print('Left %s datasets after high priority PD filter' % (len(datasets)))

years = {'2017': {'twiki_file_name': '2017ULdataFromTwiki.txt',
                  'aod_tag': '09Aug2019_UL2017',
                  'miniaod_tag': 'UL2017_MiniAODv2',
                  'nanoaod_tag': 'UL2017_MiniAODv1_NanoAODv2',
                  'dcs_json_path': '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/DCSOnly/json_DCSONLY.txt'},
         '2018': {'twiki_file_name': '2018ULdataFromTwiki.txt',
                  'aod_tag': '12Nov2019_UL2018',
                  'miniaod_tag': 'UL2018_MiniAODv2',
                  'nanoaod_tag': 'UL2018_MiniAODv1_NanoAODv2',
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
      print('\n\n  ***Could not find year info for %s ***\n\n' % (raw_dataset))
      continue

   print('  Year is %s' % (year))
   tag = year_info['aod_tag']
   miniaod_v2_tag = year_info['miniaod_tag']
   nano_aod_tag = year_info['nanoaod_tag']
   dcs_runs = year_info['dcs_json']

   twiki_row = None
   for row in year_info['twiki_file']:
      if row[0] == raw_dataset:
         twiki_row = row

   if twiki_row:
      twiki_runs = set([int(x.strip()) for x in twiki_row[2].strip('[]').split(',') if x.strip()])
   else:
      twiki_runs = set()

   print('  Twiki runs: %s' % (len(twiki_runs)))

   raw_input_workflows = get_workflows_for_input(raw_dataset)
   raw_input_workflows = [w for w in raw_input_workflows if tag == w['ProcessingString']]
   print('  Workflows (%s) with %s as input: %s' % (len(raw_input_workflows), raw_dataset, ','.join([x['RequestName'] for x in raw_input_workflows])))
   # AOD
   aod_prepid, aod_dataset, aod_dataset_status, aod_events = get_prepid_and_dataset(raw_input_workflows, 'VALID', '/AOD')
   if not aod_dataset:
      aod_prepid, aod_dataset, aod_dataset_status, aod_events = get_prepid_and_dataset(raw_input_workflows, 'PRODUCTION', '/AOD')

   print('  AOD: %s (%s)' % (aod_dataset, aod_prepid))

   # MiniAOD v1
   miniaod_v1_workflows = [w for w in raw_input_workflows if miniaod_v2_tag not in w['ProcessingString']]
   print('    MiniAODv1 workflows (%s): %s' % (len(miniaod_v1_workflows), ','.join([x['RequestName'] for x in miniaod_v1_workflows])))
   miniaod_v1_prepid, miniaod_v1_dataset, miniaod_v1_dataset_status, miniaod_v1_events = get_prepid_and_dataset(miniaod_v1_workflows, 'VALID', '/MINIAOD')
   if not miniaod_v1_dataset:
      miniaod_v1_prepid, miniaod_v1_dataset, miniaod_v1_dataset_status, miniaod_v1_events = get_prepid_and_dataset(miniaod_v1_workflows, 'PRODUCTION', '/MINIAOD')

   print('      MiniAODv1: %s (%s)' % (miniaod_v1_dataset, miniaod_v1_prepid))
   # NanoAOD of MiniAOD v1
   miniaod_v1_input_workflows = get_workflows_for_input(miniaod_v1_dataset)
   miniaod_v1_input_workflows = [w for w in miniaod_v1_input_workflows if nano_aod_tag in w['ProcessingString']]
   print('        MiniAODv1\'s NanoAOD workflows (%s): %s' % (len(miniaod_v1_input_workflows), ','.join([x['RequestName'] for x in miniaod_v1_input_workflows])))
   miniaod_v1_nanoaod_prepid, miniaod_v1_nanoaod_dataset, miniaod_v1_nanoaod_dataset_status, miniaod_v1_nanoaod_events = get_prepid_and_dataset(miniaod_v1_input_workflows, 'VALID', '/NANOAOD')
   if not miniaod_v1_nanoaod_dataset:
      miniaod_v1_nanoaod_prepid, miniaod_v1_nanoaod_dataset, miniaod_v1_nanoaod_dataset_status, miniaod_v1_nanoaod_events = get_prepid_and_dataset(miniaod_v1_input_workflows, 'PRODUCTION', '/NANOAOD')

   print('          MiniAODv1\'s NanoAOD: %s (%s)' % (miniaod_v1_nanoaod_dataset, miniaod_v1_nanoaod_prepid))

   # MiniAOD v2
   miniaod_v2_workflows = get_workflows_for_input(aod_dataset)
   miniaod_v2_workflows = [w for w in miniaod_v2_workflows if miniaod_v2_tag in w['ProcessingString']]
   print('    MiniAODv2 workflows (%s): %s' % (len(miniaod_v2_workflows), ','.join([x['RequestName'] for x in miniaod_v2_workflows])))
   miniaod_v2_prepid, miniaod_v2_dataset, miniaod_v2_dataset_status, miniaod_v2_events = get_prepid_and_dataset(miniaod_v2_workflows, 'VALID', '/MINIAOD')
   if not miniaod_v2_dataset:
      miniaod_v2_prepid, miniaod_v2_dataset, miniaod_v2_dataset_status, miniaod_v2_events = get_prepid_and_dataset(miniaod_v2_workflows, 'PRODUCTION', '/MINIAOD')

   print('      MiniAODv2: %s (%s)' % (miniaod_v2_dataset, miniaod_v2_prepid))
   # NanoAOD of MiniAOD v2
   miniaod_v2_input_workflows = get_workflows_for_input(miniaod_v2_dataset)
   miniaod_v2_input_workflows = [w for w in miniaod_v2_input_workflows if nano_aod_tag in w['ProcessingString']]
   print('        MiniAODv2\'s NanoAOD workflows (%s): %s' % (len(miniaod_v2_input_workflows), ','.join([x['RequestName'] for x in miniaod_v2_input_workflows])))
   miniaod_v2_nanoaod_prepid, miniaod_v2_nanoaod_dataset, miniaod_v2_nanoaod_dataset_status, miniaod_v2_nanoaod_events = get_prepid_and_dataset(miniaod_v2_input_workflows, 'VALID', '/NANOAOD')
   if not miniaod_v2_nanoaod_dataset:
      miniaod_v2_nanoaod_prepid, miniaod_v2_nanoaod_dataset, miniaod_v2_nanoaod_dataset_status, miniaod_v2_nanoaod_events = get_prepid_and_dataset(miniaod_v2_input_workflows, 'PRODUCTION', '/NANOAOD')

   print('          MiniAODv2\'s NanoAOD: %s (%s)' % (miniaod_v2_nanoaod_dataset, miniaod_v2_nanoaod_prepid))

   # Input dataset in DAS
   raw_events = das_get_events(raw_dataset)
   raw_runs = das_get_runs(raw_dataset)
   print('  RAW events=%s runs=%s' % (raw_events, len(raw_runs)))

   # AOD dataset in DAS
   # aod_events = das_get_events(aod_dataset)
   aod_runs = das_get_runs(aod_dataset)
   print('  AOD events=%s runs=%s' % (aod_events, len(aod_runs)))

   # MiniAODv1 dataset in DAS
   # miniaod_v1_events = das_get_events(miniaod_v1_dataset)
   miniaod_v1_runs = das_get_runs(miniaod_v1_dataset)
   print('  MiniAODv1 events=%s runs=%s' % (miniaod_v1_events, len(miniaod_v1_runs)))

   # MiniAODv1 NanoAOD dataset in DAS
   # miniaod_v1_nanoaod_events = das_get_events(miniaod_v1_nanoaod_dataset)
   miniaod_v1_nanoaod_runs = das_get_runs(miniaod_v1_nanoaod_dataset)
   print('  MiniAODv1 NanoAOD events=%s runs=%s' % (miniaod_v1_nanoaod_events, len(miniaod_v1_nanoaod_runs)))

   # MiniAODv2 dataset in DAS
   # miniaod_v2_events = das_get_events(miniaod_v2_dataset)
   miniaod_v2_runs = das_get_runs(miniaod_v2_dataset)
   print('  MiniAODv2 events=%s runs=%s' % (miniaod_v2_events, len(miniaod_v2_runs)))

   # MiniAODv1 dataset in DAS
   # miniaod_v2_nanoaod_events = das_get_events(miniaod_v2_nanoaod_dataset)
   miniaod_v2_nanoaod_runs = das_get_runs(miniaod_v2_nanoaod_dataset)
   print('  MiniAODv2 NanoAOD events=%s runs=%s' % (miniaod_v2_nanoaod_events, len(miniaod_v2_nanoaod_runs)))

   # Interesection of RAW dataset runs and DC runs
   raw_dcs_matched_runs = set(raw_runs).intersection(set(dcs_runs))   
   print('  RAW run intersection with DC JSON: %s runs' % (len(raw_dcs_matched_runs)))

   # RAW x DC intersection events
   if raw_dcs_matched_runs:
      raw_dcs_matched_runs_events = os.popen('dasgoclient --query="file run in ' + str(list(raw_dcs_matched_runs)) + ' dataset=' + raw_dataset + ' | sum(file.nevents)"').read()
      raw_dcs_matched_runs_events = int(float(raw_dcs_matched_runs_events.split(' ')[-1]))
      print('  Number of RAW events after run intersection = %s' % (raw_dcs_matched_runs_events))   
   else:
      raw_dcs_matched_runs_events = 0

   item = {'input_dataset': raw_dataset,
            'year': int(year),
            'primary_dataset': raw_dataset.split('/')[1],
            'twiki_runs': len(twiki_runs),
            'twiki_missing_runs': len(raw_dcs_matched_runs - twiki_runs),
            'twiki_surplus_runs': len(twiki_runs - raw_dcs_matched_runs),
            'twiki_runs_length': len(twiki_runs),
            'twiki_runs_diff': len(raw_dcs_matched_runs - twiki_runs) + len(twiki_runs - raw_dcs_matched_runs),
            'raw_runs': len(raw_runs),
            'raw_events': raw_events,
            'raw_dcs_matched_runs': len(raw_dcs_matched_runs),
            'raw_dcs_matched_events': raw_dcs_matched_runs_events,
            # AOD
            'aod_dataset': aod_dataset,
            'aod_dataset_status': aod_dataset_status,
            'aod_prepid': aod_prepid,
            'aod_runs': len(aod_runs),
            'aod_events': aod_events,
            'aod_missing_runs': len(raw_dcs_matched_runs - aod_runs) if aod_dataset else None,
            'aod_surplus_runs': len(aod_runs - raw_dcs_matched_runs) if aod_dataset else None,
            'aod_runs_diff': len(raw_dcs_matched_runs - aod_runs) + len(aod_runs - raw_dcs_matched_runs) if aod_dataset else None,
            'aod_ratio': (float(aod_events) / raw_dcs_matched_runs_events) if aod_dataset and raw_dcs_matched_runs_events else None,
            # MiniAOD v1
            'miniaod_v1_dataset': miniaod_v1_dataset,
            'miniaod_v1_dataset_status': miniaod_v1_dataset_status,
            'miniaod_v1_prepid': miniaod_v1_prepid,
            'miniaod_v1_runs': len(miniaod_v1_runs),
            'miniaod_v1_events': miniaod_v1_events,
            'miniaod_v1_missing_runs': len(raw_dcs_matched_runs - miniaod_v1_runs) if miniaod_v1_dataset else None,
            'miniaod_v1_surplus_runs': len(miniaod_v1_runs - raw_dcs_matched_runs) if miniaod_v1_dataset else None,
            'miniaod_v1_runs_diff': len(raw_dcs_matched_runs - miniaod_v1_runs) + len(miniaod_v1_runs - raw_dcs_matched_runs) if miniaod_v1_dataset else None,
            'miniaod_v1_ratio': (float(miniaod_v1_events) / raw_dcs_matched_runs_events) if miniaod_v1_dataset and raw_dcs_matched_runs_events else None,
            # MiniAOD v1 NanoAOD
            'miniaod_v1_nanoaod_dataset': miniaod_v1_nanoaod_dataset,
            'miniaod_v1_nanoaod_dataset_status': miniaod_v1_nanoaod_dataset_status,
            'miniaod_v1_nanoaod_prepid': miniaod_v1_nanoaod_prepid,
            'miniaod_v1_nanoaod_runs': len(miniaod_v1_nanoaod_runs),
            'miniaod_v1_nanoaod_events': miniaod_v1_nanoaod_events,
            'miniaod_v1_nanoaod_missing_runs': len(raw_dcs_matched_runs - miniaod_v1_nanoaod_runs) if miniaod_v1_nanoaod_dataset else None,
            'miniaod_v1_nanoaod_surplus_runs': len(miniaod_v1_nanoaod_runs - raw_dcs_matched_runs) if miniaod_v1_nanoaod_dataset else None,
            'miniaod_v1_nanoaod_runs_diff': len(raw_dcs_matched_runs - miniaod_v1_nanoaod_runs) + len(miniaod_v1_nanoaod_runs - raw_dcs_matched_runs) if miniaod_v1_nanoaod_dataset else None,
            'miniaod_v1_nanoaod_ratio': (float(miniaod_v1_nanoaod_events) / raw_dcs_matched_runs_events) if miniaod_v1_nanoaod_dataset and raw_dcs_matched_runs_events else None,
            # MiniAOD v2
            'miniaod_v2_dataset': miniaod_v2_dataset,
            'miniaod_v2_dataset_status': miniaod_v2_dataset_status,
            'miniaod_v2_prepid': miniaod_v2_prepid,
            'miniaod_v2_runs': len(miniaod_v2_runs),
            'miniaod_v2_events': miniaod_v2_events,
            'miniaod_v2_missing_runs': len(raw_dcs_matched_runs - miniaod_v2_runs) if miniaod_v2_dataset else None,
            'miniaod_v2_surplus_runs': len(miniaod_v2_runs - raw_dcs_matched_runs) if miniaod_v2_dataset else None,
            'miniaod_v2_runs_diff': len(raw_dcs_matched_runs - miniaod_v2_runs) + len(miniaod_v2_runs - raw_dcs_matched_runs) if miniaod_v2_dataset else None,
            'miniaod_v2_ratio': (float(miniaod_v2_events) / raw_dcs_matched_runs_events) if miniaod_v2_dataset and raw_dcs_matched_runs_events else None,
            # MiniAOD v2 NanoAOD
            'miniaod_v2_nanoaod_dataset': miniaod_v2_nanoaod_dataset,
            'miniaod_v2_nanoaod_dataset_status': miniaod_v2_nanoaod_dataset_status,
            'miniaod_v2_nanoaod_prepid': miniaod_v2_nanoaod_prepid,
            'miniaod_v2_nanoaod_runs': len(miniaod_v2_nanoaod_runs),
            'miniaod_v2_nanoaod_events': miniaod_v2_nanoaod_events,
            'miniaod_v2_nanoaod_missing_runs': len(raw_dcs_matched_runs - miniaod_v2_nanoaod_runs) if miniaod_v2_nanoaod_dataset else None,
            'miniaod_v2_nanoaod_surplus_runs': len(miniaod_v2_nanoaod_runs - raw_dcs_matched_runs) if miniaod_v2_nanoaod_dataset else None,
            'miniaod_v2_nanoaod_runs_diff': len(raw_dcs_matched_runs - miniaod_v2_nanoaod_runs) + len(miniaod_v2_nanoaod_runs - raw_dcs_matched_runs) if miniaod_v2_nanoaod_dataset else None,
            'miniaod_v2_nanoaod_ratio': (float(miniaod_v2_nanoaod_events) / raw_dcs_matched_runs_events) if miniaod_v2_nanoaod_dataset and raw_dcs_matched_runs_events else None,
            }

   results.append(item)

with open('data.json', 'w') as output_file:
   json.dump(results, output_file, indent=2, sort_keys=True)
