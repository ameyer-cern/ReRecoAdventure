import json


def del_attr(item, attr_name):
    item.pop(attr_name, None)
    for output_item in item.get('output', []):
        del_attr(output_item, attr_name)


def pick_output_item(items, tags):
    if not items:
        return {}

    for dataset_type in ('VALID', 'PRODUCTION'):
        for tag in tags:
            for item in items:
                if item['type'] == dataset_type and item['processing_string'] == tag:
                    return item

    return {}


aod_tags = {'2016': ['21Feb2020_UL2016', '21Feb2020_UL2016_HIPM'],
            '2017': ['09Aug2019_UL2017_rsb', '09Aug2019_UL2017'],
            '2018': ['12Nov2019_UL2018_rsb_v3', '12Nov2019_UL2018_rsb_v2', '12Nov2019_UL2018_rsb', '12Nov2019_UL2018']}

miniaod_v1_tags = aod_tags

miniaod_v2_tags = {'2016': [],
                   '2017': ['UL2017_MiniAODv2'],
                   '2018': ['UL2018_MiniAODv2']}

miniaod_v1_nanoaod_tags = {'2016': [],
                           '2017': ['UL2017_02Dec2019'],
                           '2018': ['Nano02Dec2019']}

miniaod_v2_nanoaod_tags = {'2016': [],
                           '2017': ['UL2017_MiniAODv1_NanoAODv2'],
                           '2018': ['UL2018_MiniAODv1_NanoAODv2']}

with open('data.json', 'r') as data_file:
    items = json.load(data_file)

print('Read %s items from data.json' % (len(items)))

results = []
for item in items:
    raw_dataset = item['dataset']
    print('Dataset %s' % (raw_dataset))

    aod = pick_output_item(item.get('output'), aod_tags[item['year']])
    miniaod_v1 = pick_output_item(aod.get('output'), miniaod_v1_tags[item['year']])
    miniaod_v2 = pick_output_item(aod.get('output'), miniaod_v2_tags[item['year']])
    miniaod_v1_nanoaod = pick_output_item(miniaod_v1.get('output'), miniaod_v1_nanoaod_tags[item['year']])
    miniaod_v2_nanoaod = pick_output_item(miniaod_v2.get('output'), miniaod_v2_nanoaod_tags[item['year']])

    if aod:
        print('  AOD: %s (%s)' % (aod['dataset'], aod['type']))

    if miniaod_v1:
        print('  MiniAODv1: %s (%s)' % (miniaod_v1['dataset'], miniaod_v1['type']))

    if miniaod_v1_nanoaod:
        print('    MiniAODv1 NanoAOD: %s (%s)' % (miniaod_v1_nanoaod['dataset'], miniaod_v1_nanoaod['type']))

    if miniaod_v2:
        print('  MiniAODv2: %s (%s)' % (miniaod_v2['dataset'], miniaod_v2['type']))

    if miniaod_v2_nanoaod:
        print('    MiniAODv2 NanoAOD: %s (%s)' % (miniaod_v2_nanoaod['dataset'], miniaod_v2_nanoaod['type']))

    twiki_runs = set(item['twiki_runs'])

    raw_x_dcs_runs = set(item['raw_x_dcs_runs'])
    raw_x_dcs_events = item['raw_x_dcs_events']

    raw_runs = set(item['runs'])
    raw_events = item['events']

    whitelist_runs = set(aod.get('aod_whitelist_raw_runs', []))
    whitelist_events = aod.get('aod_whitelist_raw_events', 0)
    whitelist_x_dcs_runs = set(aod.get('aod_whitelist_x_dcs_raw_runs', []))
    whitelist_x_dcs_events = aod.get('aod_whitelist_x_dcs_raw_events', 0)

    result = {'input_dataset': raw_dataset,
              'year': item['year'],
              'primary_dataset': raw_dataset.split('/')[1],
              'twiki_runs': len(twiki_runs),
            #   'raw_x_dcs_twiki_missing_runs': len(raw_x_dcs_runs - twiki_runs),
            #   'raw_x_dcs_twiki_surplus_runs': len(twiki_runs - raw_x_dcs_runs),
            #   'raw_x_dcs_twiki_runs_diff': len(raw_x_dcs_runs - twiki_runs) + len(twiki_runs - raw_x_dcs_runs),
              'whitelist_x_dcs_twiki_missing_runs': len(whitelist_x_dcs_runs - twiki_runs),
              'whitelist_x_dcs_twiki_surplus_runs': len(twiki_runs - whitelist_x_dcs_runs),
              'whitelist_x_dcs_twiki_runs_diff': len(whitelist_x_dcs_runs - twiki_runs) + len(twiki_runs - whitelist_x_dcs_runs),
              'raw_runs': len(raw_runs),
              'raw_events': raw_events,
            #   'raw_x_dcs_runs': len(raw_x_dcs_runs),
            #   'raw_x_dcs_events': raw_x_dcs_events,
              'whitelist_runs': len(whitelist_runs),
              'whitelist_events': whitelist_events,
              'whitelist_x_dcs_runs': len(whitelist_x_dcs_runs),
              'whitelist_x_dcs_events': whitelist_x_dcs_events}


    for prefix, thing in {'aod': aod, 'miniaod_v1': miniaod_v1, 'miniaod_v1_nanoaod': miniaod_v1_nanoaod, 'miniaod_v2': miniaod_v2, 'miniaod_v2_nanoaod': miniaod_v2_nanoaod}.items():
        dataset = thing.get('dataset')
        result[prefix + '_dataset'] = dataset
        result[prefix + '_dataset_status'] = thing.get('type')
        result[prefix + '_prepid'] = thing.get('prepid')
        runs = set(thing.get('runs', []))
        events = thing.get('events', 0)
        result[prefix + '_runs'] = len(runs)
        result[prefix + '_events'] = events
        if dataset:
            # result[prefix + '_raw_x_dcs_missing_runs'] = len(raw_x_dcs_runs - runs)
            # result[prefix + '_raw_x_dcs_surplus_runs'] = len(runs - raw_x_dcs_runs)
            # result[prefix + '_raw_x_dcs_runs_diff'] = result[prefix + '_raw_x_dcs_missing_runs'] + result[prefix + '_raw_x_dcs_surplus_runs']
            # result[prefix + '_raw_x_dcs_ratio'] = float(events) / raw_x_dcs_events if raw_x_dcs_events else None
            result[prefix + '_whitelist_x_dcs_missing_runs'] = len(whitelist_x_dcs_runs - runs)
            result[prefix + '_whitelist_x_dcs_surplus_runs'] = len(runs - whitelist_x_dcs_runs)
            result[prefix + '_whitelist_x_dcs_runs_diff'] = result[prefix + '_whitelist_x_dcs_missing_runs'] + result[prefix + '_whitelist_x_dcs_surplus_runs']
            result[prefix + '_whitelist_x_dcs_ratio'] = float(events) / whitelist_x_dcs_events if whitelist_x_dcs_events else None
        else:
            # result[prefix + '_raw_x_dcs_missing_runs'] = None
            # result[prefix + '_raw_x_dcs_surplus_runs'] = None
            # result[prefix + '_raw_x_dcs_runs_diff'] = None
            # result[prefix + '_raw_x_dcs_ratio'] = None
            result[prefix + '_whitelist_x_dcs_missing_runs'] = None
            result[prefix + '_whitelist_x_dcs_surplus_runs'] = None
            result[prefix + '_whitelist_x_dcs_runs_diff'] = None
            result[prefix + '_whitelist_x_dcs_ratio'] = None

    results.append(result)


with open('data_original_table.json', 'w') as output_file:
    json.dump(results, output_file, indent=1, sort_keys=True)
