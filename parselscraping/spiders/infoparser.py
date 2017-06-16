import re
import csv
from collections import defaultdict
from pymongo import MongoClient
from scrapy.conf import settings

connection = MongoClient(settings['MONGODB_URI'])
db = connection[settings['MONGODB_DB']]
collection = db[settings['MONGODB_COLLECTION']]

identifiers = ['SECT,TWN,RNG:', 'DESC:', 'BLK:', 'LOT:', 'SUB:', 'CONDO:']
identifiers_subdiv = ['SUBD', 'SUBDIVISION', 'FILING NO', 'FLG NO', 'FILING NO']
identifiers_condo = ['CONDOMINIUMS', 'BUILDING', 'BLDG', 'UNIT']

def parse_geolocation(legal_desc):
    if legal_desc:
        matches = re.findall(r'(SECT,TWN,RNG:.*-.*-.{0,3} |$)',
                             legal_desc)
        if matches and matches[0]:
            return matches[0].replace("SECT,TWN,RNG:",'').split(' ')[0].strip(), legal_desc.replace(matches[0], '').strip()
        else:
            return '', legal_desc

def create_geo_locs_csv():
    items = []

    def parse_all(legal_desc):
        item = {}
        for ident in identifiers:
            if ident in legal_desc:
                for ident2 in identifiers:
                    matches = re.findall(r'(%s.*%s)' % (ident, ident2), legal_desc)
                    if matches and matches[0]:
                        replaced = matches[0].replace(ident, '').replace(ident2, '').strip()
                        if not any(ident in replaced for ident in identifiers):
                            item[ident] = replaced
                            break
                    else:
                        item[ident] = ''
        for key in item:
            if not item[key]:
                matches = re.findall(r'(%s.*$)' % key, legal_desc)
                if matches and matches[0]:
                    replaced = matches[0].replace(key, '').strip()
                    if not any(ident in replaced for ident in identifiers):
                        item[key] = replaced
        for ident in identifiers:
            if ident not in item:
                item[ident] = ''
        if item[identifiers[0]]:
            splitted = item[identifiers[0]].split('-')
            if len(splitted) > 0: item['sec'] = splitted[0]
            if len(splitted) > 1: item['twp'] = splitted[1]
            if len(splitted) > 2: item['rng'] = splitted[2]
        else:
            item['sec'], item['twp'], item['rng'] = '', '', ''
        for key in item:
            if item[key]:
                item['Subdivision'] = ''
                item['Condo'] = ''
                return item
        for ident in identifiers_condo:
            if ident in legal_desc:
                item['Condo'] = legal_desc
                item['Subdivision'] = ''
                return item

        for ident in identifiers_subdiv:
            if ident in legal_desc:
                item['Condo'] = ''
                item['Subdivision'] = legal_desc
                return item
        return item

    for item in collection.find({}):
        id = item['id']
        legal_desc = item['Legal description']
        item = parse_all(legal_desc)
        item.update({'id': id, 'legal_desc': legal_desc})

        items.append(item)

    return items


def create_full_report_csv():
    def unpack(data, key):
        new = []
        for item in data:
            for n in item[key]:
                new_item = {}
                new_item.update(item)
                del new_item[key]
                new_item.update(n)
                new.append(new_item)

        return new

    main_data = list(collection.find({}, {'Legal description' : 1,
                                     'Buildings Valuation Actual Value' : 1,
                                     'Permit cases' : 1,
                                     'Subdivision plat' : 1,
                                     'Land Subtotal Assessed Value' : 1,
                                     'id' : 1,
                                     'Property table' : 1,
                                     'Land Subtotal Actual Value' : 1,
                                     'Property within Enterprise Zone' : 1,
                                     'Buildings Valuation Assessed Value' : 1,
                                    '_id' : 0
                                     }))
    for item in main_data:
        item['Property name'] = item['Property table'][0]['property']
        splited = item['Property table'][0]['owner'].split(',')
        item['Owner name'], item['Owner address'] = splited[0], ''.join(splited[1:])
        item['Owner name'] = item['Owner name'].replace('AND', ' AND ')
        del item['Property table']
        item['Permit cases'] = ', '.join(item['Permit cases'])
    columns = list(main_data[0].keys())
    with open('main_data.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in main_data:
            writer.writerow(item)

    land_val_summary = list(collection.find({}, {
        'id': 1,
        'Property table': 1,
        'Land Valuation Summary': 1,
        '_id': 0
    }))
    new_land_val_summary = unpack(land_val_summary, 'Land Valuation Summary')
    for item in new_land_val_summary:
        item['Property name'] = item['Property table'][0]['property']
        splited = item['Property table'][0]['owner'].split(',')
        item['Owner name'], item['Owner address'] = splited[0], ''.join(splited[1:])
        item['Owner name'] = item['Owner name'].replace('AND', ' AND ')
        del item['Property table']
    columns = list(new_land_val_summary[0].keys())
    with open('land_valuation_summary.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in new_land_val_summary:
            writer.writerow(item)

    d = defaultdict(dict)
    for l in (main_data, new_land_val_summary):
        for elem in l:
            d[elem['id']].update(elem)
    l3 = d.values()
    columns = list(set(list(main_data[0].keys()) + list(new_land_val_summary[0].keys())))
    with open('land_valuation_summary-main_data-merged.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in l3:
            writer.writerow(item)

    get_locs = create_geo_locs_csv()
    d = defaultdict(dict)
    for l in (l3, get_locs):
        for elem in l:
            d[elem['id']].update(elem)
    l3 = d.values()
    columns = list(columns + list(get_locs[0].keys()))
    with open('land_valuation_summary-main_data-geo_locs-merged.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in l3:
            writer.writerow(item)


    account_summary = list(collection.find({}, {
                                     'id': 1,
                                     'Property table': 1,
                                     'Account summary' : 1,
                                    '_id' : 0
                                     }))
    new_account_summary = unpack(account_summary, 'Account summary')
    for item in new_account_summary:
        item['Property name'] = item['Property table'][0]['property']
        splited = item['Property table'][0]['owner'].split(',')
        item['Owner name'], item['Owner address'] = splited[0], ''.join(splited[1:])
        item['Owner name'] = item['Owner name'].replace('AND', ' AND ')
        del item['Property table']
    columns = list(new_account_summary[0].keys())
    with open('account_summary.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in new_account_summary:
            writer.writerow(item)

    sales_summary = list(collection.find({}, {
        'id': 1,
        'Property table': 1,
        'Sales summary': 1,
            '_id' : 0
    }))
    new_sales_summary = unpack(sales_summary, 'Sales summary')
    for item in new_sales_summary:
        item['Property name'] = item['Property table'][0]['property']
        splited = item['Property table'][0]['owner'].split(',')
        item['Owner name'], item['Owner address'] = splited[0], ''.join(splited[1:])
        item['Owner name'] = item['Owner name'].replace('AND', ' AND ')
        del item['Property table']
    columns = list(new_sales_summary[0].keys())
    with open('sales_summary.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in new_sales_summary:
            writer.writerow(item)

    buildings_val_summary = list(collection.find({}, {
        'id': 1,
        'Property table': 1,
        'Buildings Valuation Summary': 1,
        '_id': 0
    }))
    new_buildings_val_summary = unpack(buildings_val_summary, 'Buildings Valuation Summary')
    for item in new_buildings_val_summary:
        item['Property name'] = item['Property table'][0]['property']
        splited = item['Property table'][0]['owner'].split(',')
        item['Owner name'], item['Owner address'] = splited[0], ''.join(splited[1:])
        item['Owner name'] = item['Owner name'].replace('AND', ' AND ')
        del item['Property table']
    columns = list(new_buildings_val_summary[0].keys())
    with open('buildings_val_summary.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for item in new_buildings_val_summary:
            writer.writerow(item)


if __name__ == '__main__':
    create_full_report_csv()


