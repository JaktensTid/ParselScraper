import re
import csv
import xlwt
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

    with open('geo_locs.csv', 'w') as csvfile:
        fieldnames = list(items[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow(item)


def create_full_report_csv():
    main_data = collection.find({}, {'Legal description' : 1,
                                     'Buildings Valuation Actual Value' : 1,
                                     'Permit cases' : 1,
                                     'Subdivision plat' : 1,
                                     'Land Subtotal Assessed Value' : 1,
                                     'id' : 1,
                                     'Individual built as detail' : 1,
                                     'Property table' : 1,
                                     'Land Subtotal Actual Value' : 1,
                                     'Property within Enterprise Zone' : 1,
                                     'Buildings Valuation Assessed Value' : 1
                                     })
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet("Main data")
    for item in main_data:
        item['Property name'] = item['Property table'][0]['property']
        item['Property owner'] = item['Property table'][0]['owner']
        item['Permit cases'] = item['Permit cases']

    columns = main_data[0].keys()  # list() is not need in Python 2.x
    for i, row in enumerate(main_data):
        for j, col in enumerate(columns):
            sheet.write(i, j, row[col])

    main_data = collection.find({}, {
                                     'id': 1,
                                     'Individual built as detail': 1,
                                     'Property table': 1,

                                     })




if __name__ == '__main__':
    create_geo_locs_csv()


