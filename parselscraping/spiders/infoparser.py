import re
import csv
from pymongo import MongoClient
from scrapy.conf import settings

identifiers = ['SECT,TWN,RNG:', 'DESC:', 'BLK:', 'LOT:', 'SUB:', 'CONDO:']

def parse_geolocation(legal_desc):
    if legal_desc:
        matches = re.findall(r'(SECT,TWN,RNG:.*-.*-.{0,3} |$)',
                             legal_desc)
        if matches and matches[0]:
            return matches[0].replace("SECT,TWN,RNG:",'').split(' ')[0].strip(), legal_desc.replace(matches[0], '').strip()
        else:
            return '', legal_desc

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
    return item

items = []

if __name__ == '__main__':
    connection = MongoClient(settings['MONGODB_URI'])
    db = connection[settings['MONGODB_DB']]
    collection = db[settings['MONGODB_COLLECTION']]
    for item in collection.find({}):
        id = item['id']
        legal_desc = item['Legal description']
        item = parse_all(legal_desc)
        item.update({'id' : id, 'legal_desc' : legal_desc})
        for ident in identifiers:
            if ident not in item:
                item[ident] = ''
        items.append(item)

    with open('geo_locs.csv', 'w') as csvfile:
         fieldnames = list(items[0].keys())
         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
         writer.writeheader()
         for item in items:
             writer.writerow(item)


