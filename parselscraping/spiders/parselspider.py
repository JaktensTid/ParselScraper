import csv
import scrapy
from scrapy import log, signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.conf import settings
from pymongo import MongoClient

count = 0

class ParselSpider(scrapy.Spider):
    connection = MongoClient(settings['MONGODB_URI'])
    db = connection[settings['MONGODB_DB']]
    collection = db[settings['MONGODB_COLLECTION']]
    name = 'parselspider'

    all_parsels = list([d['\ufeffPARCELNB'] for d in csv.DictReader(open('./spiders/parsels.csv'))])
    found = set(['0' + d['id'] for d in collection.find({}, {'id' : 1})])
    start_urls = list(['https://gisapp.adcogov.org/quicksearch/doreport.aspx?pid=%s' % i
                  for i in list(set(all_parsels) - found)])
    print('Urls count: ' + str(len(start_urls)))

    def extract_text(self, selector):
        result = selector.xpath(".//text()").extract()
        if result:
            return result[0].replace(u'\xa0', u'')
        else:
            return ''

    def parse(self, response):
        if response.status == 200 and \
            'The parcel you have selected is a parcel with multiple accounts' not in response.text:
            id = response.url[-12:]
            item = {'id' : id, 'Property table' : []}

            for i, tr in enumerate(response.xpath(".//table[@id='propertyAddressTable']//tr[2]")):
                owner = ','.join([entry for entry in tr.xpath(".//td[@id='ownerContentCell']/span//text()").extract() if entry])
                property = ','.join([entry
                    for entry in tr.xpath(".//td[@id='propertyContentCell']/span//text()").extract() if entry])
                item['Property table'].append({'owner' : owner, 'property' : property})
                break

            item['Legal description'] = response.xpath('.//span[@id="propertyReport"]/span[2]/span[9]/div/span//text()').extract()[0]
            item['Subdivision plat'] = response.xpath('//*[@id="propertyReport"]/span[2]/span[12]/div/span//text()').extract()[0]

            item['Account summary'] = []
            for tr in response.xpath("//table[contains(.,'Account Numbers')]/tr")[1:]:
                spans = tr.xpath(".//td//text()").extract()
                item['Account summary'].append({"Account numbers" : spans[0],
                                                "Date added" : spans[1],
                                                "Tax District" : spans[2],
                                                "Mill Levy" : spans[3]})

            item['Permit cases'] = response.xpath('.//span[@id="propertyReport"]/span[4]/span[3]/div/a//text()').extract()

            item['Sales summary'] = []
            for tr in response.xpath(".//table[contains(.,'Deed Type')]/tr")[1:]:
                tds = tr.xpath(".//td")
                item['Sales summary'].append({"Sale Date" : self.extract_text(tds[0]),
                                                "Sale Price" : self.extract_text(tds[1]),
                                                "Deed Type" : self.extract_text(tds[2]),
                                                "Reception Number" : self.extract_text(tds[3]),
                                                "Book" : self.extract_text(tds[4]),
                                                "Page" : self.extract_text(tds[5]),
                                                "Grantor" : self.extract_text(tds[6]),
                                                "Grantee" : self.extract_text(tds[7]),
                                                "Doc Fee" : self.extract_text(tds[8]),
                                                "Doc Date" : self.extract_text(tds[9])})

            item['Land Valuation Summary'] = []
            valuation_trs = response.xpath(".//table[contains(.,'Unit of Measure')]/tr")
            for tr in valuation_trs[1:-2]:
                tds = tr.xpath(".//td")
                lvs = {"Land Type": self.extract_text(tds[0]),
                                              "Unit of Measure": self.extract_text(tds[1]),
                                              "Number of Units": self.extract_text(tds[2]),
                                              "Fire District": self.extract_text(tds[3]),
                                              "School District": self.extract_text(tds[4]),
                                              "Vacant/Improved": self.extract_text(tds[5]),
                                              "Actual Value": self.extract_text(tds[6]),
                                              "Assessed Value": self.extract_text(tds[7])}
                item['Land Valuation Summary'].append(lvs)

            item['Land Subtotal Actual Value'] = valuation_trs[-1].xpath(".//td//text()")[-2].extract()
            item['Land Subtotal Assessed Value'] = valuation_trs[-1].xpath(".//td//text()")[-1].extract()

            item['Buildings Valuation Summary'] = []
            valuation_trs = response.xpath(".//table[contains(.,'Building Number')]/tr")
            for tr in valuation_trs[1:-2]:
                tds = tr.xpath(".//td")
                item['Buildings Valuation Summary'].append({"Building Number": self.extract_text(tds[0]),
                                                       "Property Type": self.extract_text(tds[1]),
                                                       "Actual Value": self.extract_text(tds[2]),
                                                       "Assessed Value": self.extract_text(tds[3])})

            item['Buildings Valuation Actual Value'] = valuation_trs[-1].xpath(".//td//text()")[-2].extract()
            item['Buildings Valuation Assessed Value'] = valuation_trs[-1].xpath(".//td//text()")[-1].extract()

            item['Individual Built As Detail'] = {}
            valuation_trs = response.xpath(".//table[contains(.,'Built As')]/tr")
            for tr in valuation_trs:
                tds = tr.xpath(".//td")
                item['Individual Built As Detail'][self.extract_text(tds[0]).replace(':', '')] = self.extract_text(tds[1])
                item['Individual Built As Detail'][self.extract_text(tds[2]).replace(':', '')] = self.extract_text(tds[3])

            item['Property within Enterprise Zone'] = response.xpath('//*[@id="propertyReport"]/span[15]/span[11]/div/span//text()').extract()[0]
            yield item
