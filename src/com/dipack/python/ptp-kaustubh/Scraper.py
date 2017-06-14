import requests
import logging
from urllib.parse import urlparse, urljoin, parse_qs
from bs4 import BeautifulSoup as bs
from itertools import count, product, islice
from string import ascii_uppercase
from robobrowser import RoboBrowser
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

fields = ['Vehicle No', 'License No', 'Payment URL', 'Compounding Fees',
          'Offences', 'Offense Time', 'Offender Mobile No', 'Payment Status',
          'Offense Date', 'Challan No', 'Sections', 'Evidences',
          'Impounded Document']

mockData = {'Offense Time': '11:43:51', 'Offender Mobile No': 'NA',
            'Evidences': ['http://punetrafficop.online:8080/File/PTPCHC170504000978_01.png',
                          'http://punetrafficop.online:8080/File/PTPCHC170504000978_02.png'], 'License No': 'NA',
            'Impounded Document': 'No Impound', 'Compounding Fees': '700', 'Vehicle No': 'MH12JB2300',
            'Offense Date': '2017-05-04',
            'Offences': [{'Offenses': 'Halting ahead white line', 'Fine Amount': '200', 'Sections': '19(1)/177 MVA'},
                         {'Offenses': 'Without Helmet', 'Fine Amount': '500', 'Sections': '129/177'}],
            'Payment Status': 'Pending', 'Challan No': 'PTPCHC170504000978',
            'Payment URL': ['https://punetrafficop.net/'], 'Sections': 'Fine Amount'}

excelColumnHeaderOrder = [
    'Challan No',
    'Vehicle No',
    'License No',
    'Offense Date',
    'Offense Time',
    'Offender Mobile No',
    'Offences',
    'Compounding Fees',
    'Evidences',
    'Impounded Document',
    'Payment Status',
    'Payment URL'
]


class Scraper:
    def __init__(self):
        logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p',
                            level=logging.INFO)

    @staticmethod
    def multiletters(seq):
        for n in count(1):
            for s in product(seq, repeat=n):
                yield ''.join(s)

    @staticmethod
    def license_plate_generator(licenseCharList):
        for rto in range(46):
            for charSeq in licenseCharList:
                for plateNumber in range(1000, 9999):
                    yield ''.join(['MH', (rto + 1).__str__().zfill(2), charSeq, plateNumber.__str__()])

    def get_challans_for_plate(self, licensePlate):
        ptpUrl = 'https://punetrafficop.net'

        allChallans = {}

        browser = RoboBrowser()
        browser.open(ptpUrl)
        challanForm = browser.get_form('frm')
        challanForm['categoryNo'].value = licensePlate
        browser.submit_form(challanForm)

        resultTable = browser.select('table.table')
        output = resultTable[0].contents[3].contents
        reducedList = [output[i] for i in range(len(output)) if i % 2 != 0]
        if 'No Record(s) Found' not in reducedList[0].text:
            reducedList.pop()

            tableValueKey = {
                0: 'challan_number',
                1: 'challan_date',
                2: 'driver_name',
                3: 'vehicle_number',
                4: 'license_number',
                5: 'compounding_fee',
                6: 'view_link',
                7: 'pay_link',
                8: 'download_link'
            }

            for challan in reducedList:
                challanContents = {}
                challanContentsList = [challan.contents[i] for i in range(len(challan)) if i % 2 != 0]
                for index, val in enumerate(challanContentsList):
                    if index < 6:
                        challanContents[tableValueKey[index]] = val.text
                allChallans[challanContents[tableValueKey[0]]] = challanContents

        return allChallans

    def get_challan_info(self, challanNumber):
        dataDict = {}
        getChallanInfoUrl = 'http://punetrafficop.net/y/x?x='
        getInfoForChallanUrl = getChallanInfoUrl + challanNumber
        response = requests.get(getInfoForChallanUrl)
        dataTable = bs(response.content, 'lxml').find_all('tr')
        for row in dataTable:
            columns = row.find_all('td')
            k = '';
            v = '';
            for element in columns:
                if element == columns[0]:
                    k = element.text.rstrip(':').strip()
                else:
                    if element.find_all('table'):
                        offencesDict = []
                        offencesTable = element.find_all('tr')
                        for irow in offencesTable:
                            if irow != offencesTable[0]:
                                icolumns = irow.find_all('td')
                                headerRow = [header.text.rstrip(':').strip() for header in
                                             offencesTable[0].find_all('td')]
                                offencesDict.append(
                                    {headerRow[idx]: val.text.strip() for idx, val in enumerate(icolumns)})
                        v = offencesDict
                        dataDict[k] = v
                        pass
                    elif element.find_all('a'):
                        links = [l['href'] for l in element.find_all('a')]
                        v = links
                    else:
                        v = element.text.strip()
            if k not in dataDict.keys():
                dataDict[k] = v
        validKeys = {k: dataDict[k] for k in dataDict.keys() if str(k[0]).upper() in ascii_uppercase}
        return validKeys

    def convert_dict_to_dataframe(self, dataDict):
        return pd.DataFrame.from_dict(dataDict, orient='index')

    def convert_dict_to_dataframe_style(self, dataDict):
        return {k: np.array(v) for k, v in dataDict.items()}


if __name__ == '__main__':
    s = Scraper()
    # licenseCharSeq = list(islice(Scraper.multiletters(ascii_uppercase), 26 * 27))
    # licenseCharSeq = licenseCharSeq[26:]
    # plates = list(islice(Scraper.license_plate_generator(licenseCharSeq), 10))
    sampleLicensePlate = 'mh12jb2300'
    # for plate in plates:
    #     print('Plate: {}, Data: {}'.format(plate, s.get_challans_for_plate(plate)))

    # Uncomment this out when you're ready for show time
    # challanList = s.get_challans_for_plate(sampleLicensePlate)
    # challanNumbers = [challan for challan in challanList]
    # challanInfo = s.get_challan_info

    challanInfo = dict(mockData)
    challanInfoDict = s.convert_dict_to_dataframe_style(challanInfo)
    challanInfoDf = s.convert_dict_to_dataframe(challanInfoDict).transpose()
    challanInfoDf = challanInfoDf.sort_index(axis=1)

    awesome = pd.DataFrame()

    mess = pd.DataFrame.from_records(challanInfoDf['Offences']).transpose()
    d = pd.DataFrame()
    for index, row in mess.iterrows():
        inter = pd.DataFrame()
        inter = inter.merge(challanInfoDf)
        dfRow = pd.DataFrame.from_dict(dict(row), orient='index')
        inter = inter.join(dfRow)
        d = d.append(inter)

    # writer = pd.ExcelWriter('Test.xlsx')
    # challanInfoDf.to_excel(writer, header=True, index=False, columns=excelColumnHeaderOrder)
    # writer.save()
    print(d)
