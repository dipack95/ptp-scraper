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


class PTPField:
    vehicle_no = 'vehicle_no'
    license_no = 'license_no'
    payment_url = 'payment_url'
    compounding_fees = 'compounding_fees'
    offences = 'offences'
    offense_time = 'offense_time'
    offender_mobile_no = 'offender_mobile_no'
    payment_status = 'payment_status'
    offense_date = 'offense_date'
    challan_no = 'challan_no'
    sections = 'sections'
    evidences = 'evidences'
    impounded_document = 'impounded_document'
    inner_sections = 'sections'
    inner_offences = 'offenses'
    inner_fine_amount = 'fine_amount'


fields = [PTPField.vehicle_no,
          PTPField.license_no,
          PTPField.payment_url,
          PTPField.compounding_fees,
          PTPField.offences,
          PTPField.offense_time,
          PTPField.offender_mobile_no,
          PTPField.payment_status,
          PTPField.offense_date,
          PTPField.challan_no,
          PTPField.sections,
          PTPField.evidences,
          PTPField.impounded_document]

mockData = {'challan_no': 'PTPCHC170504000978', 'vehicle_no': 'MH12JB2300',
            'offences': [{'fine_amount': '200', 'offenses': 'Halting ahead white line', 'sections': '19(1)/177 MVA'},
                         {'fine_amount': '500', 'offenses': 'Without Helmet', 'sections': '129/177'}],
            'offense_time': '11:43:51', 'payment_url': ['https://punetrafficop.net/'], 'payment_status': 'Pending',
            'compounding_fees': '700',
            'evidences': ['http://punetrafficop.online:8080/File/PTPCHC170504000978_01.png',
                          'http://punetrafficop.online:8080/File/PTPCHC170504000978_02.png'],
            'impounded_document': 'No Impound', 'offense_date': '2017-05-04', 'offender_mobile_no': 'NA',
            'license_no': 'NA'}

excelColumnHeaderOrder = [
    PTPField.challan_no,
    PTPField.vehicle_no,
    PTPField.license_no,
    PTPField.offense_date,
    PTPField.offense_time,
    PTPField.offender_mobile_no,
    PTPField.inner_offences,
    PTPField.inner_sections,
    PTPField.inner_fine_amount,
    PTPField.compounding_fees,
    PTPField.evidences,
    PTPField.impounded_document,
    PTPField.payment_status,
    PTPField.payment_url
]

groupByHeader = [
    PTPField.challan_no,
    PTPField.vehicle_no,
    PTPField.license_no,
    PTPField.offense_date,
    PTPField.offense_time,
    PTPField.offender_mobile_no,
    PTPField.compounding_fees,
    PTPField.evidences,
    PTPField.impounded_document,
    PTPField.payment_status,
    PTPField.payment_url
]


class Scraper:
    def __init__(self):
        logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p',
                            level=logging.INFO)

    @staticmethod
    def multi_letters(seq):
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
                    k = element.text.rstrip(':').strip().lower().replace(' ', '_')
                else:
                    if element.find_all('table'):
                        offencesDict = []
                        offencesTable = element.find_all('tr')
                        for irow in offencesTable:
                            if irow != offencesTable[0]:
                                icolumns = irow.find_all('td')
                                headerRow = [header.text.rstrip(':').strip().lower().replace(' ', '_') for header in
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

    def format_challan_info(self, challanInfo):
        for k, v in challanInfo.items():
            if isinstance(v, list):
                if not isinstance(v[0], dict):
                    l = challanInfo.pop(k)
                    challanInfo[k] = ', '.join([i for i in l])
                else:
                    pass
        offensesList = challanInfo.pop(PTPField.offences)
        formattedChallanInfoList = []
        for offense in offensesList:
            d = {**offense, **challanInfo.copy()}
            formattedChallanInfoList = np.append(formattedChallanInfoList, d)
        return formattedChallanInfoList


def super_awesome_lambda(x):
    print(x)
    return x


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
    # challanInfo = s.get_challan_info(challanNumbers[0])

    challanInfo = dict(mockData)

    formattedChallanInfoList = s.format_challan_info(challanInfo)
    df = pd.DataFrame.from_records(formattedChallanInfoList)
    writer = pd.ExcelWriter('Test.xlsx')
    df.to_excel(writer, header=True, index=False, columns=excelColumnHeaderOrder)
    writer.save()
