import os
import errno
import requests
import logging
import time
import validators
from bs4 import BeautifulSoup as bs
from itertools import count, product, islice
from string import ascii_uppercase
from robobrowser import RoboBrowser
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class Config:
    output_dir = '../../../../output/'
    image_save_directory = output_dir + 'evidence_images/'
    excel_location = output_dir + 'Output.xlsx'
    license_plate_cache = output_dir + 'checked_licenses.txt'
    found_challan_cache = output_dir + 'found_challans.txt'

    # In seconds
    sleep_time = 20
    wait_time_for_requests = 2


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
    PTPField.impounded_document
]


def make_dir(dir):
    try:
        os.makedirs(dir, exist_ok=True)
    except OSError as ex:
        if ex.errno == errno.EEXIST and os.path.isdir(dir):
            pass
        else:
            raise


class Scraper:
    def __init__(self):
        make_dir(Config.output_dir)
        make_dir(Config.image_save_directory)
        logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p',
                            level=logging.INFO)

    @staticmethod
    def write_to_cache(toWrite, items):
        with open(toWrite, 'a+') as licensePlateCache:
            for item in items:
                licensePlateCache.write('{}, '.format(item.__str__()))

    @staticmethod
    def read_from_cache(toRead):
        if os.path.exists(toRead):
            checkedLicenses = open(toRead, 'r').read().split(',')
            return [cl.strip() for cl in checkedLicenses if cl != '']
        else:
            return []

    @staticmethod
    def clean_cache(toClean):
        cleaned = np.unique(Scraper.read_from_cache(toClean))
        with open(toClean, 'w') as licensePlateCache:
            for checkedPlate in cleaned:
                licensePlateCache.write('{}, '.format(checkedPlate.__str__()))

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

    @staticmethod
    def rto_license_plate_generator(licenseCharList, rto=12):
        for charSeq in licenseCharList:
            for plateNumber in range(1000, 9999):
                yield ''.join(['MH', (rto).__str__().zfill(2), charSeq, plateNumber.__str__()])

    @staticmethod
    def get_challans_for_plate(licensePlate):
        logger.debug('Get Challans for Plate: {}'.format(licensePlate.__str__()))
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

    @staticmethod
    def get_challan_info(challanNumber):
        logger.debug('Getting detailed info for challan: {}'.format(challanNumber))
        dataDict = {}
        getChallanInfoUrl = 'http://punetrafficop.net/y/x?x='
        getInfoForChallanUrl = getChallanInfoUrl + challanNumber
        response = requests.get(getInfoForChallanUrl)
        dataTable = bs(response.content, 'lxml').find_all('tr')
        for row in dataTable:
            columns = row.find_all('td')
            k = ''
            v = ''
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

    @staticmethod
    def format_challan_info(challanInfo):
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

    @staticmethod
    def download_images(links, challanNumber, forceDownload=False):
        logger.info("Downloading images for challan {}".format(challanNumber))
        if isinstance(links, list):
            for img in links:
                if validators.url(img):
                    imgFileName = Config.image_save_directory + challanNumber + '_' + img.__str__().split('/')[-1]
                    if not os.path.exists(imgFileName) or forceDownload:
                        with open(imgFileName, 'wb') as imgFile:
                            response = requests.get(img)
                            if not response.ok:
                                logger.error("Failed to download images!")
                                pass
                            else:
                                imgFile.write(response.content)
                    else:
                        logger.info(
                            'Will not re-download the image as it already exists at: {}'.format(
                                os.path.abspath(imgFileName)))
                else:
                    logger.error('Invalid image URL: {}'.format(img))

    def convert_to_df(self, challanInfo):
        challanInfo.pop(PTPField.payment_url)
        challanInfo.pop(PTPField.payment_status)

        formattedChallanInfoList = self.format_challan_info(challanInfo)
        return pd.DataFrame.from_records(formattedChallanInfoList)


if __name__ == '__main__':
    sampleLicensePlate = 'mh12jb2300'
    df = pd.DataFrame(columns=excelColumnHeaderOrder)

    s = Scraper()
    Scraper.clean_cache(Config.license_plate_cache)
    Scraper.clean_cache(Config.found_challan_cache)
    checked_licenses = Scraper.read_from_cache(Config.license_plate_cache)

    licenseCharSeq = list(islice(Scraper.multi_letters(ascii_uppercase), 26 * 27))
    licenseCharSeq = licenseCharSeq[26:]

    # plates = list(islice(Scraper.rto_license_plate_generator(licenseCharSeq), 20))
    plates = list(Scraper.rto_license_plate_generator(licenseCharSeq))
    logger.info('Filtering out already checked licenses from cache: {}'.format(checked_licenses))
    plates = [x for x in plates if x not in checked_licenses]
    # plates = [sampleLicensePlate]

    checkedList = []
    foundChallans = []
    for index, plate in enumerate(plates):
        logger.info('Fetching challans for plate: {}'.format(plate.__str__()))
        challanList = Scraper.get_challans_for_plate(plate)
        if challanList:
            challanNumbers = [challan for challan in challanList]
            for challanNumber in challanNumbers:
                challanInfo = Scraper.get_challan_info(challanNumber)
                # challanInfo = dict(mockData)
                if challanInfo:
                    foundChallans = np.append(foundChallans, challanInfo[PTPField.challan_no])
                    Scraper.download_images(challanInfo[PTPField.evidences], challanInfo[PTPField.challan_no])
                    challanDf = s.convert_to_df(challanInfo)
                    df = df.append(challanDf)
                else:
                    logger.error('No information retrieved for challan: {}'.format(challanNumber))
        else:
            logger.warning('No challans found for plate: {}'.format(plate.__str__()))

        logger.info('Adding plate: {} to cache'.format(plate.__str__()))
        checkedList = np.append(checkedList, plate.__str__())
        Scraper.write_to_cache(Config.license_plate_cache, checkedList)
        Scraper.write_to_cache(Config.found_challan_cache, foundChallans)

        if not index % 10 and index != 0:
            logger.info('Sleeping for {} seconds'.format(Config.sleep_time))
            time.sleep(Config.sleep_time)
            logger.info('Resuming execution')
            logger.info('Cleaning up caches')
            Scraper.clean_cache(Config.license_plate_cache)
            Scraper.clean_cache(Config.found_challan_cache)
        time.sleep(Config.wait_time_for_requests)

    logger.info('Beginning write to excel file: {}'.format(os.path.abspath(Config.excel_location)))
    writer = pd.ExcelWriter(Config.excel_location)
    df.to_excel(writer, header=True, index=False, columns=excelColumnHeaderOrder)
    writer.save()
