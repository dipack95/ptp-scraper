import pandas as pd

from com.dipack.scraper.Scraper import Scraper, Config


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


def write_all_to_excel():
    s = Scraper()

    challans = Scraper.read_from_cache(Config.found_challan_cache)
    fChallans = [Scraper.get_challan_info(c) for c in challans if c]

    edf = pd.DataFrame(columns=excelColumnHeaderOrder)
    for f in fChallans:
        cdf = s.convert_to_df(f)
        edf = edf.append(cdf)

    writer = pd.ExcelWriter(Config.excel_location)
    edf.to_excel(writer, header=True, index=False, columns=excelColumnHeaderOrder)
    writer.save()


def update_excel():
    s = Scraper()

    challans = Scraper.read_from_cache(Config.found_challan_cache)
    fChallans = [Scraper.get_challan_info(c) for c in challans if c]

    edf = pd.read_excel(Config.excel_location, header=0).drop_duplicates()
    for f in fChallans:
        cdf = s.convert_to_df(f)
        edf = edf.append(cdf)

    writer = pd.ExcelWriter(Config.excel_location)
    edf.to_excel(writer, header=True, index=False, columns=excelColumnHeaderOrder)
    writer.save()
