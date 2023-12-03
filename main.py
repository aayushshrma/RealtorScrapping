
import re
import time
import requests
import numpy as np
import pandas as pd
from selectorlib import Extractor
from datetime import datetime, timedelta


class HandleSheets:

    def __init__(self, sheet_url):
        self.url = sheet_url

    def read_id(self):
        sheet_id = self.url.split('/')[5]
        return sheet_id

    def read_df(self, sheet_name):
        sheet_csv = f'https://docs.google.com/spreadsheets/d/{self.read_id()}/' \
                    f'gviz/tq?tqx=out:csv&sheet={sheet_name}'
        df = pd.read_csv(sheet_csv)
        return df

    def get_links(self):
        df = self.read_df('Main')
        terms = ['https://www.realtor.com', 'www.realtor.com', 'realtor.com']
        mask = df('Main')['Links'].str.contains('|'.join(terms))
        links_df = df['Links'][mask]
        return links_df


class ExtractInfo:

    def __init__(self):
        pass

    def extractor(self):
        headers = {'pragma': 'no-cache',
                   'cache-control': 'no-cache',
                   'dnt': '1',
                   'upgrade-insecure-requests': '1',
                   'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
                   'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8', }

        extract_details = Extractor.from_yaml_string("""
            houses:
                css: div.BasePropertyCard_propertyCardWrap__Z5y4p
                multiple: true
                type: Text
            """)

        extract_links = Extractor.from_yaml_string("""
            houses:
                css: div.BasePropertyCard_propertyCardWrap__Z5y4p
                multiple: true
                type: Text
                children:
                    url:
                        css: a
                        type: Link
            """)
        return headers, extract_details, extract_links

    def get_data(self, links_df):
        headers, extract_details, extract_links = self.extractor()

        all_details = []
        all_links = []
        for link in links_df:
            n = 1
            while True:
                new_link = link + f'/pg-{n}'
                response = requests.get(new_link, headers=headers)
                details = extract_details.extract(response.text)
                if not isinstance(details['houses'], list) or \
                        (n > 1 and details['houses'][:5] == all_details[-1]['houses'][:5]) or n == 10:
                    break
                all_details.append(details)
                house_links = extract_links.extract(response.text)
                all_links.append(house_links)
                n = n + 1
                time.sleep(3)

        return all_details, all_links


def re_match():

    price_pattern = re.compile(r'\$([\d,]+)')
    bed_pattern = re.compile(r'(\d+)\s*bed')
    bath_pattern = re.compile(r'(\d+(\.\d+)?)\s*bath')
    sqft_pattern = re.compile(r'([\d,]+)\s*sqft')
    lot_pattern = re.compile(r'([\d,]+)\s*sqft\s*lot')
    address_pattern = re.compile(r'(\d+)\s*([A-Za-z\s]+),\s*([A-Za-z\s]+)\s*(\d+)')

    return price_pattern, bed_pattern, bath_pattern, sqft_pattern, lot_pattern, address_pattern


def handle_df(price_pattern, bed_pattern, bath_pattern,
              sqft_pattern, lot_pattern, address_pattern, all_details, all_links,
              df, df_list1, df_list2, df_list3):

    for i in range(len(all_details)):
        for j, input_string in enumerate(all_details[i]['houses']):

            price_match = price_pattern.search(input_string)
            bed_match = bed_pattern.search(input_string)
            bath_match = bath_pattern.search(input_string)
            sqft_match = sqft_pattern.search(input_string)
            lot_match = lot_pattern.search(input_string)
            address_match = address_pattern.search(input_string)

            price = price_match.group(1) if price_match else None
            bed = bed_match.group(1) if bed_match else None
            bath = bath_match.group(1) if bath_match else None
            sqft = sqft_match.group(1) if sqft_match else None
            lot = lot_match.group(1) if lot_match else None
            address = f"{address_match.group(1)} {address_match.group(2)}, {address_match.group(3)} {address_match.group(4)}" if address_match else None
            house_url = 'www.realtor.com' + all_links[i]['houses'][j]['url']
            try:
                price_sqft = round(float(price.replace(",", "")) / float(sqft.replace(",", "")), 2)
            except:
                price_sqft = 'N/A'
            formatted_date_time = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
            date_part, time_part = formatted_date_time.split()

            tab = ''
            for k in df.iloc[:, 0]:
                if k in address:
                    selected_df = df.loc[df.iloc[:, 0] == k]
                    tab = selected_df.iloc[:, 1].values[0]
            if tab == 'List1':
                if address in df_list1['Address']:
                    prev_df = df[df_list1['Address'] == address]
                    date_created = prev_df['Date Created']
                    time_created = prev_df['Time Created']
                    new_df = pd.DataFrame(
                        {'Address': [address], 'Link': [house_url], 'Price': [f'${price}'], 'Bed': [bed],
                         'Bath': [bath], 'Sq Ft': [f'{sqft} sqft'], 'Lot': [f'{lot} sqft'],
                         'Price/Sq ft': [f'${price_sqft}'], 'Date Created': [date_created],
                         'Time Created': [time_created], 'Date Updated': [date_part], 'Time Updated': [time_part]})
                    df_list1 = pd.concat([df_list1, new_df], ignore_index=True)
                    df_list1 = df_list1.drop_duplicates(subset=['Address'], keep='last')
                else:
                    new_df = pd.DataFrame(
                        {'Address': [address], 'Link': [house_url], 'Price': [f'${price}'], 'Bed': [bed],
                         'Bath': [bath], 'Sq Ft': [f'{sqft} sqft'], 'Lot': [f'{lot} sqft'],
                         'Price/Sq ft': [f'${price_sqft}'], 'Date Created': [date_part], 'Time Created': [time_part],
                         'Date Updated': [np.nan], 'Time Updated': [np.nan]})
                    df_list1 = pd.concat([df_list1, new_df], ignore_index=True)
            elif tab == 'List2':
                if address in df_list2['Address']:
                    prev_df = df[df_list2['Address'] == address]
                    date_created = prev_df['Date Created']
                    time_created = prev_df['Time Created']
                    new_df = pd.DataFrame(
                        {'Address': [address], 'Link': [house_url], 'Price': [f'${price}'], 'Bed': [bed],
                         'Bath': [bath], 'Sq Ft': [f'{sqft} sqft'], 'Lot': [f'{lot} sqft'],
                         'Price/Sq ft': [f'${price_sqft}'], 'Date Created': [date_created],
                         'Time Created': [time_created], 'Date Updated': [date_part], 'Time Updated': [time_part]})
                    df_list2 = pd.concat([df_list2, new_df], ignore_index=True)
                    df_list2 = df_list2.drop_duplicates(subset=['Address'], keep='last')
                else:
                    new_df = pd.DataFrame(
                        {'Address': [address], 'Link': [house_url], 'Price': [f'${price}'], 'Bed': [bed],
                         'Bath': [bath], 'Sq Ft': [f'{sqft} sqft'], 'Lot': [f'{lot} sqft'],
                         'Price/Sq ft': [f'${price_sqft}'], 'Date Created': [date_part], 'Time Created': [time_part],
                         'Date Updated': [np.nan], 'Time Updated': [np.nan]})
                    df_list2 = pd.concat([df_list2, new_df], ignore_index=True)
            else:
                if address in df_list3['Address']:
                    prev_df = df[df_list3['Address'] == address]
                    date_created = prev_df['Date Created']
                    time_created = prev_df['Time Created']
                    new_df = pd.DataFrame(
                        {'Address': [address], 'Link': [house_url], 'Price': [f'${price}'], 'Bed': [bed],
                         'Bath': [bath], 'Sq Ft': [f'{sqft} sqft'], 'Lot': [f'{lot} sqft'],
                         'Price/Sq ft': [f'${price_sqft}'], 'Date Created': [date_created],
                         'Time Created': [time_created], 'Date Updated': [date_part], 'Time Updated': [time_part]})
                    df_list3 = pd.concat([df_list3, new_df], ignore_index=True)
                    df_list3 = df_list3.drop_duplicates(subset=['Address'], keep='last')
                else:
                    new_df = pd.DataFrame(
                        {'Address': [address], 'Link': [house_url], 'Price': [f'${price}'], 'Bed': [bed],
                         'Bath': [bath], 'Sq Ft': [f'{sqft} sqft'], 'Lot': [f'{lot} sqft'],
                         'Price/Sq ft': [f'${price_sqft}'], 'Date Created': [date_part], 'Time Created': [time_part],
                         'Date Updated': [np.nan], 'Time Updated': [np.nan]})
                    df_list3 = pd.concat([df_list3, new_df], ignore_index=True)

    return df_list1, df_list2, df_list3


def main():

    sheets_link = 'https://docs.google.com/spreadsheets/d/1Gy4krTMIvShtRSpybOk2z8uE79DRWgSrEJCbR5Uh_Ug/' \
                  'edit#gid=1893615227'


if __name__ == '__main__':
    main()

