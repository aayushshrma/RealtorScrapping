
import time
import requests
import pandas as pd
from selectorlib import Extractor
from datetime import datetime, timedelta


class ReadSheets:

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


def main():

    sheets_link = 'https://docs.google.com/spreadsheets/d/1bbh3qYXWwy9qM9I_ZkV2WWZ' \
                  'gEGy1xStWpCCptu8bZZo/edit#gid=659590753'


if __name__ == '__main__':
    main()

