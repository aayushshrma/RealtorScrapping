
import time
import requests
import pandas as pd
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


def extract_info():
    pass


def main():

    sheets_link = 'https://docs.google.com/spreadsheets/d/1bbh3qYXWwy9qM9I_ZkV2WWZ' \
                  'gEGy1xStWpCCptu8bZZo/edit#gid=659590753'


if __name__ == '__main__':
    main()

