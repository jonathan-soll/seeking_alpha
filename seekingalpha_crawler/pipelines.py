# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pyodbc

class SeekingalphaCrawlerPipeline(object):
    '''
    Processes data from the web scraper.
    For each item yielded by the scraper it writes it to the SQL Server Database
    '''

    def __init__(self):
        '''
        Initializes the class with the variables needed to connect to the database
        '''
        # set up the ODBC connection
        self.server = 'firstserverjs.database.windows.net'
        self.database = 'securities_master'
        self.username = 'JSoll'
        self.password = 'Emjosa139'
        self.driver= '{ODBC Driver 17 for SQL Server}'
        self.cnxn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1433;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)

    def process_item(self, item, spider):
        '''
        Writes the scraped data to the SQL Server database

        Parameters:
        item: a dict {symbol, title} that contains stuff scraped from Seeking SeekingAlpha
        spider: the spider that scraped the item (seekingalpha)
        '''

        print(item)
        if item['symbol'] != None:
            item['title'] = item['title'][:50]

            data = tuple(item.values())
            data = data + data


            insert_str = '''INSERT INTO  seekingalpha_symbols
                            SELECT ?, ?, GETDATE()
                            WHERE NOT EXISTS
                            (
                                SELECT 1
                                FROM seekingalpha_symbols
                                WHERE
                                    symbol = ?
                                    AND title = ?
                            )'''


            with self.cnxn:
                cur = self.cnxn.cursor()
                try:
                    cur.execute(insert_str, data)
                except:
                    print('Error importing to SQL Server')
