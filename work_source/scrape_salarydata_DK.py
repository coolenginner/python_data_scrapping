import requests
import os
import csv
import json
import argparse
from save_scrape_data import *
from datetime import datetime
import re

def scrape_slate_data(gameType):
    
    API_url = ''
    if gameType == 'PGA':
        API_url = 'https://www.draftkings.com/lobby/getcontests?sport=GOLF'
    else:
        API_url = 'https://www.draftkings.com/lobby/getcontests?sport={}'\
            .format(gameType)

    API_response = requests.get(API_url)
    json_API_data = json.loads(API_response.text)
  
    slates = list()
    if gameType == 'PGA':
      for draftGroup in json_API_data['DraftGroups']:
          if draftGroup['AllowUGC'] == True:
              if gameType == 'PGA':
                  slates.append(draftGroup)
    gameType_id = 0
    if gameType == 'NFL':
        for NFL_gameType in json_API_data['GameTypes']:
            if NFL_gameType['Name'] == 'Classic':
                gameType_id = NFL_gameType['GameTypeId']
        for draftGroup in json_API_data['DraftGroups']:
            if draftGroup['GameTypeId'] == gameType_id:
                slates.append(draftGroup)
    if gameType == 'NBA':
        for NFL_gameType in json_API_data['GameTypes']:
            if NFL_gameType['Name'] == 'Classic':
                gameType_id = NFL_gameType['GameTypeId']
        for draftGroup in json_API_data['DraftGroups']:
            regex = re.compile(datetime.today().strftime('%Y-%m-%d'))     
            if(regex.search(draftGroup['StartDateEst']) != None):
                if draftGroup['GameTypeId'] == gameType_id:
                   slates.append(draftGroup)
    return [slate for slate in slates]


def read_csv_from_response(response):
    """Receives a CSV response and returns a list of dictionaries.
    """
    filename = 'results.csv'
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    with open(filename, 'w') as file:
        file.write(response.text)
    csv_list = list()
    # ['Name', 'Salary', 'Position', 'TeamAbbrev',  'ID']
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row[2] != 'Name':
                new_row = list()
                new_row.append(row[2])
                new_row.append(row[5])
                new_row.append(row[0])
                new_row.append(row[6])
                new_row.append(row[1])
                csv_list.append(new_row)
    os.remove(filename)
    return csv_list
        

def download_CSV_from_slate(slates, sport):
    """Goes to the slate web page and downloads the CSV file needed.
    """
    slates_data = list()
    for slate in slates:
        slate_response = requests.get('https://www.draftkings.com/lineup/'\
            'getavailableplayerscsv?draftGroupId={}'.format(slate['DraftGroupId']))
        slate_csv = read_csv_from_response(slate_response)
        title = ''
        if slate['ContestStartTimeSuffix'] is not None: 
            title = slate['ContestStartTimeSuffix'].strip()

        slates_data.append({
            'draftGroupId':slate['DraftGroupId'],
            'sportType':sport,
            'title':title,
            'gameCount': slate['GameCount'],
            'time':slate['StartDateEst'],
            'data':slate_csv
        })
    return slates_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
        'Module that extracts slates list and projection data.')
    parser.add_argument('-s', '--sport', metavar='S', type=str,
        help='Determines the sport to extract data from.')
    args = vars(parser.parse_args())

    slates = scrape_slate_data(args['sport'])

    slates_csv_data = download_CSV_from_slate(slates, args['sport'])

    insert_slates_into_db(slates_csv_data, args['sport'])
