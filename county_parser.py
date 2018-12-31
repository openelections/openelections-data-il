import requests
import csv
import re

years = [2008, 2010, 2012, 2014, 2016]

OFFICES = {
    'PRESIDENT AND VICE PRESIDENT': 'President',
    'UNITED STATES SENATOR': 'U.S. Senate',
    'GOVERNOR AND LIEUTENANT GOVERNOR': 'Governor'
}


for year in years:
    results = []
    outfile = f"{year}__primary__county.csv"
    url = f"https://www.elections.il.gov/Downloads/ElectionInformation/VoteTotals/GP{year}Cty.txt"
    r = requests.get(url)
    if year == 2018:
        csv_reader = csv.DictReader(r.text.split('\r\n'))
    else:
        csv_reader = csv.DictReader(r.text.split('\r\n'), delimiter='\t')
    for row in csv_reader:
        district = None
        if 'CONGRESS' in row['OfficeName']:
            office = 'U.S. House'
            district = re.findall(r'\d+', row['OfficeName'])[0]
        elif 'SENATE' in row['OfficeName']:
            office = 'State Senate'
            district = re.findall(r'\d+', row['OfficeName'])[0]
        elif 'REPRESENTATIVE' in row['OfficeName']:
            office = 'State House'
            district = re.findall(r'\d+', row['OfficeName'])[0]
        elif 'PRESIDENT' in row['OfficeName']:
            office = 'President'
        elif 'SENATOR' in row['OfficeName']:
            office = 'U.S. Senate'
        elif 'GOVERNOR' in row['OfficeName']:
            office = 'Governor'
        else:
            office = row['OfficeName']
        results.append([row['County'], office, district, row['PartyAbbrev'], row['CanFirstName']+" "+row['CanLastName'], row['Votes']])
    with open(outfile, 'wt') as csvfile:
        w = csv.writer(csvfile)
        headers = ['county', 'office', 'district', 'party', 'candidate', 'votes']
        w.writerow(headers)
        w.writerows(results)
