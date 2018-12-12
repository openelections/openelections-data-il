import argparse
import os
import re

import numpy as  np
import pandas as pd 


parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str, help="The input directory containing the source CSVs", required=True)
parser.add_argument("-o", type=str, help="The output file path", required=True)

args = parser.parse_args()

# This might not be an exhaustive list for all cycles
PARTY_TRANSLATIONS = {
    '': '',
    'CONSERVATIVE': 'CONSERVATIVE',
    'Conservative': 'CONSERVATIVE', 
    'DEMOCRACT': 'DEMOCRACTIC',
    'Democrat': 'DEMOCRACTIC',
    'DEMOCRATIC': 'DEMOCRACTIC',
    'Democratic': 'DEMOCRACTIC',
    'DOWNSTATE UNITED': 'DOWNSTATE UNITED',
    'Green': 'GREEN',
    'GREEN': 'GREEN',
    'Independent': 'INDEPENDENT',
    'INDEPENDENT': 'INDEPENDENT',
    'LIBERTARIAN': 'LIBERTARIAN',
    'Libertarian': 'LIBERTARIAN',
    'NON-PARTISAN': 'NONPARTISAN',
    'Non-Partisan': 'NONPARTISAN',
    'Nonpartisan': 'NONPARTISAN',
    'NonPartisan': 'NONPARTISAN',
    'NONPARTISAN': 'NONPARTISAN',
    'REPUBLICAN': 'REPUBLICAN',
    'Republican': 'REPUBLICAN',
}

WRITE_IN_TRANSLATIONS = {
    'WRITE-IN': 'WRITE-IN',
    'Write-In': 'WRITE-IN',
    'write-in': 'WRITE-IN',
    'Write-in': 'WRITE-IN',
}

def extract_district_from_contest(contest):
    if contest.endswith(' REPRESENTATIVE') or contest.endswith(' SENATE') or contest.endswith(' CONGRESS'):
        return re.findall(r'\d+', contest)[0]
    else:
        return np.nan


def extract_office_from_contest(contest):
    if contest and contest.endswith(' REPRESENTATIVE'):
        return 'STATE HOUSE'
    elif contest and contest.endswith(' SENATE'):
        return 'STATE SENATE'
    elif contest and contest.endswith(' CONGRESS'):
        return 'U.S. HOUSE'
    else:
        return contest

all_data_df = None

for dirpath, _, filenames in os.walk(args.i):
    for filename in filenames:
        full_path = '{}{}'.format(dirpath, filename)
        df = pd.read_csv(full_path, encoding='utf-8')

        if all_data_df is None:
            all_data_df = df
        else:
            all_data_df = pd.concat([all_data_df, df])

all_data_df.PartyName.fillna('', inplace=True)
all_data_df['PartyName'] = all_data_df['PartyName'].map(lambda a: PARTY_TRANSLATIONS[a])
all_data_df['CandidateName'] = all_data_df['CandidateName'].map(lambda a: WRITE_IN_TRANSLATIONS[a] if a in WRITE_IN_TRANSLATIONS else a)

# Get all the ContestName that are NaN
# And replace that null value with the first not-null ContestName of the same EISContestID
target_ids = all_data_df[all_data_df.ContestName.isnull()].EISContestID.unique()
for target_id in target_ids:
    filler_name = all_data_df[
        (all_data_df.EISContestID == target_id) & 
        (all_data_df.ContestName.notnull())
    ].iloc[0].ContestName

    mask = (all_data_df.EISContestID == target_id)
    #repalce NaNs
    all_data_df.loc[mask, 'ContestName'] = all_data_df.loc[mask, 'ContestName'].fillna(filler_name)


all_data_df['office'] = all_data_df.apply(
    lambda row: extract_office_from_contest(row['ContestName']),
    axis=1,
)

all_data_df['district'] = all_data_df.apply(
    lambda row: extract_district_from_contest(row['ContestName']),
    axis=1,
)

all_data_df.rename(
    columns={
        'JurisName': 'county',
        'CandidateName': 'candidate',
        'PrecinctName': 'precinct',
        'PartyName': 'party',
        'VoteCount': 'votes',
    },
    inplace=True
)

all_data_df = all_data_df[['county', 'precinct', 'office', 'district', 'party', 'candidate', 'votes']]

all_data_df.sort_values(by=['county', 'precinct', 'office', 'district', 'party'], inplace=True)

all_data_df.to_csv(args.o, encoding='utf-8', na_rep='', index=False)
