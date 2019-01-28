import argparse
import os
import re

import numpy as  np
import pandas as pd 


# This might not be an exhaustive list for all cycles
PARTY_TRANSLATIONS = {
    '': '',
    'CONSERVATIVE': 'CONSERVATIVE',
    'Conservative': 'CONSERVATIVE', 
    'DEMOCRACT': 'DEM',
    'Democrat': 'DEM',
    'DEMOCRATIC': 'DEM',
    'Democratic': 'DEM',
    'DOWNSTATE UNITED': 'DOWNSTATE UNITED',
    'Downstate United': 'DOWNSTATE UNITED',
    'Green': 'GRN',
    'GREEN': 'GRN',
    'Independent': 'IND',
    'INDEPENDENT': 'IND',
    'LIBERTARIAN': 'LIB',
    'Libertarian': 'LIB',
    'NON-PARTISAN': 'NONPARTISAN',
    'Non-Partisan': 'NONPARTISAN',
    'Nonpartisan': 'NONPARTISAN',
    'NonPartisan': 'NONPARTISAN',
    'NONPARTISAN': 'NONPARTISAN',
    'REPUBLICAN': 'REP',
    'Republican': 'REP',
}

OFFICE_TRANSLATIONS = {
    'GOVERNOR AND LIEUTENANT GOVERNOR': 'Governor',
    'ATTORNEY GENERAL': 'Attorney General',
    'SECRETARY OF STATE': 'Secretary of State',
    'STATE HOUSE': 'State House',
    'STATE SENATE': 'State Senate',
    'TREASURER': 'Treasurer',
    'U.S. HOUSE': 'U.S. House',
    'U.S. SENATE': 'U.S. Senate',
    'COMPTROLLER': 'Comptroller'
 }

WRITE_IN_TRANSLATIONS = {
    'WRITE-IN': 'Write-ins',
    'Write-In': 'Write-ins',
    'write-in': 'Write-ins',
    'Write-in': 'Write-ins',
}

def extract_district_from_contest(contest):
    if contest.endswith(' REPRESENTATIVE') or contest.endswith(' SENATE') or contest.endswith(' CONGRESS'):
        return re.findall(r'\d+', contest)[0]
    else:
        return np.nan


def extract_office_from_contest(contest):
    if contest and contest.endswith(' REPRESENTATIVE'):
        return 'State House'
    elif contest and contest.endswith(' SENATE'):
        return 'State Senate'
    elif contest and contest.endswith(' CONGRESS'):
        return 'U.S. House'
    else:
        return OFFICE_TRANSLATIONS.get(contest, contest)

def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", type=str, help="The input directory containing the source CSVs", required=True)
    parser.add_argument("-o", type=str, help="The output file path", required=True)

    return parser.parse_args()


def main():
    args = parseArguments()

    all_data_df = None

    for dirpath, _, filenames in os.walk(args.i):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
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

    all_data_df['county'] = all_data_df['county'].str.title()

    all_data_df.sort_values(by=['county', 'precinct', 'office', 'district', 'party'], inplace=True)

    all_data_df.to_csv(args.o, encoding='utf-8', na_rep='', index=False)


# Default function is main()
if __name__ == '__main__':
    main()