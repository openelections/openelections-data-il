import tabula
import pandas as pd
from decorator import decorator
import re

def scrape_tables(pdf, areas, county, pages=1, headers=None, labels=None):
    
    def get_area(left,top,width,height):
        y1 = top
        x1 = left
        y2 = top + height
        x2 = left + width
        return (y1,x1,y2,x2)
    
    HEADER_AREA = get_area(**areas['header'])
    LABEL_AREA = get_area(**areas['label'])
    CONTENT_AREA = get_area(**areas['content'])
    
    def get_header(pdf, pages):
        if pages in headers:
            df = pd.DataFrame(columns=range(len(headers[pages])))
            df.loc[0] = headers[pages]
        else:
            df = tabula.read_pdf(pdf, area=HEADER_AREA, pages=pages, guess=False, pandas_options={'header': None})
        header = list(df.fillna('').agg(lambda col: ' '.join(col)).str.strip().values)
        header = [val for val in header if '%' not in val]
        return [unicode('Precinct')] + header

    def get_label(pdf, pages):
        if pages in labels:
            return labels[pages]
        else:
            df = tabula.read_pdf(pdf, area=LABEL_AREA, pages=pages, guess=False, latice=True, pandas_options={'header': None})
            return df.loc[0].str.cat(sep=' ').strip()
    
    @decorator
    def drop_nan(f, *args, **kw):
        df = f(*args, **kw)

        for col in df:
            if pd.isnull(df[col]).all():
                df = df.drop(col, axis=1)
        return df

    @decorator
    def drop_percent(f, *args, **kw):
        grab_numeric = lambda series: series.str.split(expand=True)[0]
        df = f(*args, **kw)

        for col in df:
            try:
                if df[col].str.endswith('%').any():
                    # Sometimes the count and percent don't get split, so we grab the numeric col
                    if df[col].str.contains(' ').any():
                        df[col] = grab_numeric(df[col])
                    else:
                        df = df.drop(col, axis=1)
            except AttributeError: # except isn't string
                pass
        return df

    @decorator
    def add_header(f, *args, **kw):
        header = get_header(*args, **kw)
        df = f(*args, **kw)

        i = 0
        while len(header) < len(df.columns):
            header += ['Unnamed: %s' % i]
            i += 1
        
        display(header)
        df.columns = header
        return df
    
    @decorator
    def add_county(f, *args, **kw):
        df = f(*args, **kw)
        df['County'] = county
        return df
    
    @decorator
    def drop_jurisdiction_header(f, *args, **kw):
        df = f(*args, **kw)
        df = df[df.Precinct != 'Jurisdiction Wide']
        return df

    @drop_jurisdiction_header
    @add_county
    @add_header
    @drop_percent
    @drop_nan
    def get_content(pdf, pages):
        df = tabula.read_pdf(pdf, pages=pages, pandas_options={'header': None})
        return df
    
    results = {}

    for n in pages:
        df = get_content(pdf, n)
        display(df)
        label = get_label(pdf, n)
        if label not in results:
            results[label] = df
#         elif results[label].columns.all() == df.columns.all():
#             results[label] = pd.concat([results[label], df])
        else:
            results[label] = pd.concat([results[label], df], join="outer")
        print("Added result from page %s. Race: %s" % (n, label))
    
    return results

def clean_results(results):
    
    def lookup_office(label):
        patterns = {
            'D[0-9]+ REPRESENTATIVE': 'General Assembly',
            'REPRES [0-9]+[A-Z]{2} DIST': 'General Assembly',
            'D[0-9]+ CONGRESS': 'U.S. House',
            'REP IN CONG [0-9]+[A-Z]{2} DIST': 'U.S. House',
            '[0-9]+[A-Z]{2} CONGRESSIONAL DIST': 'U.S. House',
            'D[0-9]+ STATE SENATE': 'State Senate',
            'SENATE DIST [0-9]+': 'State Senate',
            'TURN OUT': 'Voters',
            'PRESIDENT': 'President'
        }

        for p in patterns:
            if re.match(p, label):
                return patterns[p]

        return label.title()

    def lookup_district(label):
        try:
            return re.search('[0-9]+', label).group(0)
        except AttributeError:
            return '-'

    def lookup_candidate(cand):
        return re.search('[A-Za-z \.-]+', cand).group(0).strip()

    def lookup_party(cand):
        try:
            return re.search('(?<=\()([A-Z]+)', cand).group(0)
        except AttributeError:
            return '-'

    def process_df(label, df):
        template = pd.DataFrame(columns=['county', 'precinct', 'office', 'district', 'candidate', 'party', 'votes'])
        template.precinct = df.Precinct.str.title()
        df = df.drop('Precinct', axis=1)
        template.county = df.County
        df = df.drop('County', axis=1)
        template.office = lookup_office(label)
        template.district = lookup_district(label)

        candidates = [val for val in df.columns.values if val not in ['Reg. Voters', 'Times Counted', 'Total Votes', 'Vote For', 'Times Over Voted', 'Number Of Under Votes']]

        if lookup_office(label) == 'Voters':
            candidates = ['Reg. Voters', 'Cards Cast']

        def process_candidate(c):
            c_df = template.copy()
            c_df.votes = df[c]
            c_df.candidate = lookup_candidate(c)
            c_df.party = lookup_party(c)
            c_df = c_df[~c_df.precinct.isin(['Presidential Ballot', 'Total', 'Elections Office'])]
            c_df = c_df[c_df.votes >= 0]
            return c_df

        frames = [process_candidate(c) for c in candidates]
        return pd.concat(frames)
    
    final_frames = [process_df(label, df) for label,df in results.items()]
    final = pd.concat(final_frames)
    return final

def main(config, output):
    results = scrape_tables(**config)
    final = clean_results(results)
    final.to_csv(output, index=False)
    print "Wrote cleaned results to %s" % output
    
# SAMPLE CONFIG TO LOAD INTO SCRAPER:
#
# CONFIG = {
#     'areas': {
#         'label': {
#             'left': 37.56,
#             'top': 136,
#             'width': 527.65,
#             'height': 30
#         },
#         'header': {
#             'left': 37.56,
#             'top': 157,
#             'width': 527.65,
#             'height': 50
#         },
#         'content': {
#             'left': 34.89,
#             'top': 137.18,
#             'width': 527.65,
#             'height': 640.11
#         }
#     },
#     "pdf": "../raw/2012+nov+6+il+edgar.pdf",
#     "pages": [1,3,4,5,6,7,8,9,10],
#     "county": "Edgar",
#     'headers': {
#         1: ['Reg. Voters', 'Cards Cast', '% Turnout'],
#     },
#     'labels': {
#         1: 'TURN OUT',
#     }
# }