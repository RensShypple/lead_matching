import sys

import json
import numpy as np
import pandas as pd
from rapidfuzz import process, utils, fuzz
import math

# Gets string as input and returns string without site prefix
def removehttp(site):
    prefix = ['http://www.', 'https://www.', 'www.', 'https://', 'http://']
    for pfx in prefix:
        if site.startswith(pfx):
            return site[len(pfx):]
    return site


# Reads in input csv, preprocesses data and matches input with copper.csv data (extraction from cssv)
def match_company_name():
    df = pd.read_csv('Input Companies.csv')
    df_copy = df.copy()
    # Remove all uppercase letters, symbols
    for i in range(len(df['Website'])):
        df['Website'].iloc[i] = utils.default_process(removehttp(str(df['Website'].iloc[i])))
        df['Original'].iloc[i] = utils.default_process(str(df['Original'].iloc[i]))

    copper = pd.read_csv('copper.csv')
    copper_copy = copper.copy()
    # Remove all uppercase letters, symbols
    for i in range(len(copper['ORG_COPPER_NAME'])):
        copper['ORG_COPPER_NAME'].iloc[i] = utils.default_process(str(copper['ORG_COPPER_NAME'].iloc[i]))
        copper['ORG_WEBSITE'].iloc[i] = utils.default_process(removehttp(str(copper['ORG_WEBSITE'].iloc[i])))

    dict_list = []
    # Loop through data in input
    for i in range(len(df)):

        website = df['Website'].iloc[i]
        # Use rapidfuzz to match the website
        web_match = process.extractOne(website, copper['ORG_WEBSITE'].values, processor=None, score_cutoff=90)
        if web_match is None or web_match[0] == "nan":
            name = df['Original'].iloc[i]
            if str(name) != "nan":
                # Match company name if no web match was found
                match = process.extractOne(str(name), copper['ORG_COPPER_NAME'].values, processor=None, score_cutoff=95)
            else:
                match = None

            # New dict for storing data
            dict_ = {}
            if match is not None:
                if match[0] != "nan":
                    dict_.update({"Input Name": df_copy['Original'].iloc[i]})
                    dict_.update({"Input Website": df_copy['Website'].iloc[i]})
                    dict_.update({"Match": copper_copy['ORG_COPPER_NAME'].iloc[match[2]]})
                    dict_.update({"Stage": copper_copy['STAGE'].iloc[match[2]]})
                    dict_.update({"Copper link": copper_copy['COPPER_LINK'].iloc[match[2]]})
                    dict_.update({"Score": match[1]})
                    dict_.update({"Source": df_copy['Source'].iloc[i]})
                    dict_.update({"Status": copper_copy['STATUS'].iloc[match[2]]})
            else:
                dict_.update({"Input Name": df_copy['Original'].iloc[i]})
                dict_.update({"Input Website": df_copy['Website'].iloc[i]})
                dict_.update({"Match": "No Match"})
                dict_.update({"Stage": "-"})
                dict_.update({"Copper link": "-"})
                dict_.update({"Score": 0})
                dict_.update({"Source": df_copy['Source'].iloc[i]})
                dict_.update({"Status": "-"})
        else:
            dict_ = {}
            dict_.update({"Input Name": df_copy['Original'].iloc[i]})
            dict_.update({"Input Website": df_copy['Website'].iloc[i]})
            dict_.update({"Match": copper_copy['ORG_WEBSITE'].iloc[web_match[2]]})
            dict_.update({"Stage": copper_copy['STAGE'].iloc[web_match[2]]})
            dict_.update({"Copper link": copper_copy['COPPER_LINK'].iloc[web_match[2]]})
            dict_.update({"Score": web_match[1]})
            dict_.update({"Source": df_copy['Source'].iloc[i]})
            dict_.update({"Status": copper_copy['STATUS'].iloc[web_match[2]]})
        dict_list.append(dict_)

    merge_table = pd.DataFrame(dict_list)

    df_result = merge_table
    df_result.to_csv('output2.csv')


# Gets as input a list of emails and companies, matches emails with copper_people, extracts company_id from
# copper_people match and finally extracts stage from company_id match (from cssv)
def match_email():
    email_input = pd.read_csv('Company Source Match - Input Email.csv')
    email_match = pd.read_csv('Email match.csv')

    for i in range(len(email_match)):
        email_match['EMAILS'].iloc[i] = str(email_match['EMAILS'].iloc[i])

    dict_list = []
    # Loop through email input
    for i in range(len(email_input)):
        dict_ = {}
        email = email_input['Email'].iloc[i]
        # Find email match
        match = process.extractOne(str(email), email_match['EMAILS'].values, processor=None, score_cutoff=90)
        if match is not None:
            if match[0] != "nan":
                dict_.update({"Email": email_input['Email'].iloc[i]})
                dict_.update({"Match": email_match['EMAILS'].iloc[match[2]]})
                dict_.update({"Company ID - match": match_company_id(email_match['COMPANY_ID'].iloc[match[2]])})
                dict_.update({"Company name - input": email_input['Company'].iloc[i]})
        else:
            dict_.update({"Email": email_input['Email'].iloc[i]})
            dict_.update({"Match": "No Match"})
            dict_.update({"Company ID - match": "-"})
            dict_.update({"Company name - input": email_input['Company'].iloc[i]})
        dict_list.append(dict_)

    merge_table = pd.DataFrame(dict_list)

    df_result = merge_table
    df_result.to_csv('output_email.csv')


# Input is a company_id, output is a stage corresponding to that company_id in cssv
def match_company_id(company_id):
    copper = pd.read_csv('copper.csv')

    stage = "-"
    if not math.isnan(company_id):
        company_id = math.floor(company_id)
        res = copper.loc[copper['ORG_COPPER_ID'] == company_id]
        if not res.empty:
            stage = res['STAGE'].values[0]
    return stage


if __name__ == "__main__":
    match_company_name()
