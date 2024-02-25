
#Load data into sqlite database
import numpy as np
import pandas as pd
import sqlite3

#define a few functions 

# Functions to use to add taxonomy codes to nppes

def add_taxonomy(dataframe):
    result_values = {}
    for i in range(1, 16):
        codes_columns = f'Healthcare Provider Taxonomy Code_{i}'
        switch_columns = f'Healthcare Provider Primary Taxonomy Switch_{i}'
        
        # Check the condition and append values to the result list
        keys = dataframe['NPI'][dataframe[switch_columns]=='Y'].tolist()
        values = dataframe[codes_columns][dataframe[switch_columns]=='Y']
        for key, value in zip(keys,values):
            result_values[key] = value
            
    taxonomy_df = pd.DataFrame(list(result_values.items()), columns=['NPI', 'Taxonomy_Code'])
    nppes_merged = dataframe.merge(taxonomy_df, on = 'NPI', how = 'left')
    return nppes_merged
                


# function to clean up zipcodes

def Convert_strings_to_nan(value):
    try:
        return float(value)
    except ValueError:
        return np.nan

def fix_zipcode(series):
    # fill na with zeroes
    #change to int so the zeroes at the end gets removed
    # then change to strings and use zfill to add leading zeroes
    series = series.fillna(0).astype(float).astype(int).astype(str).str.zfill(5)
    # now i can split the strings at 5th item
    return series.str[:5]


#load files
cbsa = pd.read_csv('data/ZIP_CBSA.csv')
taxonomy_code_classification =  pd.read_csv('data/nucc_taxonomy_240.csv')

#fix zipcodes in cbsa data
cbsa['zipcodes'] = fix_zipcode(cbsa['ZIP'])





#read the npi data in chunk, filter them by some conditions and then write to sql.
db = sqlite3.connect('data/npi.sqlite')
# for chunk in pd.read_csv('data/DocGraph_Hop_Teaming/DocGraph_Hop_Teaming_2018.csv', 
#                               chunksize = 10000):
#     chunk = chunk[chunk['transaction_count']>50]
#     chunk = chunk[chunk['average_day_wait']<50]
#     chunk.to_sql('npi', 
#                 db, 
#                 if_exists = 'append', 
#                 index = False)  



# load the nppes data to sqlite database

# db = sqlite3.connect('data/npi.sqlite')
for chunk in pd.read_csv('data/NPPES_Data_Dissemination_February_2024/npidata_pfile_20050523-20240211.csv', 
                              chunksize = 10000):
    chunk_taxonomy = add_taxonomy(chunk)
    chunk_merged = pd.merge(left = chunk_taxonomy, 
                            right = taxonomy_code_classification[['Code', 'Classification']].set_index('Code'), 
                            how = 'left',
                            left_on = 'Taxonomy_Code',
                            right_index = True)
    chunk_merged['Provider Business Practice Location Address Postal Code'] = chunk_merged['Provider Business Practice Location Address Postal Code'].apply(Convert_strings_to_nan)
    chunk_merged['zipcodes']=fix_zipcode(
        chunk_merged['Provider Business Practice Location Address Postal Code']
        )
    chunk_merged_cbsa = pd.merge(left = chunk_merged, 
         right = cbsa[['zipcodes','CBSA']].set_index('zipcodes'), 
         how = 'left',
         left_on = 'zipcodes', 
         right_index = True)
    
    chunk_merged_cbsa['CBSA']= chunk_merged_cbsa['CBSA'].fillna(0).astype(int)
    
    chunk_merged_cbsa.to_sql('nppes', 
                db, 
                if_exists = 'append', 
                index = False)  
    
db.close()