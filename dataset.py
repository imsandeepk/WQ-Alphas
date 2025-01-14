import sys
import os
import pickle
import time

# Append paths for the libraries
sys.path.append('alpha_creation_engine-main')
import ace_lib as ace
import helpful_functions as hf

sys.path.append('/home/ubuntu/WQ-Alphas/')
import pandas as pd
import requests
import logging
import plotly.express as px

# Define the file to store the session object and the expiration time (in seconds)
SESSION_CACHE_FILE = 'session_cache.pkl'
SESSION_EXPIRATION_TIME = 3 * 60 * 60  # 3 hours in seconds

# Configure logging
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

def get_or_create_session():
    """
    Get an existing session from cache or create a new one if it doesn't exist or has expired.
    """
    # Check if the session cache file exists
    if os.path.exists(SESSION_CACHE_FILE):
        with open(SESSION_CACHE_FILE, 'rb') as f:
            print("Loading session from cache...")
            cache_data = pickle.load(f)
            session = cache_data['session']
            timestamp = cache_data['timestamp']

            # Check if the session has expired
            if time.time() - timestamp < SESSION_EXPIRATION_TIME:
                print("Session is still valid.")
                return session
            else:
                print("Session has expired. Deleting the cache file...")
                os.remove(SESSION_CACHE_FILE)

    # Create a new session and save it
    print("Creating a new session...")
    session = ace.start_session()
    with open(SESSION_CACHE_FILE, 'wb') as f:
        pickle.dump({'session': session, 'timestamp': time.time()}, f)
    return session

def dowload_datasets(s):
    datasets_df = hf.get_datasets(s,region="GLB") # by default we load all datasets USA TOP3000 delay 1
    datasets_df.to_csv('datasets_usa.csv',index=True)

def read_datasets(path):
    datasets_df = pd.read_csv(path)
    return datasets_df

def get_dataset(session,df):
    top_alphas_df = list(df.sort_values(by='alphaCount',ascending=False)["id"])
    info_of_top_alphas_df = list(df.sort_values(by='alphaCount',ascending=False)["name"])
    # top_alphas_df_id = (top_alphas_df)

    # print("Category:",top_alphas_df_id)

    print("#############################################################################################################")
    for i in range(len(top_alphas_df)):
        try:
            datafields_df = hf.get_datafields(session, dataset_id=top_alphas_df[i]) # download all fields of dataset news92
            # datafields_df = datafields_df.sort_values(by='alphaCount',ascending=False)
            logger.info(f"Category: {top_alphas_df[i]} has this number of fields: {len(datafields_df)}, Info: {info_of_top_alphas_df[i]}")
        except:
            logger.error(f"Error in getting datafields for category {top_alphas_df[i]}")
    
    return datafields_df
# Use the function to get the session



if __name__ == '__main__':
    # Get the session
    session = get_or_create_session()
    get_dataset(session, read_datasets('datasets_usa.csv'))

    




