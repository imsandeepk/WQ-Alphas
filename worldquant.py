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
import plotly.express as px

# Define the file to store the session object and the expiration time (in seconds)
SESSION_CACHE_FILE = 'session_cache.pkl'
SESSION_EXPIRATION_TIME = 3 * 60 * 60  # 3 hours in seconds

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
    top_alphas_df = df.sort_values(by='alphaCount',ascending=False)["id"]
    top_alphas_df_id = list(top_alphas_df)[4]

    print("Category:",top_alphas_df_id)

    print("#############################################################################################################")

    datafields_df = hf.get_datafields(session, dataset_id=top_alphas_df_id) # doenload all fields of dataset news92
    return datafields_df
# Use the function to get the session

def generate_alphas_list(df):
    print("Generating the alphas...")
    expression_list = []
    for i in df["id"]:
        for j in df["id"]:
            if(i==j):
                continue
            expression = f"zscore({i}/(1+{j}))"
            expression_list.append(expression)

    alpha_list = [ace.generate_alpha(x, region= "GLB", universe = "TOP3000",truncation= 0.1,neutralization='INDUSTRY', decay=8) for x in expression_list]
    return list(alpha_list)

import pandas as pd

def simulate(session, alpha_list):
    print("Simulating the alphas...")
    print("Alpha list size:", len(alpha_list))
    data_to_save = []  # Use a list to collect rows

    for outer_idx in range(0, len(alpha_list), 10):
        # Simulate a batch of alphas
        result = ace.simulate_alpha_list_multi(session, alpha_list[outer_idx:outer_idx + 10])
        print("Getting the results...")

        # Extract results where "is_stats" is not None
        result_df = [
            result[x]["is_stats"]
            for x in range(len(result))
            if result[x].get("is_stats") is not None
        ]
        alphas_simulated = [
            result[x]["simulate_data"]
            for x in range(len(result))
            if result[x].get("is_stats") is not None
        ]

        for inner_idx in range(len(result_df)):
            alpha = alpha_list[outer_idx + inner_idx]
            sharpe_value = result_df[inner_idx]["sharpe"]

            # Print information for debugging
            print(alpha["regular"])
            print(sharpe_value)

            # Ensure `sharpe_value` is a scalar
            if isinstance(sharpe_value, pd.Series):
                sharpe_value = sharpe_value.iloc[0]

            if sharpe_value > 1 or sharpe_value < -1:
                # Flatten and clean the result data for saving
                row = {
                    key: value.iloc[0] if isinstance(value, pd.Series) else value
                    for key, value in result_df[inner_idx].items()
                }
                row["alpha"] = alphas_simulated[inner_idx]["regular"]
                data_to_save.append(row)

    # Convert collected data to a DataFrame and save to CSV
    print("#############################################################################################################")
    print("Saving the results...")
    df_save = pd.DataFrame(data_to_save)
    df_save.to_csv("result_zscore_index4.csv", index=False)



if __name__ == '__main__':
    # Get the session
    session = get_or_create_session()

    # Get the datasets
    # dowload_datasets(session)

    # Read the datasets
    datasets_df = read_datasets('datasets_usa.csv')

    # Get the dataset
    dataset_fields=get_dataset(session,datasets_df)
    # Generate the alpha
    alpha_list = generate_alphas_list(dataset_fields)
    print("#############################################################################################################")
    simulate(session,alpha_list)
    




