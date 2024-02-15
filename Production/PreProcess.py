import numpy as np
import pandas as pd
from glob import glob
import os
import json
from tqdm import tqdm

base_dir = '/vols/cms/lcr119/HiggsDNA/output/tt'

samples = {
    'DYto2L_M-50_madgraphMLM': {'os': True, 'max_files': None},
    'DYto2L_M-50_madgraphMLM_ext1': {'os': True, 'max_files': None},
    'DYto2L_M-50_1J_madgraphMLM': {'os': True, 'max_files': None},
    'DYto2L_M-50_2J_madgraphMLM': {'os': True, 'max_files': None},
    'DYto2L_M-50_3J_madgraphMLM': {'os': True, 'max_files': None},
    'DYto2L_M-50_4J_madgraphMLM': {'os': True, 'max_files': None},
    'GluGluHToTauTau_M125': {'os': True, 'max_files': None},
    'VBFHTauTau_M125': {'os': True, 'max_files': None},
    'data_C': {'os': False, 'max_files': 40}
          }

out_dir = '/vols/cms/lcr119/tuples/TauCP/Concat1302'

def concat_and_save(df_list, save_path):
    save_df = pd.concat(df_list, ignore_index = True)
    save_df.to_parquet(save_path, engine = 'pyarrow')
    
    

for key, sel in samples.items():
    
    n_sel = 0 # total selected
    n_tot = 0 # total processed
    n_eff_tot = 0 # total number of effective events
    
    # Find files for given dataset
    files = glob(f"{base_dir}/{key}/nominal/*.parquet")[:sel['max_files']] # data
    run_info = json.load(open(f"{base_dir}/{key}/nominal/run_info.json", "r"))

    print(f"Processing {len(files)} files for Dataset:\033[1m{key}\033[0m")
    
    if sel['os']:
        print("Selecting \033[1mopposite\033[0m sign pairs")
    else:
        print("Selecting \033[1msame\033[0m sign pairs")
    
    sel_count = 0 # Track number of selected Events in current file save
    save_list = [] # List of df to save
    
    out_path = f"{out_dir}/{key}/"
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    n_shards = 0 # Track how many files have been saved
    
    for f in tqdm(files):
        
        # Read the file into a dataframe
        df = pd.read_parquet(f, engine='pyarrow')
        n_tot += len(df["os"])
        
        # Read in effective event info
        n_eff_tot += run_info[f.split("/nominal/")[1]] # Neff corresponding to the file key
        
        # Select sign of the pair
        if sel['os']:
            df = df[df["os"]==True] 
        else:
            df = df[df["os"]==False]     
        n_sel += len(df["os"])
        
        sel_count += len(df["os"]) # Track how many selected in current batch
        save_list.append(df) # Add to list of df to save in this file
        
        # Save the selected events 
        if sel_count > 10000:
            print(f"Saving {sel_count} events from {len(save_list)} files")
            concat_and_save(save_list, f"{out_path}{key}_{n_shards}.parquet")
            n_shards += 1
            sel_count = 0
            save_list = []
    
    print(f"Saving {sel_count} events from {len(save_list)} files")
    concat_and_save(save_list, f"{out_path}{key}_{n_shards}.parquet")
    
    #  Save dataset summary information
    json.dump({"Selected_Events": n_sel, "Effective_Events": n_eff_tot}, open(f"{out_path}Summary.json", "w"))

    print(f"{key}: \033[1mSelected\033[0m {n_sel} events (originally {n_tot} -> {round(n_sel*100/n_tot, 3)}%). Effective number of events: {n_eff_tot}.")
    print("----------------------------------------------------------------")
          

