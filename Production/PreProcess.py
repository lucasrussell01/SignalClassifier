import numpy as np
import pandas as pd
from glob import glob
import os
import json
from tqdm import tqdm

base_dir = '/vols/cms/lcr119/HiggsDNA/output/tt'
file_end = "/nominal/*.parquet"

samples = {
    'DYto2L_M-50_madgraphMLM_ext1': {'os': True, 'max_files': None},
    'GluGluHToTauTau_M125': {'os': True, 'max_files': None},
    'VBFHTauTau_M125': {'os': True, 'max_files': None}}#,
    # 'data_C': {'os': False, 'max_files': 20}
    #       }

out_dir = '/vols/cms/lcr119/tuples/TauCP/TauTau'

for key, sel in samples.items():
    
    n_sel = 0 # total selected
    n_tot = 0 # total processed
    n_eff_tot = 0 # total number of effective events
    
    # Find files for given dataset
    data_files = glob(f"{base_dir}/{key}{file_end}")[:sel['max_files']] # data
    
    print(f"Processing {len(data_files)} files for Dataset:\033[1m{key}\033[0m")
    
    if sel['os']:
        print("Selecting \033[1mopposite\033[0m sign pairs")
    else:
        print("Selecting \033[1msame\033[0m sign pairs")
    
    for i, f in enumerate(tqdm(data_files)):
        
        # Read the file into a dataframe
        df = pd.read_parquet(f, engine='pyarrow')
        n_tot += len(df["os"])
        
        # Read in effective event info
        run_info = f"{f.split('.parquet')[0]}.json" # info on N effective
        n_eff_tot += json.load(open(run_info))["n_eff_events"]
        
        # Select sign of the pair
        if sel['os']:
            df = df[df["os"]==True] 
        else:
            df = df[df["os"]==False]     
        n_sel += len(df["os"])
        
        # Save the selected events
        out_path = f"{out_dir}/{key}/"
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        out_fname = f"{key}_{i}.parquet"
        df.to_parquet(out_path + out_fname, engine = 'pyarrow')
        
    # Save dataset summary information
    json.dump({"Selected_Events": n_sel, "Effective_Events": n_eff_tot}, open(f"{out_path}Summary.json", "w"))
        
    print(f"{key}: \033[1mSelected\033[0m {n_sel} events (originally {n_tot} -> {round(n_sel*100/n_tot, 3)}%). Effective number of events: {n_eff_tot}.")
    print("----------------------------------------------------------------")
          

