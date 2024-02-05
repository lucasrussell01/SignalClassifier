import numpy as np
import pandas as pd
from glob import glob
import os


base_dir = '/vols/cms/lcr119/HiggsDNA/output/tt/'
file_end = "/*/*.parquet"
samples = ['GluGluHToTauTau_M125', 'VBFHTauTau_M125', 'data_C', 'DYJetsToLL_M-50_forPOG']

out_dir = '/vols/cms/lcr119/tuples/TauCP/TauTau/'


for samp in samples:
    
    i = 0
    first = True
    
    print(f"\033[1;32m  Processing {samp} \033[0m")
    n = 0
    n_bef = 0

    if "data" in samp: # don't use all data
        files = glob(base_dir + samp + file_end)[:15]
    else:
        files = glob(base_dir + samp + file_end)
    
    for f in files:
        
        df = pd.read_parquet(f, engine='pyarrow')
        n_bef += len(df["os"])
        
        if 'data' in samp:
            # QCD
            if first:
                print("\033[1;31mWARNING:\033[0m Selecting \033[1msame\033[0m sign pairs")
                first = False
            samp = "QCD_samesign" # rename for output
            df = df[df["os"]==False] 
            
        else:
            if first:
                print("Selecting opposite \033[1mopposite\033[0m sign pairs") 
                first = False
            df = df[df["os"]==True] 
        
        
        out_path = f"{out_dir}{samp}/"
        
        if not os.path.exists(out_path):
            os.makedirs(out_path)
            
        out_fname = f"{samp}_{i}.parquet"
        
        df.to_parquet(out_path + out_fname, engine = 'pyarrow')
        
        n += len(df["os"])
        i += 1
        
    print(f"Original number of events in {samp}: {n_bef}")
    print(f"Number of \033[1mselected\033[0m events in {samp}: {n} ({round(100*n/n_bef, 3)}%)")


