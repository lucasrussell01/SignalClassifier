import numpy as np
import pandas as pd
from glob import glob
from selection_utils import sel_oppositesign, sel_samesign
import os
import json



ggH_sec = 51.96
qqH_sec = 4.067



sel_dir = "/vols/cms/lcr119/tuples/TauCP/TauTau/"
cfg_dir = "/vols/cms/lcr119/HiggsDNA/output/tt/"


data_end = "/*.parquet"
cfg_end = "/nominal/*.json"

samples = ['GluGluHToTauTau_M125', 'VBFHTauTau_M125', 'QCD_samesign', 'DYJetsToLL_M-50_forPOG']

out_dir = '/vols/cms/lcr119/tuples/TauCP/TauTau_Weighted/'

nHiggs = 0
nQCD = 0
nTau = 0
nALL = 0 # total number as target

for samp in samples:
    
    print(f"\033[1;32m  Processing {samp} \033[0m")

    n_tot = 0 # number selected
    n_eff_tot = 0 # effective number processed

    data_files = glob(sel_dir + samp + data_end)
    
    
    if ("GluGlu" in samp) or ("VBF" in samp):

        cfg_files = glob(cfg_dir + samp + cfg_end)
        
        for (f, c) in zip(data_files, cfg_files):
            
            df = pd.read_parquet(f, engine='pyarrow')
            n_eff = json.load(open(c))["n_eff_events"]
            
            n_tot += len(df["os"])
            n_eff_tot += n_eff
        
        if "GluGlu" in samp:
            w = ggH_sec/n_eff_tot
        elif "VBF" in samp:
            w = qqH_sec/n_eff_tot
        w = 1e4 * w

    else:

        for f in data_files:
            
            df = pd.read_parquet(f, engine='pyarrow')
            n_tot += len(df["os"])

        w = 1
        
    if ("GluGlu" in samp) or ("VBF" in samp):
        nHiggs += n_tot*w
    elif "DY" in samp:
        nTau += n_tot*w
    elif "QCD" in samp:
        nQCD += n_tot*w
    nALL += n_tot
        
    print(f"Weighted Selected Events: {n_tot*w} (Weight is: {w})")
    
    # NOW ADD WEIGHTS AND SAVE:
    i = 0
    
    print("Saving initial weights to dataframes")
    
    for f in data_files:
        
        df = pd.read_parquet(f, engine='pyarrow')
        df["weight"] = w
        
        out_path = f"{out_dir}{samp}/"
        
        if not os.path.exists(out_path):
            os.makedirs(out_path)
            
        out_fname = f"{samp}_weighted_{i}.parquet"
        
        df.to_parquet(out_path + out_fname, engine = 'pyarrow')
        i +=1
        
        
    
print("------ TOTAL STATS ------")

targ = nALL/3
print(f"Total Raw events: {nALL}, target for each category: {targ}")
print(f"Higgs: {nHiggs}, Scale by: {targ/nHiggs}")
print(f"Taus: {nTau}, Scale by: {targ/nTau}")
print(f"QCD: {nQCD}, Scale by: {targ/nQCD}")


for samp in samples:
    
    print(f"Scaling weights for {samp}")

    weighted_data_files = glob(f"{out_dir}{samp}/*weighted*.parquet")
    
    for f in weighted_data_files:
        
        df = pd.read_parquet(f, engine='pyarrow')
        
        if ("GluGlu" in samp) or ("VBF" in samp):
            df["weight"] = df["weight"]*targ/nHiggs
        elif "DY" in samp:
            df["weight"] = df["weight"]*targ/nTau
        elif "QCD" in samp:
            df["weight"] = df["weight"]*targ/nQCD
        
        df.to_parquet(f)
    
        