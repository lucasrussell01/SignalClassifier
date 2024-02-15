import numpy as np
import pandas as pd
from glob import glob
import os
import json
from tqdm import tqdm

base_dir = '/vols/cms/lcr119/tuples/TauCP/Concat1302'
out_dir = '/vols/cms/lcr119/tuples/TauCP/Stitched1502'

samples = {
    'DYto2L_M-50_madgraphMLM': {'nJets': 'inc'}, 
    'DYto2L_M-50_madgraphMLM_ext1': {'nJets': 'inc'},
    'DYto2L_M-50_1J_madgraphMLM': {'nJets': '1j'},
    'DYto2L_M-50_2J_madgraphMLM': {'nJets': '2j'},
    'DYto2L_M-50_3J_madgraphMLM': {'nJets': '3j'},
    'DYto2L_M-50_4J_madgraphMLM': {'nJets': '4j'}
          }

xsec = [5455.0, 978.3, 315.1, 93.7, 45.4]

stitch_info = {
    "inc": {'nEff': 0, 'fXS': None},
    "1j": {'nEff': 0, 'fXS': xsec[1]/xsec[0]},
    "2j": {'nEff': 0, 'fXS': xsec[2]/xsec[0]}, 
    "3j": {'nEff': 0, 'fXS': xsec[3]/xsec[0]},
    "4j": {'nEff': 0, 'fXS': xsec[4]/xsec[0]},
        }

     
# Add effective event numbers across the different samples
for key, info in samples.items():
    n_eff_events =  json.load(open(f"{base_dir}/{key}/Summary.json", "r"))["Effective_Events"]
    stitch_info[info['nJets']]['nEff'] += n_eff_events # add to nevents of corresponding category


# Store stitching information in dictionary
for key, info in stitch_info.items(): 
    if key != "inc":
        # calculate weight for different N jets samples
        w_num = info['fXS'] * stitch_info["inc"]["nEff"]
        w_den = info['fXS'] * stitch_info["inc"]["nEff"] + info['nEff']
        info['stitchWeight'] = w_num/w_den
    else:
        info['rel_xsec'] = None
        info['stitchWeight'] = 1

w = [info['stitchWeight'] for key, info in stitch_info.items()]

# Add a stitching weights column to the files, and change overall weight.    
for key, info in samples.items():
    files = glob(f"{base_dir}/{key}/*.parquet")
    print(f"Processing {key}")
    
    out_path = f"{out_dir}/{key}"
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    # Copy across the summary json file for downstream reweighting:
    os.system(f"cp {base_dir}/{key}/Summary.json {out_path}/Summary.json")
        
    for i, f in enumerate(tqdm(files)):
        # Read the file into a dataframe
        df = pd.read_parquet(f, engine='pyarrow')
        df["stitchWeight"] = df["nLHEjets"].apply(lambda n: w[n]) # Apply the weight for correct njet...
        df["weight"] *= df["stitchWeight"]
        
        save_path = out_path + f"/{key}_STITCHED_{i}.parquet"
        df.to_parquet(save_path, engine = 'pyarrow')
        

        
        
        
        
    








