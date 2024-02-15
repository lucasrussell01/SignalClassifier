import pandas as pd
import mplhep as hep
from glob import glob
import numpy as np
import json 
from tqdm import tqdm

output = {
    "QCD": {'m_vis': [], 'weight': []},
    "ggH": {'m_vis': [], 'weight': []},
    "VBF": {'m_vis': [], 'weight': []},
    "DY": {'m_vis': [], 'weight': [], 'stitchWeight': []}
}

samples = ['DYto2L_M-50_madgraphMLM',
    'DYto2L_M-50_madgraphMLM_ext1',
    'DYto2L_M-50_1J_madgraphMLM',
    'DYto2L_M-50_2J_madgraphMLM',
    'DYto2L_M-50_3J_madgraphMLM',
    'DYto2L_M-50_4J_madgraphMLM',
    'GluGluHToTauTau_M125',
    'VBFHTauTau_M125',
    'data_C']

base_dir = '/vols/cms/lcr119/tuples/TauCP/NN_weighted'

print("Extracting columns from Dataframes")


for key in tqdm(samples):
    
    files = glob(f"{base_dir}/{key}/*.parquet")
    
    for i, f in enumerate(files):
        # Read the file into a dataframe
        df = pd.read_parquet(f, engine='pyarrow')

        if 'GluGlu' in key:
            out_cat = "ggH"
        elif 'VBF' in key:
            out_cat = "VBF"
        elif 'DYto2L' in key:
            out_cat = "DY"
            output["DY"]['stitchWeight'] = np.concatenate([output["DY"]['stitchWeight'], df["stitchWeight"].values]).tolist()
        elif 'data' in key:
            out_cat = "QCD"
        
        output[out_cat]['m_vis'] = np.concatenate([output[out_cat]['m_vis'], df["m_vis"].values]).tolist()
        output[out_cat]['weight'] = np.concatenate([output[out_cat]['weight'], df["weight"].values]).tolist()
        
        


with open('column.json', 'w') as file:
    json.dump(output, file)

print(f"Dictionary saved")





