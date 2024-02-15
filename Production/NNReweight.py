import numpy as np
import pandas as pd
from glob import glob
from selection_utils import sel_oppositesign, sel_samesign
import os
import json



base_dir = "/vols/cms/lcr119/tuples/TauCP/Stitched1502"


# Dictionary with Samples 
# -> Type: - 0: Background
#          - 1: Higgs
#          - 2: Genuine Taus
# -> sum_weights: Sum of weights

samples = {
    'DYto2L_M-50_madgraphMLM': {'type': 2, 'sum_weights': 0 },
    'DYto2L_M-50_madgraphMLM_ext1': {'type': 2, 'sum_weights': 0 },
    'DYto2L_M-50_1J_madgraphMLM': {'type': 2, 'sum_weights': 0 },
    'DYto2L_M-50_2J_madgraphMLM': {'type': 2, 'sum_weights': 0 },
    'DYto2L_M-50_3J_madgraphMLM': {'type': 2, 'sum_weights': 0 },
    'DYto2L_M-50_4J_madgraphMLM': {'type': 2, 'sum_weights': 0 },
    'GluGluHToTauTau_M125': {'type': 1, 'sum_weights': 0, 'higgs_type': 0},
    'VBFHTauTau_M125': {'type': 1, 'sum_weights': 0, 'higgs_type': 1},
    'data_C': {'type': 0, 'sum_weights': 0 }
          }

out_dir = '/vols/cms/lcr119/tuples/TauCP/NN_weighted'


higgs_eff_num = np.zeros(2) # ggH and VBF effective numbers
higgs_sel_num = np.zeros(2) # number selected
higgs_XS = np.array([51.96, 4.067]) # ggH and VBF XSecs



sum_categories = np.array([0, 0, 0]) # sum of weights for all categories


# Get all key info
print("Collecting file information")
for key, info in samples.items():
    files = glob(f"{base_dir}/{key}/*.parquet")
    if info['type'] == 1: # Store effective number of Higgs for weights   
        info["n_eff"] = json.load(open(f"{base_dir}/{key}/Summary.json", "r"))["Effective_Events"] 
        higgs_eff_num[info['higgs_type']] = info["n_eff"]
    for i, f in enumerate(files):
        df = pd.read_parquet(f, engine='pyarrow')
        if info['type'] != 1: # If not higgs store sum weights
            sum_categories[info['type']] += np.sum(df['weight']) 
        else: # If Higgs store in ggH and VBF cat
            higgs_sel_num[info['higgs_type']] += len(df['weight'])

print("---------------------------------------------------")
        
# Figure out Higgs weights
higgs_weights = 1e4*higgs_XS/np.sum(higgs_eff_num)  # XS process/Total Eff Higgs Events
higgs_sel_num_weighted = higgs_weights*higgs_sel_num 
sum_categories[1] = np.sum(higgs_sel_num_weighted) # total weights must account for sum of the Higgs ones
print(f"Higgs weights for ggH and VBF are: {higgs_weights}")


# Figure out Global weights (all 3 categories equal!)
target = np.sum(sum_categories)/3
weights_categories = target/sum_categories
print(f"Category weights for background, Higgs and Taus are: {weights_categories} (target per category was {target})")

print("---------------------------------------------------")


# Save appropriate weights
for key, info in samples.items():
    
    files = glob(f"{base_dir}/{key}/*.parquet")
    print(f"Saving weights for {key}")
    
    # Check output path is open
    out_path = f"{out_dir}/{key}"
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    
    for i, f in enumerate(files):
        # Read the file into a dataframe
        df = pd.read_parquet(f, engine='pyarrow')
        
        df["category_weight"] = weights_categories[info['type']]
        if info['type'] == 1: # Higgs
            df['higgs_weight'] = higgs_weights[info['higgs_type']]
            df['weight'] *= df['category_weight']*df['higgs_weight']
        else: 
            df['weight'] *= df['category_weight']

        # Store the category truth (bkg, higgs, tau)
        df['true_category'] = info['type']
        
        save_path = out_path + f"/{key}_NNweighted_{i}.parquet"
        df.to_parquet(save_path, engine = 'pyarrow')
    
    