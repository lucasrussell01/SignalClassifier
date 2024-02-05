from pandas import read_parquet
import mplhep as hep
import glob
import numpy as np
import json 

datasets = {
    "QCD_samesign": {'name': "QCD_samesign", 'mvis': [], 'weight': []},
    "GluGluHToTauTau_M125": {'name': "GluGluHToTauTau_M125", 'mvis': [], 'weight': []},
    "VBFHTauTau_M125": {'name': "VBFHTauTau_M125", 'mvis': [], 'weight': []},
    "DYJetsToLL_M-50_forPOG": {'name': "DYJetsToLL_M-50_forPOG", 'mvis': [], 'weight': []}
}


for key, sample_dict in datasets.items():
    

    print(f"Processing dataset {key}")
    
    paths = glob.glob(f"/vols/cms/lcr119/tuples/TauCP/TauTau_Weighted/{key}/*.parquet")
    
    mvis = []
    weights = []
    
    for path in paths:
        df = read_parquet(path,engine="pyarrow")
        mvis.append(df["m_vis"].values)
        weights.append(df["weight"].values)


    mvis = np.concatenate(mvis)
    weights = np.concatenate(weights)
    
    sample_dict["mvis"] = mvis.tolist()
    sample_dict["weight"] = weights.tolist()

    # print(sample_dict)
    print(f"Weighted number of events: {np.sum(weights)}")


with open('test_out', 'w') as file:
    json.dump(datasets, file)

print(f"Dictionary saved")





