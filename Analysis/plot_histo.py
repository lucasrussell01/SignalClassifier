import numpy as np
import matplotlib.pyplot as plt
import json
import mplhep as hep

plt.style.use(hep.style.ROOT)
purple = (152/255, 152/255, 201/255)


yellow = (243/255,170/255,37/255)
blue = (2/255, 114/255, 187/255)
green = (159/255, 223/255, 132/255)
red = (203/255, 68/255, 10/255)

plt.rcParams.update({"font.size": 14})


bin_size = 10
bins = np.arange(0, 300 + bin_size, bin_size)
bin_centre = bins[:-1]+ np.diff(bins)/2

step_edges = np.append(bins,2*bins[-1]-bins[-2])

file_path = "column.json"

# Read the JSON file
with open(file_path, 'r') as file:
    data = json.load(file)



taus = np.histogram(data["DY"]["m_vis"], bins = bins, weights = data["DY"]["weight"])[0]
qcd = np.histogram(data["QCD"]["m_vis"], bins=bins, weights = data["QCD"]["weight"])[0]



fig, ax = plt.subplots(figsize = (6,6))

ax.bar(bin_centre, taus, width = bin_size, color = yellow, label = r"$Z\to\tau\tau$")
ax.bar(bin_centre, qcd, width = bin_size, color = green, bottom = taus, label = r"jet $\to \tau_h$")

taus_step = np.append(np.insert(taus,0,0.0),0.0)
qcd_step = np.append(np.insert(qcd,0,0.0),0.0) + taus_step

ax.step(step_edges, taus_step, color='black', linewidth = 0.5)
ax.step(step_edges, qcd_step, color='black', linewidth = 0.5)

ax.hist(data["ggH"]["m_vis"], histtype="step", bins=bins, color = red, linewidth = 2, label = r"ggH$\to\tau\tau$", weights=data["ggH"]["weight"])
ax.hist(data["VBF"]["m_vis"], histtype="step", bins=bins, color = blue, linewidth = 2, label = r"qqH$\to\tau\tau$", weights=data["VBF"]["weight"])

ax.set_xlabel(r"m$_{vis}$ (GeV)")
ax.set_ylabel(f"Weighted Events/{bin_size} GeV")
ax.set_xlim(0, 300)
ax.text(0.56, 1.02, "2022 PreEE (13.6 TeV)", fontsize=14, transform=ax.transAxes)
ax.text(0.01, 1.02, 'CMS', fontsize=20, transform=ax.transAxes, fontweight='bold', fontfamily='sans-serif')
ax.text(0.16, 1.02, 'Simulation', fontsize=16, transform=ax.transAxes, fontstyle='italic',fontfamily='sans-serif')

ax.legend()
plt.savefig(f"m_vis.pdf")