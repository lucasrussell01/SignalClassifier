# Production

`PreProcess.py` will select same sign or opposite sign events from the `parquet` files that are outputed by HiggsDNA.

`BaseReweight.py` will read in the effective number of events from the `json` files stored alongside the `parquet` files by HiggsDNA (my Fork).
The ggH and VBF contributions will then be weighted to the expected fractions. All 3 categories (Higgs, Taus, Fakes) will then be reweighted so that they have equal importance for training.