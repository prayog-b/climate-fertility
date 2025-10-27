********************************************************************************
* PROJECT:           Climate Change and Fertility 
* AUTHOR:            Prayog Bhattarai
* DATE CREATED:      2025-06-09
* DATE MODIFIED:     2025-10-09
* DESCRIPTION:       Download crop phenology data using shell script     
* DEPENDENCIES:      
* 
* NOTES: 
********************************************************************************


* Define local download directory
local download_path "$cf/data/source/fao/crop-phenology"

!mkdir -p "`download_path'"

* Download files using gsutil
// Call gsutil with multiple files using the shell from within Stata
!gsutil -m cp ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.EOS.LC-C.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.EOS.LC-C.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.EOS.LC-G.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.EOS.LC-G.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.MOS.LC-C.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.MOS.LC-C.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.MOS.LC-G.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.MOS.LC-G.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.SOS.LC-C.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.SOS.LC-C.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.SOS.LC-G.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS1.SOS.LC-G.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.EOS.LC-C.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.EOS.LC-C.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.EOS.LC-G.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.EOS.LC-G.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.MOS.LC-C.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.MOS.LC-C.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.MOS.LC-G.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.MOS.LC-G.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.SOS.LC-C.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.SOS.LC-C.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.SOS.LC-G.json" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.GS2.SOS.LC-G.tif" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.html" ///
  "gs://fao-gismgr-asis-data/DATA/ASIS/MAPSET/PHE/ASIS.PHE.json" ///
  "`download_path'"

clear
  
