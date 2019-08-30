set more off

// args ifile ofile

args dayid out_dir

local date = `=`=td(1dec2016)'+`dayid'-1'
local datestr = `=string(`date',"%tdCCYYNNDD")'
local nextdaystr = `=string(`date'+1,"%tdCCYYNNDD")'

local vehlocppaths F:/CDGData/vehicle_location
local vehlocppaths `vehlocppaths' C:/data
local vehlocppaths `vehlocppaths' /NAS/project01/chujunhong_SPIRE/vehicle_location

foreach path in `vehlocppaths' {
  di "`path'"
  capture confirm file "`path'/vehicle_location_`datestr'_`nextdaystr'.dta"
  if _rc==0 {
    local vehlocfile "`path'/vehicle_location_`datestr'_`nextdaystr'.dta"
    use "`vehlocfile'", clear
    continue, break
  }
}

if "`vehlocfile'" == "" {
  di "File not found"
  exit
}

* points outside singapore land -- likely to be errors
* however, in terms of status, it does not matter
drop if lat < 1.23765 | lat > 1.47086
drop if lon < 103.60609 | lon > 104.044496
drop if lat < 1.277231 & lon > 103.87861
drop if lat > 1.399197 & lon > 103.932422
drop if lat > 1.443136 & lon > 103.872984

use F:/CDGData/vehicle_location/vehicle_location_20161201_20161202.dta, clear

// use "`ifile'", clear

gen long fivemin = int(log_dt/1000/60/5)
bys vehicle_cd driver_cd fivemin (log_dt): keep if _n == 1
drop if driver_cd == .
drop fivemin
compress

save `out_dir'/loc_status_5m_`datestr'.dta, replace

