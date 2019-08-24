set more off
set rmsg on

args dayid out_dir

local date = `=`=td(1dec2016)'+`dayid'-1'
local datestr = `=string(`date',"%tdCCYYNNDD")'
local nextdaystr = `=string(`date'+1,"%tdCCYYNNDD")'

local vehlocppaths E:/CDGData/vehicle_location
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

// sample 1

local mappaths D:/Dropbox/work/data
local mappaths `mappaths' C:/Users/long/Dropbox/work/data
local mappaths `mappaths' /NAS/project01/chujunhong_SPIRE/common

foreach path in `mappaths' {
  capture confirm file "`path'/master-plan-2014-planning-area-latlon/masterplan14_coord.dta"
  if _rc==0 {
    global mappath "`path'/master-plan-2014-planning-area-latlon"
    continue, break
  }
}

if "$mappath" == "" {
  di "Map not found"
  exit
}

// // global commondata D:/Dropbox/work/data/dta
// global commondata C:/Users/long/Dropbox/work/data/dta

// // use F:/CDGData/vehicle_location/vehicle_location_20161203_20161204, clear
// use C:/data/vehicle_location_20161201_20161202, clear


foreach x in lat lon log_dt {
  bys vehicle_cd (log_dt): gen double last_`x' = `x'[_n-1]
  bys vehicle_cd (log_dt): replace last_`x' = `x' if status != 6 & status[_n-1] == 6
}
gen duration_mins = (log_dt - last_log_dt)/1000/60

geodist lat lon last_lat last_lon, gen(dist_km) sphere

gen date = dofc(log_dt)
gen hour = hh(log_dt)
gen min = mm(log_dt)
gen interval15m = int(min/15)
gen double clock_15m = (interval15m*15 + hour*60 + date*24*60)*60*1000
replace duration_mins = (log_dt - clock_15m)/1000/60 if clock_15m > last_log_dt

// exit

// sample 1

* points outside singapore land -- likely to be errors
* however, in terms of status, it does not matter
drop if lat < 1.23765 | lat > 1.47086
drop if lon < 103.60609 | lon > 104.044496
drop if lat < 1.277231 & lon > 103.87861
drop if lat > 1.399197 & lon > 103.932422
drop if lat > 1.443136 & lon > 103.872984

geoinpoly lat lon using $mappath/masterplan14_coord
fcollapse (sum) duration_mins dist_km, by(date hour interval15m _ID status)
compress
fmerge m:1 _ID using $mappath/masterplan14_data, keep(match master) nogen keepusing(PLN_AREA_C)


// preserve
// use postal REGION_C using $commondata/postal_area_region.dta, clear
// set seed 123455
// sample 1000, by(REGION_C) count
// merge m:1 postal using  $commondata/postal.dta, keep(match) nogen
// drop if REGION_C == ""
// tempfile sampledpostal
// save `sampledpostal'
// restore

// gen long id = _n
// geonear id lat lon using `sampledpostal', n(postal latitude longitude) g(postal)
// merge m:1 postal using $commondata/postal_area_region.dta, keep(match master) nogen keepusing(REGION_C)
// rename REGION_C REGION_C_geonear

* create the output folder, if it does not exist
capture mkdir `out_dir'

* output the spells
di "`out_dir'/dur_and_dist_`datestr'.dta"
save `out_dir'/dur_and_dist_`datestr'.dta, replace
