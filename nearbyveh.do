// by Hai Long, 09/2019
// !!! Generate trips_dec2feb_for_nearbyveh.dta first, using the commented block of code at the beginning.

// use id booking_dt req_pickup_dt pickup_postcode job_status if job_status != . using E:/cdg_data/generated/trips_merged_dec2feb_everything, clear

// gen double broadcast_dt = max(req_pickup_dt - 15*60*1000, booking_dt)

// replace pickup_postcode = . if pickup_postcode <= 10000
// merge 1:1 id using F:/Temp/imputed_bookings, keep(match master match_update match_conflict) update keepusing(pickup_postcode) nogen

// rename pickup_postcode postal
// fmerge m:1 postal using D:/Dropbox/work/data/dta/postalgeocode_extra, keep(match master) nogen keepusing(latitude longitude)
// rename postal pickup_postcode
// rename latitude pickup_lat
// rename longitude pickup_lon

// keep id broadcast_dt pickup_lat pickup_lon

// save E:/cdg_data/generated/trips_dec2feb_for_nearbyveh.dta, replace

args dayid outpath

set more off
set rmsg on

local date = `=`=td(1dec2016)'+`dayid'-1'
local datestr = `=string(`date',"%tdCCYYNNDD")'
local nextdaystr = `=string(`date'+1,"%tdCCYYNNDD")'
local nextnextdaystr = `=string(`date'+2,"%tdCCYYNNDD")'
local locfile "vehicle_location_`datestr'_`nextdaystr'.dta"

local timebuffer 10
local timewindow 5


local paths F:/CDGData/vehicle_location
local paths `paths' E:/cdg_data/generated
local paths `paths' /NAS/project01/chujunhong_SPIRE/vehicle_location
local paths `paths' /NAS/project01/chujunhong_SPIRE/trips

foreach path in `paths' {
  capture confirm file "`path'/`locfile'"
  if _rc==0 {
    global locpath "`path'"
    continue, break
  }
}

foreach path in `paths' {
  capture confirm file "`path'/trips_dec2feb_for_nearbyveh.dta"
  if _rc==0 {
    global trippath "`path'"
    continue, break
  }
}


timer clear 1

local windowstart `=dhms(`date',0,0,0)'
local nextdaystart `=dhms(`date'+1,0,0,0)'
local nextnextdaystart `=dhms(`date'+2,0,0,0)'

use if log_dt < `nextdaystart' + `timebuffer'*60*1000 using $locpath/vehicle_location_`nextdaystr'_`nextnextdaystr', clear
append using $locpath/`locfile'

bys vehicle_cd (log_dt): gen double last_log_dt = log_dt[_n-1] if status[_n-1] != 6
bys vehicle_cd (log_dt): replace last_log_dt = log_dt if status[_n-1] == 6
drop if vehicle_cd == . | driver_cd == . | status == 6
gen long log_id = _n

tempfile modified_locfile
save `modified_locfile'

// profiler on

clear
tempfile combinedfile
save "`combinedfile'", emptyok

while `windowstart' < `nextdaystart' - 1 { 

  timer on 1

  di "======================================================================================="
  di "`datestr' `=hh(`windowstart')'h `=mm(`windowstart')'m"

  local windowend `=`windowstart' + `timewindow'*1000*60'
  local bufferend `=`windowend' + `timebuffer'*1000*60'


  use if inrange(log_dt, `windowstart', `bufferend') & ~inlist(., lat, lon) using `modified_locfile', clear

  if _N == 0 {
    local windowstart `windowend'
    continue
  }

  tempfile windowlocfile
  save "`windowlocfile'"


  use if inrange(broadcast_dt, `windowstart', `windowend'-1) & ~inlist(., pickup_lat, pickup_lon) using $trippath/trips_dec2feb_for_nearbyveh.dta, clear

  if _N == 0 {
    local windowstart `windowend'
    continue
  }
  
  tempfile bookingfile
  save "`bookingfile'"


  geonear id pickup_lat pickup_lon using "`windowlocfile'", n(log_id lat lon) long within(2)
  fmerge m:1 id using "`bookingfile'", keep(match master) keepusing(broadcast_dt) nogen
  fmerge m:1 log_id using "`windowlocfile'", keep(match master) keepusing(log_dt last_log_dt status vehicle_cd) nogen

  timer on 2
  keep if inrange(log_dt, broadcast_dt, broadcast_dt + `timebuffer'*60*1000)
  timer off 2
  timer list 2

  gen byte combstatus = 1 if status == 4
  replace combstatus = 2 if inlist(status, 1, 5, 7, 8, 9, 10)
  replace combstatus = 3 if inlist(status, 2, 3)

  label define combstatus 1 free 2 work 3 busy
  label values combstatus combstatus

  gen mins = (log_dt - broadcast_dt)/1000/60

  foreach radius in 500 1000 2000 {
    foreach time in 3 5 10 {
      local suffix `=`time'*10000+`radius''
      gen veh_mins`suffix' = (log_dt - max(broadcast_dt, last_log_dt))/1000/60 if (log_dt -broadcast_dt)/1000/60 <= `time' & km_to_log_id <= `radius'/1000
      gen veh_count`suffix' = 1 if (log_dt -broadcast_dt)/1000/60 <= `time' & km_to_log_id <= `radius'/1000
      // egen veh_count`suffix' = tag(id combstatus vehicle_cd) if (log_dt -broadcast_dt)/1000/60 <= `time' & km_to_log_id <= `radius'/1000
    }
  }


  if _N == 0 {
    local windowstart `windowend'
    continue
  }

  fcollapse (sum) veh_mins* (max) veh_count*, by(id combstatus vehicle_cd)
  fcollapse (sum) veh_mins* veh_count*, by(id combstatus)

  append using `combinedfile'
  save "`combinedfile'", replace

  local windowstart `windowend'

  timer off 1
  timer list 1


}

cap mkdir `outpath'
save `outpath'/nearbyveh_`datestr'.dta, replace

// profiler off
// profiler report


