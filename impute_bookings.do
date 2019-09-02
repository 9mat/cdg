args dayid outdir

set more off
set rmsg on

local paths "E:/cdg_data/generated"
local paths `paths' "C:/data"
local paths `paths' "/NAS/project01/chujunhong_SPIRE/trips"
local paths `paths' "F:/CDGData/vehicle_location"
local paths `paths' "/NAS/project01/chujunhong_SPIRE/vehicle_location"
local paths `paths' "D:/Dropbox/work/data/dta"
local paths `paths' "C:/Users/dhlong/Dropbox/work/data/dta"
local paths `paths' "/NAS/project01/chujunhong_SPIRE/common"


foreach path in `paths' {
  capture confirm file "`path'/vehicle_location_20161201_20161202.dta"
  if _rc==0 & "$locpath"=="" {
    global locpath "`path'"
  }

  capture confirm file "`path'/postalgeocode.dta"
  if _rc==0 & "$commonpath"=="" {
    global commonpath "`path'"
  }

  capture confirm file "`path'/trips_merged_dec2feb_everything.dta"
  if _rc==0 & "$datapath"=="" {
    global datapath "`path'"
  }
}

local date = `=`=td(1dec2016)'+`dayid'-1'
local datestr = `=string(`date',"%tdCCYYNNDD")'
local nextdaystr = `=string(`date'+1,"%tdCCYYNNDD")'

use * if log_dt != . & vehicle_cd != . using "$locpath/vehicle_location_`datestr'_`nextdaystr'.dta", clear
bys vehicle_cd log_dt: keep if _n == 1
rename vehicle_cd vehicle_cd_loc
tempfile locfile
save `locfile'

bys vehicle_cd_loc (log_dt): keep if inlist(status, 1, 5, 7) & ~inlist(status[_n-1], 1, 5, 7)
tempfile oncallstartfile
save `oncallstartfile'

use `locfile', clear
bys vehicle_cd_loc (log_dt): keep if inlist(status, 1, 5, 7) & ~inlist(status[_n+1], 1, 5, 7)
tempfile oncallendfile
save `oncallendfile'



// impute destination postal code
use id vehicle_cd trip_end_dt dest_postcode if (dest_postcode == . | dest_postcode <= 10000) & dofc(trip_end_dt) == `date' using $datapath/trips_merged_dec2feb_everything, clear
merge m:1 vehicle_cd using $datapath/vehicle_cd_match, keep(match master) nogen

keep id vehicle_cd_loc trip_end_dt
rename trip_end_dt log_dt
nearmrg vehicle_cd_loc using `locfile', nearvar(log_dt) limit(10*60*1000) keep(match master) nogen keepusing(lat lon)
geonear id lat lon using $commonpath/postalgeocode, n(postal latitude longitude) g(imputed_dest_postcode)
keep id imputed_dest_postcode

save `outdir'/imputed_dest_`datestr', replace





use id vehicle_cd trip_start_dt pickup_postcode if (pickup_postcode == . | pickup_postcode <= 10000) & dofc(trip_start_dt) == `date' using $datapath/trips_merged_dec2feb_everything, clear
merge m:1 vehicle_cd using $datapath/vehicle_cd_match, keep(match master) nogen

keep id vehicle_cd_loc trip_start_dt
renam trip_start_dt log_dt
nearmrg vehicle_cd_loc using `locfile', nearvar(log_dt) limit(10*60*1000) keep(match master) nogen keepusing(lat lon)
geonear id lat lon using $commonpath/postalgeocode, n(postal latitude longitude) g(imputed_pickup_postcode)
keep id imputed_pickup_postcode

save `outdir'/imputed_pickup_`datestr', replace



// 2 is coded for COMPLETED job
use id vehicle_cd trip_start_dt job_status if job_status == 2 & dofc(trip_start_dt) == `date' using $datapath/trips_merged_dec2feb_everything, clear
merge m:1 vehicle_cd using $datapath/vehicle_cd_match, keep(match master) nogen

gen double log_dt = trip_start_dt + 10*1000
nearmrg vehicle_cd_loc using `oncallstartfile', nearvar(log_dt) lower nogen genmatch(oncall_start_dt) keepusing(lat lon) limit(15*60*1000)
rename lat oncall_start_lat
rename lon oncall_start_lon
keep id oncall_start_lon oncall_start_lat oncall_start_dt
save `outdir'/imputed_oncallstart1_`datestr', replace



// rerefence time for CNS is the broadcasting time, which is the later of either the booking time or 15 minutes before the requested pickup
// add a margin of 10 seconds before the reference to account of possible technical errors

// 1 is CANCELLED and 4 is NO SHOW
use id vehicle_cd booking_dt req_pickup_dt job_status if inlist(job_status, 1, 4) & inlist(`date', dofc(req_pickup_dt - 15*60*1000), dofc(booking_dt)) using $datapath/trips_merged_dec2feb_everything, clear
merge m:1 vehicle_cd using $datapath/vehicle_cd_match, keep(match master) nogen

gen double log_dt = max(booking_dt, req_pickup_dt - 15*60*1000) - 10*1000 
nearmrg vehicle_cd_loc using `oncallstartfile', nearvar(log_dt) upper nogen genmatch(oncall_start_dt) keepusing(lat lon) limit(15*60*1000)
rename lat oncall_start_lat
rename lon oncall_start_lon
save `outdir'/imputed_oncallstart2_`datestr', replace




use id vehicle_cd booking_dt req_pickup_dt job_status if inlist(job_status, 1, 4) & inlist(`date', dofc(req_pickup_dt - 15*60*1000), dofc(booking_dt)) using $datapath/trips_merged_dec2feb_everything, clear
merge m:1 vehicle_cd using $datapath/vehicle_cd_match, keep(match master) nogen

gen double log_dt = max(booking_dt, req_pickup_dt - 15*60*1000) - 10*1000 
nearmrg vehicle_cd_loc using `oncallendfile', nearvar(log_dt) upper nogen genmatch(oncall_end_dt) keepusing(lat lon) limit(15*60*1000)
rename lat oncall_end_lat
rename lon oncall_end_lon
save `outdir'/imputed_oncallend2_`datestr', replace
