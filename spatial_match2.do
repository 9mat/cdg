
// find number of nearby vehicles within a time window from the booking time
args booking_id_file vehloc_dir outdir from to
set more off

// // need to generate the bookings_id_geocode.dta file first
// use id pickup_postcode booking_dt if booking_dt != . using "E:\cdg_data\generated\trips_merged_dec2feb_everything.dta", clear
// rename pickup_postcode postal 
// merge m:1 postal using D:\Dropbox\work\data\dta\postalgeocode.dta, keep(match master) keepusing(lat lon)
// keep id lat lon booking_dt
// save E:\cdg_data\generated\bookings_id_geocode.dta, replace

// // sample command
// do spatial_match2 E:\cdg_data\generated\bookings_id_geocode.dta F:\CDGData\vehicle_location F:/spatial_match 1 10
// do spatial_match2 /NAS/project01/chujunhong_SPIRE/trips/bookings_id_geocode.dta /NAS/project01/chujunhong_SPIRE/vehicle_location /NAS/project01/chujunhong_SPIRE/spatial_match2 1 10

global window_mins 3
global max_radius_km 1
global batch 5

global booking_id_file E:\cdg_data\generated\bookings_id_geocode.dta
global vehloc_dir F:\CDGData\vehicle_location
global outdir F:/spatial_match

global booking_id_file "`booking_id_file'"
global vehloc_dir "`vehloc_dir'"
global outdir "`outdir'"

cap mkdir "`outdir'"

timer clear 1

forvalues i=`from'/`to' {
  timer on 1

  local starttime = `=dhms(td(1dec2016),0,0,0)' + 1000*60*$window_mins*`i'*$batch
  local endime = `starttime' + 1000*60*$window_mins*$batch

  local date = `=dofc(`starttime')'
  local nextdate = `date'+1
  local datestr = `"`=string(`date', "%tdCYND")'"'
  local nextdatestr = `"`=string(`nextdate', "%tdCYND")'"'

  di "`datestr'"

  if `=dofc(`starttime')' > `=td(28feb2017)' continue, break

  use if booking_dt >= `starttime' & booking_dt < `endime' using $booking_id_file, clear
  tempfile tmpbooking
  save `tmpbooking', replace

  use if log_dt >= `starttime' & log_dt < `endime' + 1000*60*$window_mins using "$vehloc_dir/vehicle_location_`datestr'_`nextdatestr'.dta", clear
  // 1 ARRIVED 2 BREAK 3 BUSY 4 FREE 5 NOSHOW 6 OFFLINE 7 ONCALL 8 PAYMENT 9 POB 10 STC
  keep if status == 4
  gen long lid = _n
  tempfile tmploc
  save `tmploc', replace

  geonear lid lat lon using `tmpbooking', long within($max_radius_km) neighbor(id lat lon)

  merge m:1 id using `tmpbooking', keep(match master) keepusing(booking_dt) nogen
  merge m:1 lid using `tmploc', keep(match master) nogen 

  keep if log_dt > booking_dt & log_dt < booking_dt + 1000*60*$window_mins

  collapse (min) km_to_id, by(id vehicle_cd)
  gen count_vehin_1km = km_to_id < 1

  foreach r in 100 200 300 400 500 600 700 800 900 {
    gen count_vehin_`r'm = km_to_id < `r'/1000
  }

  collapse (sum) count_vehin_*, by(id)

  save "$outdir/spatial_match2_`i'", replace

  timer off 1
  timer list 1
}

