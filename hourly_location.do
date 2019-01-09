set more off

args ifile ofile

// use F:/CDGData/vehicle_location/vehicle_location_20161201_20161202.dta, clear

use "`ifile'", clear

foreach x in lat lon log_dt {
	bys vehicle_cd (log_dt): gen double next_`x' = `x'[_n+1] if _n < _N
}

geodist lat lon next_lat next_lon, g(km0) sphere
gen double duration0 = next_log_dt - log_dt

gen date = dofc(log_dt)
gen hour = hh(log_dt)
gen double next_exact_hour = dhms(date, hour, 0, 0) + 60*60*1000

gen double duration1 = max(next_exact_hour, next_log_dt) - next_exact_hour
gen km1 = duration1*km0/duration0

replace duration0 = duration0 - duration1
replace km0 = km0 - km1


// gen byte status = 1 if v6 == "ARRIVED"
// replace status =  2 if v6 == "BREAK"
// replace status =  3 if v6 == "BUSY"
// replace status =  4 if v6 == "FREE"
// replace status =  5 if v6 == "NOSHOW"
// replace status =  6 if v6 == "OFFLINE"
// replace status =  7 if v6 == "ONCALL"
// replace status =  8 if v6 == "PAYMENT"
// replace status =  9 if v6 == "POB"
// replace status = 10 if v6 == "STC"

gen combstatus = "pob" if inlist(status, 8, 9, 10)
replace combstatus = "free" if status == 4
replace combstatus = "busy" if inlist(status, 2, 3)
replace combstatus = "oncall" if inlist(status, 1, 5, 7)

drop if driver_cd == . | combstatus == "" | date == . | hour == .

collapse (sum) duration0 duration1 km0 km1, by(driver_cd date hour combstatus)


gen long id = _n

reshape long duration km, i(id) j(nexthour)

replace hour = hour + nexthour
replace date = date + (hour==24)
replace hour = 0 if hour == 24
collapse (sum) duration km, by(driver_cd date hour combstatus)

egen long id = group(driver_cd date hour)
reshape wide duration km, i(id) j(combstatus) string

drop id
order driver_cd date hour

timer off 1 
timer list 1

save "`ofile'", replace
