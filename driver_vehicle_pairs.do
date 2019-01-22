args command inputpath outputpath
set more off

if "`command'" == "combine" {
  clear
  local filelist: dir "`inputpath'" files "*.dta"
  foreach filename in local filelist {
    append using "`inputpath'/`filename'"
  }
  duplicates drop
}

if "`command'" == "compress" {
  use vehicle_cd driver_cd log_dt using "`inputpath'", clear
  gen date = dofc(log_dt)
  gen hour = hh(log_dt)
  compress
  keep vehicle_cd driver_cd date hour
  duplicates drop
}

if "`command'" == "spell" {
  use vehicle_cd driver_cd log_dt using "`inputpath'", clear
  drop if vehicle_cd == . | driver_cd == .
  bys vehicle_cd driver_cd  (log_dt): gen interval_mins = (log_dt - log_dt[_n-1])/1000/60 if _n > 1
  gen newpell = interval_mins > 10
  bys vehicle_cd driver_cd (log_dt): gen spellnum = sum(newpell)
  fcollapse (min) drspell_start_dt = log_dt (max) drspell_end_dt = log_dt, by(vehicle_cd driver_cd spellnum)
}

if "`command'" == "vspell" {
  use vehicle_cd driver_cd log_dt using "`inputpath'", clear
  bys vehicle_cd (log_dt): gen newpell = driver_cd != driver_cd[_n-1] if _n > 1
  bys vehicle_cd (log_dt): replace newpell = 1 if _n == 1
  bys vehicle_cd (log_dt): gen spellnum = sum(newpell)
  fcollapse (min) vspell_start_dt = log_dt (max) vspell_end_dt = log_dt (first) driver_cd, by(vehicle_cd spellnum) fast
  drop spellnum
}

if "`command'" == "oncallspell" {
  use vehicle_cd log_dt status using "`inputpath'", clear  
  gen byte oncall = inlist(status, 1, 5, 7)
  bys vehicle_cd (log_dt): gen newoncall = oncall & (_n==1 | ~oncall[_n-1] | (log_dt - log_dt[_n-1])/1000/60 > 10)
  keep if oncall
  bys vehicle_cd (log_dt): gen oncallnum = sum(newoncall)
  fcollapse (min) oncall_start_dt = log_dt (max) oncall_end_dt = log_dt, by(vehicle_cd oncallnum)
  drop oncallnum
}



if "`command'" == "oncallspellfull" {
  use "`inputpath'", clear  
  gen byte oncall = inlist(status, 1, 5, 7)

  drop if lat < 1.23765 | lat > 1.47086
  drop if lon < 103.60609 | lon > 104.044496
  drop if lat < 1.277231 & lon > 103.87861
  drop if lat > 1.399197 & lon > 103.932422
  drop if lat > 1.443136 & lon > 103.872984

  bys vehicle_cd (log_dt): gen newoncall = oncall & (_n==1 | ~oncall[_n-1] | (log_dt - log_dt[_n-1])/1000/60 > 10 | driver_cd != driver_cd[_n-1])
  bys vehicle_cd (log_dt): gen oncallnum = sum(newoncall)
  bys vehicle_cd (log_dt): gen next_lat = lat[_n+1] if _n < _N
  bys vehicle_cd (log_dt): gen next_lon = lon[_n+1] if _n < _N

  keep if oncall
  sort vehicle_cd log_dt

  geodist lat lon next_lat next_lon, g(km_to_next) sphere
  fcollapse (min) oncall_start_dt = log_dt (max) oncall_end_dt = log_dt (firstnm) oncall_start_lat = lat oncall_start_lon = lon driver_cd (lastnm) oncall_end_lat = lat oncall_end_lon = lon (sum) oncall_km = km_to_next, by(vehicle_cd oncallnum)
  drop oncallnum
}

if "`command'" == "freespellfull" {
  use "`inputpath'", clear  

  drop if lat < 1.23765 | lat > 1.47086
  drop if lon < 103.60609 | lon > 104.044496
  drop if lat < 1.277231 & lon > 103.87861
  drop if lat > 1.399197 & lon > 103.932422
  drop if lat > 1.443136 & lon > 103.872984

  bys vehicle_cd (log_dt): gen newfree = status==4 & (_n==1 | status[_n-1] != 4 | (log_dt - log_dt[_n-1])/1000/60 > 10 | driver_cd != driver_cd[_n-1])
  bys vehicle_cd (log_dt): gen freenum = sum(newfree)
  bys vehicle_cd (log_dt): gen next_lat = lat[_n+1] if _n < _N
  bys vehicle_cd (log_dt): gen next_lon = lon[_n+1] if _n < _N

  keep if status == 4
  sort vehicle_cd log_dt

  geodist lat lon next_lat next_lon, g(km_to_next) sphere
  fcollapse (min) free_start_dt = log_dt (max) free_end_dt = log_dt (firstnm) free_start_lat = lat free_start_lon = lon driver_cd (lastnm) free_end_lat = lat free_end_lon = lon (sum) free_km = km_to_next, by(vehicle_cd freenum)
  drop freenum
}

compress
if ~missing("`outputpath'") save "`outputpath'", replace
