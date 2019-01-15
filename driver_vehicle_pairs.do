args command inputpath outputpath

set more off

if "`command'" == "combine" {
  clear
  local filelist: dir "`inputpath'" files "*.dta"
  foreach filename in local filelist {
    append using "`inputpath'/`filename'"
  }
  duplicates drop
  if ~missing("`outputpath'") save "`outputpath'", replace
  exit
}

if "`command'" == "compress" {
  use vehicle_cd driver_cd log_dt using "`inputpath'", clear
  gen date = dofc(log_dt)
  gen hour = hh(log_dt)
  compress
  keep vehicle_cd driver_cd date hour
  duplicates drop
  if ~missing("`outputpath'") save "`outputpath'", replace  
}

if "`command'" == "spell" {
  use vehicle_cd driver_cd log_dt using "`inputpath'", clear
  drop if vehicle_cd == . | driver_cd == .
  bys vehicle_cd driver_cd  (log_dt): gen interval_mins = (log_dt - log_dt[_n-1])/1000/60 if _n > 1
  gen newpell = interval_mins > 10
  bys vehicle_cd driver_cd (log_dt): gen spellnum = sum(newpell)
  fcollapse (min) drspell_start_dt = log_dt (max) drspell_end_dt = log_dt, by(vehicle_cd driver_cd spellnum)
  if ~missing("`outputpath'") save "`outputpath'", replace  
}

