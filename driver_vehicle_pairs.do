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

use vehicle_cd driver_cd log_dt using "`inputpath'", clear
gen date = dofc(log_dt)
compress
keep vehicle_cd driver_cd date
duplicates drop
if ~missing("`outputpath'") save "`outputpath'", replace
