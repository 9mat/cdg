set more off

args vehicle_dir vehicle_group out_dir

local mod_base 100

clear

local filelist: dir "`vehicle_dir'" files "vehicle_location*.dta"

tempfile tmp

timer clear 1
foreach filename of local filelist {
  timer on 1
  di "`filename'"
  
  preserve
  use "`vehicle_dir'/`filename'" if mod(vehicle_cd, `mod_base') == `vehicle_group', clear
  save "`tmp'", replace
  restore
  append using "`tmp'"
  describe
  di r(N)

  timer off 1
  timer list 1
}


describe
if r(N) > 0 {
  save "`out_dir'/vehicle_location_vmod`mod_base'_`vehicle_group'.dta", replace
}

