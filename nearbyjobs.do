args postaldemandfile nearbymatchfile outputfolder ddd

capture program drop prepare_nearby_demand
program define prepare_nearby_demand

  import delimited D:\Dropbox\work\data\Singapore_postcode_geocoordinates.csv, clear

  keep postal latitude longitude
  drop if postal == "NIL"
  destring postal, replace
  bys postal: keep if _n == 1

  preserve
  rename postal nearby_postal
  save E:/tmp/nearby_postal.dta, replace
  restore

  geonear postal latitude longitude using E:/tmp/nearby_postal.dta, neighbors(nearby_postal latitude longitude) long within(1) 

  drop km_*

  save E:/cdg_data/generated/match_nearby_postal.dta, replace


  use E:/cdg_data/generated/trips_merged_dec2feb.dta, clear

  gen double ref_dt = trip_start_dt if job_status == .
  replace ref_dt = req_pickup_dt if job_status != .

  keep pickup_postcode ref_dt

  gen date = dofc(ref_dt)
  gen hour = hh(ref_dt)
  gen demand = 1

  compress

  fcollapse (sum) demand, by(pickup_postcode date hour)
  save E:/tmp/hourly_demand_postal.dta, replace

end

timer on 1

use `postaldemandfile' if date == `ddd', clear
gegen long id = group(pickup_postcode date)
reshape wide demand, i(id) j(hour)
rename pickup_postcode nearby_postal
//join, into(`nearbymatchfile') by(nearby_postal) keep(match) nogen
merge 1:m nearby_postal using `nearbymatchfile', keep(match) nogen
gcollapse (sum) demand*, by(postal date)
compress

cap mkdir `outputfolder'
saveold `outputfolder'/nearby_demand_`=string(`ddd',"%tdCCYYNNDD")', replace version(13)

timer off 1
timer list 1

// do nearby_geocodes.do E:/tmp/hourly_demand_postal.dta E:/cdg_data/generated/match_nearby_postal.dta `=td(2dec2016)'
