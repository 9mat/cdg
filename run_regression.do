args inputfile outputfolder prefix regid

set more off

use "`inputfile'", clear

if `regid' >= 40 {
  cap bys driver_cd shift_num: egen shift_working_hours = total(working_hours)
  cap gen remaining_idle_pct = 100 - 100*(shift_working_hours - cum_working_hours)*60/remaining_mins
}

gen ref_postcode = dest_postcode if job_status == . | job_status == "COMPLETED":job_status
replace ref_postcode = pickup_postcode if inlist(job_status, "CANCELLED":job_status, "NO SHOW":job_status)

replace nearby_50m = nearby_50m/1000
replace nearby_500m = nearby_500m/1000
replace demand = demand/1000

capture program drop estadd_hdfe
program define estadd_hdfe, eclass
  foreach x in `e(absvars)' {
    ereturn local `x'_fe = "Yes"
  }
end

**************************************
* quit
if `regid' == 11 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd) cluster(driver_cd)
} 
else if `regid' == 12 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow) cluster(driver_cd)
}
else if `regid' == 13 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 14 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 15 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 16 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 17 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 18 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* remaining_mins
else if `regid' == 21 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date) cluster(driver_cd)
} 
else if `regid' == 22 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow) cluster(driver_cd)
}
else if `regid' == 23 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 24 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 25 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 26 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 27 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 28 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* remaining_wage
else if `regid' == 31 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date) cluster(driver_cd)
} 
else if `regid' == 32 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date hour#dow) cluster(driver_cd)
}
else if `regid' == 33 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 34 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 35 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 36 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 37 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 38 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* remaining_idle_pct
else if `regid' == 41 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date) cluster(driver_cd)
} 
else if `regid' == 42 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date hour#dow) cluster(driver_cd)
}
else if `regid' == 43 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 44 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 45 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 46 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 47 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 48 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* main quit
else if `regid' == 51 {
  reghdfe quit cum_cancellations cum_noshows cum_hours cum_income_100 demand, absorb(driver_cd date houroffday) cluster(driver_cd)
}
else if `regid' == 52 {
  reghdfe quit cum_cancellations cum_noshows demand, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins) cluster(driver_cd)
}
else if `regid' == 53 {
  reghdfe quit cum_cancellations cum_noshows demand, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins zonecode) cluster(driver_cd)
}
else if `regid' == 54 {
  reghdfe quit cum_cancellations cum_noshows demand tmpc relh pm25 dv_rain, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins zonecode) cluster(driver_cd)
} 
**************************************
* main remaining_mins
else if `regid' == 61 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours demand, absorb(driver_cd date houroffday) cluster(driver_cd)
}
else if `regid' == 62 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins) cluster(driver_cd)
}
else if `regid' == 63 {
  reghdfe remaining_mins dv_cancellation dv_noshow demand, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins) cluster(driver_cd)
}
else if `regid' == 64 {
  reghdfe remaining_mins dv_cancellation dv_noshow demand tmpc relh pm25 dv_rain, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins zonecode) cluster(driver_cd)
} 
**************************************
* main remaining_wage
else if `regid' == 71 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours demand if remaining_mins > 60, absorb(driver_cd date houroffday) cluster(driver_cd)
}
else if `regid' == 72 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date houroffday) cluster(driver_cd)
}
else if `regid' == 73 {
  reghdfe remaining_wage dv_cancellation dv_noshow demand if remaining_mins > 60, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins) cluster(driver_cd)
}
else if `regid' == 74 {
  reghdfe remaining_wage dv_cancellation dv_noshow demand tmpc relh pm25 dv_rain if remaining_mins > 60, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins zonecode) cluster(driver_cd)
}
**************************************
* main remaining_idle_pct
else if `regid' == 81 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours demand if remaining_mins > 60, absorb(driver_cd date houroffday) cluster(driver_cd)
}
else if `regid' == 82 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd date houroffday) cluster(driver_cd)
}
else if `regid' == 83 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow demand if remaining_mins > 60, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins) cluster(driver_cd)
}
else if `regid' == 84 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow demand tmpc relh pm25 dv_rain if remaining_mins > 60, absorb(driver_cd date houroffday cum_hours_bins cum_income_bins zonecode) cluster(driver_cd)
}
**************************************
* rerun main remaining_mins
else if `regid' == 120 {
  reghdfe remaining_mins dv_cancellation dv_noshow, cluster(driver_cd) noabsorb
} 
else if `regid' == 121 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100, absorb(driver_cd date hour#dow) cluster(driver_cd)
} 
else if `regid' == 122 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100, absorb(driver_cd date hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 123 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 124 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 125 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 cum_completed_bookings, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 126 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 127 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m cum_completed_bookings distance_to_pickup oncall_mins tmpc relh pm25 dv_rain, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 110 {
  reghdfe quit dv_cancellation dv_noshow, cluster(driver_cd) noabsorb
} 
else if `regid' == 111 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100, absorb(driver_cd date hour#dow) cluster(driver_cd)
} 
else if `regid' == 112 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100, absorb(driver_cd date hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 113 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 114 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 115 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 cum_completed_bookings, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 116 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 117 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m cum_completed_bookings distance_to_pickup oncall_mins tmpc relh pm25 dv_rain, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 991 {
  reghdfe quit c.(dv_cancellation dv_noshow)##c.(cum_hours cum_income_100) demand, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 992 {
  gen byte cum_hours_interval = 1 if cum_hours <= 4
  replace cum_hours_interval = 2 if cum_hours > 4 & cum_hours <= 6
  replace cum_hours_interval = 3 if cum_hours > 6 & cum_hours <= 8
  replace cum_hours_interval = 4 if cum_hours > 8 & cum_hours <= 10
  replace cum_hours_interval = 5 if cum_hours > 10 & cum_hours <= 12
  replace cum_hours_interval = 6 if cum_hours > 12

  gen byte cum_income_interval = 1 if cum_income <= 100
  replace cum_income_interval = 2 if cum_income > 100 & cum_income <= 150
  replace cum_income_interval = 3 if cum_income > 150 & cum_income <= 200
  replace cum_income_interval = 4 if cum_income > 200 & cum_income <= 250
  replace cum_income_interval = 5 if cum_income > 250 & cum_income <= 300
  replace cum_income_interval = 6 if cum_income > 300

  reghdfe quit dv_cancellation dv_noshow c.(dv_cancellation dv_noshow)#i.(cum_hours_interval cum_income_interval) cum_hours cum_income_100 demand, absorb(driver_cd date hour#dow#zonecode cum_hours_interval cum_income_interval) cluster(driver_cd)  
}
else if `regid' == 993 {
  gen byte cum_hours_interval = 1 if cum_hours <= 4
  replace cum_hours_interval = 2 if cum_hours > 4 & cum_hours <= 8
  replace cum_hours_interval = 3 if cum_hours > 8 & cum_hours <= 12
  replace cum_hours_interval = 4 if cum_hours > 12

  label define hourintlbl 1 "hours<=4" 2 "hours=4-8" 3 "hours=8-12" 4 "hours>12"
  label values cum_hours_interval hourintlbl


  gen byte cum_income_interval = 1 if cum_income <= 100
  replace cum_income_interval = 2 if cum_income > 100 & cum_income <= 200
  replace cum_income_interval = 3 if cum_income > 200 & cum_income <= 300
  replace cum_income_interval = 4 if cum_income > 300

  label define incomeintlbl 1 "income<100" 2 "income=1000-200" 3 "income=200-300" 4 "income>300"
  label values cum_income_interval incomeintlbl

  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.(cum_hours cum_income_100) c.(dv_cancellation dv_noshow)#i.(cum_hours_interval cum_income_interval) , absorb(driver_cd date hour#dow#zonecode cum_hours_interval cum_income_interval) cluster(driver_cd)  
}
else if `regid' == 994 {
  gen byte cum_hours_interval = 1 if cum_hours <= 4
  replace cum_hours_interval = 2 if cum_hours > 4 & cum_hours <= 8
  replace cum_hours_interval = 3 if cum_hours > 8

  label define hourintlbl 1 "hours<=4" 2 "hours=4-8" 3 "hours>8"
  label values cum_hours_interval hourintlbl


  gen byte cum_income_interval = 1 if cum_income <= 100
  replace cum_income_interval = 2 if cum_income > 100 & cum_income <= 200
  replace cum_income_interval = 3 if cum_income > 200

  label define incomeintlbl 1 "income<100" 2 "income=1000-200" 3 "income>200"
  label values cum_income_interval incomeintlbl

  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#i.cum_hours_interval c.(dv_cancellation dv_noshow)#i.cum_income_interval c.(dv_cancellation dv_noshow)#i.cum_hours_interval#i.cum_income_interval, absorb(driver_cd date hour#dow#zonecode cum_hours_interval cum_income_interval) cluster(driver_cd)  
}
else if `regid' == 995 {
  gen cum_hours_2 = cum_hours^2
  gen cum_income_100_2 = cum_income_100^2
  gen cum_hours_3 = cum_hours^3
  gen cum_income_100_3 = cum_income_100^3

  gen hour_income = cum_hours*cum_income_100
  gen hour2_income = cum_hours_2*cum_income_100
  gen hour_income2 = cum_hours*cum_income_100_2

  reghdfe quit demand c.(dv_cancellation dv_noshow)##c.(cum_hours cum_hours_2 cum_hours_3 cum_income_100 cum_income_100_2 cum_income_100_3 hour_income hour_income2 hour2_income), absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 996 {
  gen cum_hours_2 = cum_hours^2
  gen cum_hours_3 = cum_hours^3
  gen cum_hours_4 = cum_hours^4
  gen cum_income_100_2 = cum_income_100^2
  gen cum_income_100_3 = cum_income_100^3
  gen cum_income_100_4 = cum_income_100^4

  reghdfe quit demand c.(dv_cancellation dv_noshow)##c.(cum_hours cum_hours_2 cum_hours_3 cum_hours_4 cum_income_100 cum_income_100_2 cum_income_100_3 cum_income_100_4), absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
* quit without date 
else if `regid' == 711 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd) cluster(driver_cd)
} 
else if `regid' == 712 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd hour#dow) cluster(driver_cd)
}
else if `regid' == 713 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 714 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 715 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 716 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 717 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 718 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins dv_completed_booking, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* remaining_mins
else if `regid' == 721 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd ) cluster(driver_cd)
} 
else if `regid' == 722 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd  hour#dow) cluster(driver_cd)
}
else if `regid' == 723 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd  hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 724 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 725 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 726 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 727 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 728 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* remaining_wage
else if `regid' == 731 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd ) cluster(driver_cd)
} 
else if `regid' == 732 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd  hour#dow) cluster(driver_cd)
}
else if `regid' == 733 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd  hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 734 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 735 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 736 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 737 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 738 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)  
}
**************************************
* remaining_idle_pct
else if `regid' == 741 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd ) cluster(driver_cd)
} 
else if `regid' == 742 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd  hour#dow) cluster(driver_cd)
}
else if `regid' == 743 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd  hour#dow ref_postcode) cluster(driver_cd)
}
else if `regid' == 744 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 745 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 746 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand nearby_500m if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 747 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)
}
else if `regid' == 748 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd  hour#dow#zonecode) cluster(driver_cd)  
}
*****************************************
* interacting with demand
else if `regid' == 914 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 918 {
  reghdfe quit dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 924 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 928 {
  reghdfe remaining_mins dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins, absorb(driver_cd date hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 934 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand if remaining_mins > 60, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 938 {
  reghdfe remaining_wage dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 944 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand  if remaining_mins > 60, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)  
}
else if `regid' == 948 {
  reghdfe remaining_idle_pct dv_cancellation dv_noshow cum_hours cum_income_100 demand c.(dv_cancellation dv_noshow)#c.demand cum_completed_bookings nearby_500m distance_to_pickup oncall_mins if remaining_mins > 60, absorb(driver_cd hour#dow#zonecode) cluster(driver_cd)  
}


estadd_hdfe
est store `prefix'`regid'

cap mkdir "`outputfolder'"
estimate save "`outputfolder'/`prefix'`regid'", replace
