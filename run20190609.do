args inputfile outputfolder prefix id


set more off

local y quit
local x dv_cancellation dv_noshow

local control1 cum_hours
local fe1 driver_cd


local control2 cum_hours
local fe2 driver_cd date hour#dow


local control3 cum_hours
local fe3 driver_cd date hour#dow postcode

local control4 cum_hours
local fe4 driver_cd date hour#dow#zonecode

local control5 cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking
local fe5 driver_cd date hour#dow#zonecode

local control6 `control5' cum_hours_sqr cum_hours_cub
local fe6 driver_cd date hour#dow#zonecode

local control7 `control6' cum_income_100 cum_income_100_sqr cum_income_100_cub
local fe7 driver_cd date hour#dow#zonecode


use `y' `x' remaining_wage remaining_mins driver_cd hour date postcode zonecode cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings job_status cum_income total_trip_fare shift_num using $inputfile, clear

// gen dv_booking = job_status != .
// gen dow = dow(date)
// replace dow = 8 if inlist(date, ///
//   td(25dec2016), /// Christmas
//   td(26dec2016), /// in lieu of Christmas
//   td(1jan2017), /// New Year
//   td(2jan2017), /// in lieu of New Year
//   td(28jan2017), /// CNY 1
//   td(29jan2017), /// CNY 2
//   td(30jan2017), /// in lieu of CNY 2
//   td(14apr2017), /// good Friday
//   td(10may2017), /// Vesak
//   td(25jun2017), /// Hari Raya Puasa
//   td(26jun2017), /// in lieu of Hari Raya Puasa
//   td(9aug2017), /// National Day
//   td(1sep2017), /// Hari Raya Haji
//   td(18oct2017), /// Deepavali
//   td(25dec2017), /// Christmas
//   td(1jan2018), /// New year
//   td(16feb2018), /// CNY 1
//   td(17feb2018), /// CNY 2
//   td(30mar2018) /// Good Friday
// )

// gen cum_hours_sqr = cum_hours^2
// gen cum_hours_cub = cum_hours^3	

// gen cum_income_100 = cum_income/100
// gen cum_income_100_sqr = cum_income_100^2
// gen cum_income_100_cub = cum_income_100^3


// foreach id in 1 2 3 4 5 6 7 {
//   local mcontrol `control`id''
//   local mfe `fe`id''

//   reghdfe remaining_wage `x' `mcontrol' if remaining_mins > 60, absorb(`mfe') cluster(driver_cd)

//   estadd ysumm


//   if `: list posof "cum_hours_cub" in mcontrol' > 0 {
//     lincom cum_hours + 16*cum_hours_sqr + 192*cum_hours_cub
//   }
//   else {
//     lincom cum_hours
//   }

//   estadd scalar mxhour8 = `r(estimate)'
//   estadd scalar mxhour8se = `r(se)'


//   if `: list posof "cum_income_100" in mcontrol' > 0 {
//     if `: list posof "cum_income_100_cub" in mcontrol' > 0 {
//       lincom cum_income_100 + 4*cum_income_100_sqr + 12*cum_income_100_cub
//     }
//     else  {
//       lincom cum_income_100
//     }

//     estadd scalar mxincome200 = `r(estimate)'
//     estadd scalar mxincome200se = `r(se)'
//   }
//   else {
//     estadd scalar mxincome200 = .
//     estadd scalar mxincome200se = .
//   }

//   est store `prefix'wage`id'
//   cap mkdir "`outputfolder'"
//   estimate save "`outputfolder'/`prefix'wage`id'", replace  

// }


// exit

foreach id in 1 2 3 4 5 6 7 {
  local mcontrol `control`id''
  local mfe `fe`id''

  reghdfe `y' `x' `mcontrol', absorb(`mfe') cluster(driver_cd)



  estadd ysumm


  if `: list posof "cum_hours_cub" in mcontrol' > 0 {
    lincom cum_hours + 16*cum_hours_sqr + 192*cum_hours_cub
  }
  else {
    lincom cum_hours
  }

  estadd scalar mxhour8 = `r(estimate)'
  estadd scalar mxhour8se = `r(se)'


  if `: list posof "cum_income_100" in mcontrol' > 0 {
    if `: list posof "cum_income_100_cub" in mcontrol' > 0 {
      lincom cum_income_100 + 4*cum_income_100_sqr + 12*cum_income_100_cub
    }
    else  {
      lincom cum_income_100
    }

    estadd scalar mxincome200 = `r(estimate)'
    estadd scalar mxincome200se = `r(se)'
  }
  else {
    estadd scalar mxincome200 = .
    estadd scalar mxincome200se = .
  }

  est store `prefix'quit`id'
  cap mkdir "`outputfolder'"
  estimate save "`outputfolder'/`prefix'quit`id'", replace  

}




forvalues h = 6/10 {
  reghdfe `y' `x' `control5' if cum_hours >= `h'-1 & cum_hours <`h' & , absorb(`fe6') cluster(driver_cd)
  estadd ysumm

  est store `prefix'quith`h'
  cap mkdir "`outputfolder'"
  estimate save "`outputfolder'/`prefix'quith`h'", replace  
}


gen farebeforeh5 = total_trip_fare if cum_hours < 5
bys driver_cd shift_num: egen earningsbeforeh5 = total(farebeforeh5)
drop farebeforeh5


forvalues h = 6/9 {
  cap gen fareh`h' = total_trip_fare if cum_hours >= `h'-1 & cum_hours < `h'
  bys driver_cd shift_num: egen earningsh`h' = total(fareh`h')
  drop fareh`h'
}


preserve
  keep if cum_hours > 9 & cum_hours <= 10

  gen earningsh10 = cum_income - earningsbeforeh5 - earningsh6 - earningsh7 - earningsh8 - earningsh9

  foreach v of varlist earnings* {
    gen `v'_100 = `v'/100
  }

  reghdfe `y' `x' `control5' earningsbeforeh5_100 earningsh6_100 earningsh7_100 earningsh8_100 earningsh9_100 earningsh10_100, absorb(`fe6') cluster(driver_cd)
  estadd ysumm

  est store `prefix'quith10thakral
  cap mkdir "`outputfolder'"
  estimate save "`outputfolder'/`prefix'quith10thakral", replace  
restore






// tabulate





use id ref_start_dt ref_end_dt cum_hours total_trip_fare shift_num  driver_cd using $inputfile, clear
bys driver_cd shift_num (ref_end_dt): gen order = _n


fegen shift_id = group(driver_cd shift_num)

xtset shift_id order

compress

cap drop earningsin1h
cap drop future_fare future_start_dt future_end_dt cutoff_start_dt cutoff_end_dt

gen earningsin1h = 0

forvalues i=1/15 {
  di "Iteration `i'"
  gen double future_start_dt = F`i'.ref_start_dt
  count if future_start_dt < ref_end_dt+60*60*1000
  if `r(N)' == 0 {
    continue, break
    drop future_start_dt
  }
  gen double future_end_dt = F`i'.ref_end_dt
  gen future_fare = F`i'.total_trip_fare if future_start_dt < ref_end_dt + 60*60*1000

  gen double cutoff_start_dt = min(future_start_dt, ref_end_dt + 60*60*1000)
  gen double cutoff_end_dt = min(future_end_dt, ref_end_dt + 60*60*1000)

  replace earningsin1h = earningsin1h + future_fare*(cutoff_end_dt-cutoff_start_dt)/(future_end_dt-future_start_dt+1) if !inlist(., future_fare, future_start_dt, future_end_dt)
  drop future_fare future_start_dt future_end_dt cutoff_start_dt cutoff_end_dt
}



gen earningsin30min = 0

forvalues i=1/15 {
  di "Iteration `i'"
  gen double future_start_dt = F`i'.ref_start_dt
  count if future_start_dt < ref_end_dt+30*60*1000
  if `r(N)' == 0 {
    continue, break
    drop future_start_dt
  }
  gen double future_end_dt = F`i'.ref_end_dt
  gen future_fare = F`i'.total_trip_fare if future_start_dt < ref_end_dt + 30*60*1000

  gen double cutoff_start_dt = min(future_start_dt, ref_end_dt + 30*60*1000)
  gen double cutoff_end_dt = min(future_end_dt, ref_end_dt + 30*60*1000)

  replace earningsin30min = earningsin30min + future_fare*(cutoff_end_dt-cutoff_start_dt)/(future_end_dt-future_start_dt+1) if !inlist(., future_fare, future_start_dt, future_end_dt)
  drop future_fare future_start_dt future_end_dt cutoff_start_dt cutoff_end_dt
}


keep id earningsin1h earningsin30min
save F:/Temp/futureearnings, replace



use id quit dv_cancellation dv_noshow remaining_wage remaining_mins driver_cd hour date postcode zonecode cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings job_status cum_income total_trip_fare shift_num using $inputfile, clear


merge 1:1 id using F:/Temp/futureearnings, nogen keep(match master)

gen cum_hours_sqr = cum_hours^2

gen dv_booking = job_status != .




reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 7 & cum_hours < 10 & cum_income < 100 & cum_income > 50, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
estadd ysumm

estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/quitincome1.ster, replace
eststo quitincome1

reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 7 & cum_hours < 10 & cum_income < 150 & cum_income > 100, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)

estadd ysumm

estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/quitincome2.ster, replace
eststo quitincome2


reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 7 & cum_hours < 10 & cum_income < 200 & cum_income > 150, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
estadd ysumm

estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/quitincome3.ster, replace
eststo quitincome3

// reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 8 & cum_hours < 9 & cum_income < 250 & cum_income > 200, absorb(driver_cd hour#dow#zonecode date)


reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 7 & cum_hours < 10 & cum_income < 300 & cum_income > 250, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
estadd ysumm

estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/quitincome4.ster, replace
eststo quitincome4

reghdfe quit dv_cancellation dv_noshow cum_hours cum_income demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 6 & cum_hours < 9 &  cum_income > 300 & cum_income < 350, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
estadd ysumm

estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/quitincome5.ster, replace
eststo quitincome5




gen cum_income_100 = cum_income/100
gen cum_income_100_sqr = cum_income_100^2
reghdfe quit c.(dv_cancellation dv_noshow)##c.(cum_income_100 cum_income_100_sqr cum_hours) demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours > 8 & cum_hours < 9, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
estadd ysumm
eststo quitincome6

esttab quitincome1 quitincome2 quitincome3 quitincome4 quitincome5, stat(ymean N r2) se keep(dv_cancellation dv_noshow)



reghdfe quit c.(dv_cancellation dv_noshow)##c.(cum_income_100 cum_income_100_sqr cum_hours) cum_completed_bookings dv_booking if cum_hours > 8 & cum_hours < 9, absorb(driver_cd hour#dow#zonecode date)

reghdfe quit c.(dv_cancellation dv_noshow)##c.(cum_income_100 cum_income_100_sqr cum_hours) if cum_hours > 6 & cum_hours < 7,absorb(driver_cd hour#dow)





use quit dv_cancellation dv_noshow shift_start_dt driver_cd hour date postcode zonecode cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings job_status cum_income total_trip_fare shift_num using $inputfile, clear

gen start_hour = hh(shift_start_dt)
gen dv_booking = job_status != .
gen dow = dow(date)
replace dow = 8 if inlist(date, ///
  td(25dec2016), /// Christmas
  td(26dec2016), /// in lieu of Christmas
  td(1jan2017), /// New Year
  td(2jan2017), /// in lieu of New Year
  td(28jan2017), /// CNY 1
  td(29jan2017), /// CNY 2
  td(30jan2017), /// in lieu of CNY 2
  td(14apr2017), /// good Friday
  td(10may2017), /// Vesak
  td(25jun2017), /// Hari Raya Puasa
  td(26jun2017), /// in lieu of Hari Raya Puasa
  td(9aug2017), /// National Day
  td(1sep2017), /// Hari Raya Haji
  td(18oct2017), /// Deepavali
  td(25dec2017), /// Christmas
  td(1jan2018), /// New year
  td(16feb2018), /// CNY 1
  td(17feb2018), /// CNY 2
  td(30mar2018) /// Good Friday
)

forvalues h = 6/10 {
  reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours >= `h'-1 & cum_hours <`h' & start_hour > 4 & start_hour < 12, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
  estadd ysumm

  est store eq20190609quith`h'dayshift
  estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/eq20190609quith`h'dayshift, replace  
}


forvalues h = 6/10 {
  reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours >= `h'-1 & cum_hours <`h' & start_hour > 15, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
  estadd ysumm

  est store eq20190609quith`h'nightshift
  estimate save D:/Dropbox/work/cdg/cdg/src/ster/run20190609/eq20190609quith`h'nightshift, replace  
}



reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours >= 6 & cum_hours <10 & start_hour > 15, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)

reghdfe quit dv_cancellation dv_noshow cum_hours demand nearby_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking if cum_hours >= 6 & cum_hours <10 & start_hour > 4 & start_hour < 12, absorb(driver_cd hour#dow#zonecode date) cluster(driver_cd)
