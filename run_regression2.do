args inputfile outputfolder prefix regid hour incomelb incomeub xid feid

set more off

local nonlinear_income cum_income cum_income_sqr cum_income_cub

local controls0 cum_hours
local controls1 `controls0' cum_hours_sqr cum_hours_cub
local controls2 `controls1' demand nearby_500m
local controls3 `controls2' tmpc relh pm25 dv_rain
local controls4 `controls3' cum_completed_bookings dv_booking
local controls5 `controls4' oncall_mins distance_to_pickup
local controls6 `controls1' `nonlinear_income'
local controls7 `controls2' `nonlinear_income'
local controls8 `controls3' `nonlinear_income'
local controls9 `controls4' `nonlinear_income'
local controls10 `controls5' `nonlinear_income'


local y1 quit
local y2 remaining_mins
local y3 remaining_wage
local y4 remaining_idle_pct

local cond1 " "
local cond2 " "
local cond3 " if remaining_mins > 60 "
local cond4 "`cond3'"


local feset "driver_cd hour#dow#zonecode date"
local feset1 "driver_cd date"
local feset2 "driver_cd hour#dow date"
local feset3 "driver_cd hour#dow#zonecode date"

local fe `feset`feid''



local fevars driver_cd hour dow zonecode date

if "`xid'" != "none" {
  local x dv_cancellation dv_noshow
}
else {
  local x ""
}



local j `=mod(`regid',10)'
local i `=(`regid'-`j')/10'

local mcontrols `controls`i''
local my `y`j''
local mcond `cond`j''


// variables that need generating
local vars_to_generate cum_hours_sqr cum_hours_cub remaining_idle_pct dv_booking dow cum_income_sqr cum_income_cub

// variables needed for regression
local vars_to_reg `my' `fevars' `x' id `mcontrols' job_status cum_income
if `j'==4 {
  local vars_to_reg `vars_to_reg' working_hours shift_num
}
if `j'==3 | `j'==4 {
  local vars_to_reg `vars_to_reg' remaining_mins
}


// variables to be read from the data
local vars_to_use: list vars_to_reg - vars_to_generate



// load the data
use `vars_to_use' using "`inputfile'", clear



// generate varibales for regression that are not in the data yet

if `: list posof "remaining_idle_pct" in vars_to_reg' > 0 {
  bys driver_cd shift_num: egen shift_working_hours = total(working_hours)
  gen remaining_working_mins = shift_working_hours*60 - remaining_mins
  gen remaining_idle_pct = 100 - 100*remaining_working_mins/remaining_mins
  drop shift_num shift_working_hours remaining_working_mins
}

if `: list posof "cum_hours_sqr" in vars_to_reg' > 0 {
  cap gen cum_hours_sqr = cum_hours^2
}

if `: list posof "cum_hours_cub" in vars_to_reg' > 0 {
  cap gen cum_hours_cub = cum_hours^3
}

if `: list posof "cum_income" in vars_to_reg' > 0 {
  replace cum_income = cum_income/100
}

if `: list posof "cum_income_sqr" in vars_to_reg' > 0 {
  cap gen cum_income_sqr = cum_income^2
}

if `: list posof "cum_income_cub" in vars_to_reg' > 0 {
  cap gen cum_income_cub = cum_income^3
}


cap gen dow = dow(date)
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

if `: list posof "dv_booking" in vars_to_reg' > 0 {
  cap gen dv_booking = job_status != .
}


local postfix ""

if "`hour'" != "" & "`hour'" != "all" {
  keep if cum_hours >= `hour'-1 & cum_hours < `hour'
  local postfix "`postfix'h`hour'"
}

if "`incomelb'" != "" & "`incomeub'" != "" & "`incomelb'" != "-" & "`incomeub'" != "-" {
  keep if cum_income >= `incomelb'/100 & cum_income < `incomeub'/100
  local postfix "`postfix'il`incomelb'iu`incomeub'"
}

if "`xid'" != "normal" & "`xid'" != "" {
  local postfix "`postfix'x`nox'"
}

if "`feid'" != "" {
  local postfix "`postfix'fe`feid'"
}


if `i' > 60 {
  reghdfe `my' `x' `mcontrols' if cum_hours >= 9 & cum_hours < 10, absorb(`fe') cluster(driver_cd) timeit
}

if `i' > 50 {
  forvalues h=1/9 {
    gen fare_hour`h' = total_trip_fare if cum_hours <= `h' & cum_hours > `h'-1
    bys driver_cd shift_num: egen income_hour`h' = total(fare_hour`h')
    drop fare_hour`h'
  }
  
  gen income_before5 = income_hour1 + income_hour2 + income_hour3 + income_hour4
  drop income_hour1-income_hour4


  reghdfe `my' `x' `mcontrols' income_before5 income_hour5-income_hour9 if cum_hours >= 9 & cum_hours < 10, absorb(`fe') cluster(driver_cd) timeit
}



if `i' < 50 {
  reghdfe `my' `x' `mcontrols' `mcond', absorb(`fe') cluster(driver_cd) timeit
}


estadd ysumm


if `: list posof "cum_hours_cub" in vars_to_reg' > 0 {
  lincom cum_hours + 16*cum_hours_sqr + 192*cum_hours_cub
}
else {
  lincom cum_hours
}

estadd scalar mxhour8 = `r(estimate)'
estadd scalar mxhour8se = `r(se)'


if `: list posof "cum_income" in mcontrols' > 0 {
  if `: list posof "cum_income_cub" in vars_to_reg' > 0 {
    lincom cum_income + 4*cum_income_sqr + 12*cum_income_cub
  }
  else  {
    lincom cum_income
  }

  estadd scalar mxincome200 = `r(estimate)'
  estadd scalar mxincome200se = `r(se)'
}
else {
  estadd scalar mxincome200 = .
  estadd scalar mxincome200se = .
}

est store `prefix'`regid'`postfix'
cap mkdir "`outputfolder'"
estimate save "`outputfolder'/`prefix'`regid'`postfix'", replace
