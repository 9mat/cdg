set more off

include definepath.do
global today : display %tdCYND date(c(current_date), "DMY")



global X1 dv_cancellation dv_noshow
global X2 $X1 dv_placebotrip
global X3 $X1 dv_placebotrip2



global demandcontrols demand nearby_500m bookings500m5min bookings1000m5min streethails500m5min streethails1000m5min
global fatigecontrols cum_work_mins cum_breakmins
global oncallcontrols oncall_mins oncall_km 
global weathercontrols temperature humidity wind_speed rainmm rainmins pm25
global fatigecontrols2 cum_work_mins cum_breakmins breakmins_last60m pobmins_last60m pobkm_last60m
global incomecontrols cum_income cum_income_sqr cum_income_cub



global controls1 cum_hours cum_hours_sqr cum_hours_cub
global fe1 driver_cd

global controls2 $controls1 
global fe2 $fe1 date

global controls3 $controls1
global fe3 $fe2 hour#dow

global controls4 $controls1
global fe4 $fe3 zonecode

global controls5 $controls1
global fe5 $fe3 ref_postcode



global controls6 $controls1
global fe6 $fe2 hour#dow#zonecode

global controls7 $controls6 $weathercontrols
global fe7 $fe6

global controls8 $controls7 $demandcontrols
global fe8 $fe6

global controls9 $controls8 $fatigecontrols2
global fe9 $fe6

global controls10 $controls9 $incomecontrols
global fe10 $fe6



global controls11 $controls1
global fe11 $fe2 hour#dow#ref_postcode

global controls12 $controls11 $weathercontrols
global fe12 $fe11

global controls13 $controls12 $demandcontrols
global fe13 $fe11

global controls14 $controls13 $fatigecontrols2
global fe14 $fe11

global controls15 $controls14 $incomecontrols
global fe15 $fe11





global subsample0 job_status != .
global subsample1 job_status != . & freegap_mins < 2 & free_mins > 3 
global subsample2 job_status != . & freegap_mins < 2 & free_mins > 3 & cum_hours > 2
global subsample3 job_status != . & freegap_mins < 2 & free_mins > 3 & cum_hours > 1
global subsample4 1
global subsample5 freegap_mins < 2 & free_mins > 3 
global subsample6 freegap_mins < 2 & free_mins > 3 & cum_hours > 2
global subsample7 freegap_mins < 2 & free_mins > 3 & cum_hours > 1


args i

log using $sterdir/reglog`i'_$today.smcl, replace smcl name(reg`i') 


global scratchfile /scratch/trips_merged_dec2feb_20190129_2.dta

cap confirm file "$scratchfile"

if _rc==0{
  use "$scratchfile", clear
}
else {
  use $trip201912, clear
}


cap gen holiday = inlist(date, ///
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

cap drop dow
gen dow = dow(dofc(ref_end_dt))
replace dow = 8 if holiday

cap gen cum_hours_sqr = cum_hours^2
cap gen cum_hours_cub = cum_hours^3

cap gen cum_income_sqr = cum_income^2
cap gen cum_income_cub = cum_income^3


forvalues s=0/7 {
  forvalues j=1/3 {
    reghdfe quit ${X`j'} ${controls`i'} if ${subsample`s'}, absorb(${fe`i'}) cluster(date driver_cd)
    estadd ysumm
    eststo quit`i'X`j's`s'
    estimate save $sterdir/run20191222/quit`i'X`j's`s', replace

    reghdfe earnings_next60m ${X`j'} ${controls`i'} if ${subsample`s'} & remaining_mins > 60, absorb(${fe`i'}) cluster(date driver_cd)
    estadd ysumm
    eststo earn`i'X`j's`s'
    estimate save $sterdir/run20191222/earn`i'X`j's`s', replace
  }  
}


log close reg`i'
