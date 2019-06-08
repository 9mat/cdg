args inputfile outputfolder prefix id


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

local control5 cum_hours demand nearby_veh_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings dv_booking
local fe5 driver_cd date hour#dow#zonecode

local control6 `control5' cum_hours_sqr cum_hours_cub
local fe6 driver_cd date hour#dow#zonecode

local control7 `control6' cum_income_100 cum_income_100_sqr cum_income_100_cub
local fe7 driver_cd date hour#dow#zonecode

local mcontrol `control`id''
local mfe `fe`id''


use `y' `x' driver_cd hour date postcode zonecode cum_hours demand nearby_veh_500m tmpc relh pm25 dv_rain oncall_mins distance_to_pickup cum_completed_bookings job_status cum_income using $inputfile


gen dv_booking = job_status != .
gen dow = dow(date)

if id >= 5 {
	gen cum_hours_sqr = cum_hours^2
	gen cum_hours_cub = cum_hours^3	
}

if id >= 6 {
	gen cum_income_100 = cum_income/100
	gen cum_income_100_sqr = cum_income_100^2
	gen cum_income_100_cub = cum_income_100^3
}

reghdfe `y' `x' `mcontrol', absorb(`mfe') cluster(driver_cd)


