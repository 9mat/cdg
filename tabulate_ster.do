set more off
global sterdir "./ster"
local filelist: dir "$sterdir" files "eq*.ster", respectcase

foreach filename in `filelist' {
  estimates use "$sterdir/`filename'"
  local eqname=subinstr("`filename'", ".ster", "", .)
  estimates store `eqname'
}

cap mkdir ./tb

local tb1 quit
local tb2 mins
local tb3 wage
local tb4 idle
local tb5 quit
local tb6 mins
local tb7 idle
local tb8 wage
local tb71 quit
local tb72 mins
local tb73 wage
local tb74 idle

// cap rm ./tb/eq.rtf

// #delim ;

// estfe eq18 eq28 eq38 eq48, labels(
//   driver_cd "Driver FE" 
//   hour#dow "Hour*DOW FE" 
//   date "Date FE"
//   ref_postcode "Postal code FE" 
//   hour#dow#zonecode "Hour*DOW*Zone FE")
// ;

// esttab eq18 eq28 eq38 eq48 using ./tb/eqsum.rtf,
//   append
//   label se star(* 0.1 ** 0.05 *** 0.01) nonotes noconstant
//   b(3) se(3)
//   indicate(`r(indicate_fe)', label("{Yes}" "{-}"))
//   coeflabels(
//     dv_cancellation "Cancellation (dummy)"
//     dv_noshow "No-show (dummy)"
//     cum_hours "Cumulative hours"
//     cum_income_100 "Cumulative income ('00 SGD)"
//     demand "Demand density"
//     cum_completed_bookings "Previous bookings (count)"
//     nearby_50m "Vehicles within 50m ('000)"
//     nearby_500m "Vehicles within 500m ('000)"
//     dv_completed_booking "Completed Booking (dummy)"
//     distance_to_pickup "Distance to pickup (km)"
//     oncall_mins "Oncall duration (mins)")
//   mtitles("Stopping work" "Remaining time (mins)" "Remaining wage (SGD)" "Remaining idleness (%)")
//   stats(N r2,
//     label(Observations Rsquare)
//     fmt(0 3))
//   ;

// estfe eq18 eq28 eq38 eq48, restore;

// #delim cr

// exit

forvalues i=71/74 {

#delim ;

estfe eq`i'?, labels(
  driver_cd "Driver FE" 
  hour#dow "Hour*DOW FE" 
  ref_postcode "Postal code FE" 
  hour#dow#zonecode "Hour*DOW*Zone FE")
;

esttab eq`i'1 eq`i'2 eq`i'3 eq`i'4 eq`i'5 eq`i'6 eq`i'7 eq`i'8 using ./tb/eq.rtf,
  append
  label se star(* 0.1 ** 0.05 *** 0.01) nomtitle nonotes noconstant
  b(3) se(3)
  indicate(`r(indicate_fe)', label("{Yes}" "{-}"))
  coeflabels(
    dv_cancellation "Cancellation (dummy)"
    dv_noshow "No-show (dummy)"
    cum_hours "Cumulative hours"
    cum_income_100 "Cumulative income ('00 SGD)"
    demand "Demand density"
    cum_completed_bookings "Previous bookings (count)"
    nearby_50m "Vehicles within 50m ('000)"
    nearby_500m "Vehicles within 500m ('000)"
    dv_completed_booking "Completed Booking (dummy)"
    distance_to_pickup "Distance to pickup (km)"
    oncall_mins "Oncall duration (mins)")
  stats(N r2,
    label(Observations Rsquare)
    fmt(0 3))
  ;

estfe eq`i'?, restore;

estfe eq`i'?, labels(
  driver_cd "Driver FE" 
  hour#dow "Hour\(\times\) DOW FE" 
  ref_postcode "Postal code FE" 
  hour#dow#zonecode "Hour\(\times\)DOW\(\times\)Zone FE")
;

esttab eq`i'1 eq`i'2 eq`i'3 eq`i'4 eq`i'5 eq`i'6 eq`i'7 eq`i'8 using ./tb/eq`i'`tb`i''.tex,
  replace
  label se star(* 0.1 ** 0.05 *** 0.01) nomtitle nonotes noconstant
  b(3) se(3)
  indicate(`r(indicate_fe)', label("{Yes}" "{-}"))
  coeflabels(
    dv_cancellation "Cancellation (dummy)"
    dv_noshow "No-show (dummy)"
    cum_hours "Cumulative hours"
    cum_income_100 "Cumulative income ('00 SGD)"
    demand "Demand density"
    cum_completed_bookings "Previous bookings (count)"
    nearby_50m "Vehicles within 50m ('000)"
    nearby_500m "Vehicles within 500m ('000)"
    dv_completed_booking "Completed Booking (dummy)"
    distance_to_pickup "Distance to pickup (km)"
    oncall_mins "Oncall duration (mins)")
  stats(N r2,
    label(Observations Rsquare)
    layout(" \num{@}" {@})
    fmt(0 3))
  booktabs
  align(S)
  ;

#delim cr

}

exit

forvalues i=5/8 {

#delim ;

estfe eq`i'*, labels(
  driver_cd "Driver FE" 
  date "Date FE" 
  houroffday "Hour*Workday FE"
  zonecode "Zone FE"
  cum_hours_bins "Cumulative hours FE"
  cum_income_bins "Cumulative income FE")
;

esttab eq`i'1 eq`i'2 eq`i'3 eq`i'4 using ./tb/eqmain.rtf,
  append
  label se star(* 0.1 ** 0.05 *** 0.01) nomtitle nonotes noconstant
  b(4) se(4)
  indicate(`r(indicate_fe)', label("{Yes}" "{-}"))
  coeflabels(
    cum_cancellations "Cumulative cancellations"
    cum_noshows "Cumulative no-shows"
    dv_cancellation "Cancellation (dummy)"
    dv_noshow "No-show (dummy)"
    cum_hours "Cumulative hours"
    cum_income_100 "Cumulative income ('00 SGD)"
    demand "Demand density"
    tmpc "Temperature"
    relh "Relative humidity"
    pm25 "PM 2.5"
    dv_rain "Rainfall > 0.2mm")
  stats(N r2,
    label(Observations Rsquare)
    fmt(0 3))
  ;

estfe eq`i'*, restore;

estfe eq`i'*, labels(
  driver_cd "Driver FE" 
  date "Date FE" 
  houroffday "Hour\(\times\)Workday FE"
  zonecode "Zone FE"
  cum_hours_bins "Cumulative hours FE"
  cum_income_bins "Cumulative income FE")
;

esttab eq`i'1 eq`i'2 eq`i'3 eq`i'4 using ./tb/eq`i'`tb`i''.tex,
  replace
  label se star(* 0.1 ** 0.05 *** 0.01) nomtitle nonotes noconstant
  b(4) se(4)
  indicate(`r(indicate_fe)', label("{Yes}" "{-}"))
  coeflabels(
    cum_cancellations "Cumulative cancellations"
    cum_noshows "Cumulative no-shows"
    dv_cancellation "Cancellation (dummy)"
    dv_noshow "No-show (dummy)"
    cum_hours "Cumulative hours"
    cum_income_100 "Cumulative income ('00 SGD)"
    demand "Demand density"
    tmpc "Temperature (\textdegree C)"
    relh "Relative humidity (\%)"
    pm25 "PM 2.5"
    dv_rain "Rainfall \textgreater 0.2mm")
  stats(N r2,
    label(Observations Rsquare)
    layout(" \num{@}" {@})
    fmt(0 3))
  booktabs
  align(S)
  ;

#delim cr

}
