global projectdir /NAS/project01/chujunhong_SPIRE
global homedir /NAS/home01/chujunhong

use $projectdir/trips/trips_merged_dec2feb_everything.dta, clear

preserve
set seed 12345
keep driver_cd
duplicates drop
drop if driver_cd == .
sort driver_cd
sample 1000, count
tempfile random100drivers
save `random100drivers'
restore

merge m:1 driver_cd using `random100drivers', keep(match) nogen

gen month = month(date)
egen hdz = group(hour offday zonecode)

logit quit dv_cancellation dv_noshow cum_hours cum_income_100 demand i.hour i.dow i.month date, cluster(driver_cd)
estimates store logit1
estimates save $homedir/cdg/ster/logit1.ster, replace

clogit quit dv_cancellation dv_noshow cum_hours cum_income_100 demand i.hour i.dow i.month date, group(driver_cd) cluster(driver_cd)
estimates store logit2
estimates save $homedir/cdg/ster/logit2.ster, replace

clogit quit dv_cancellation dv_noshow  cum_hours cum_income_100 demand i.month date, group(hdz) cluster(driver_cd)
estimates store logit3
estimates save $homedir/cdg/ster/logit3.ster, replace



foreach m in logit1 logit2 logit3 {
  estimates use $homedir/cdg/ster/`m'.ster
  estimates store `m'
}

#delim ;

esttab logit1 logit2 logit3,
  label se star(* 0.1 ** 0.05 *** 0.01) nomtitle nonotes
  b(4) se(4)
  indicate("Hour FE=*hour" "Day of week FE=*dow" "Month FE=*month")
  coeflabels(
    dv_cancellation "Cancellation (dummy)"
    dv_noshow "No-show (dummy)"
    cum_hours "Cumulative hours"
    cum_income_100 "Cumulative income ('00 SGD)"
    date "Time trend (days)")
  stats(N r2,
    label(Observations Rsquare)
    fmt(0 3))
  ;



#delim cr
