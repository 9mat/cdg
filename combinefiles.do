args folder files output

set more off

local filelist: dir "`folder'" files "`files'", respectcase

clear
foreach filename in `filelist' {
  append using "`folder'/`filename'"
}

saveold "`output'", replace
