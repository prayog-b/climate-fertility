* ============================================================================
* HELPER PROGRAMS (defined at top level)
* ============================================================================

* Program to set up timestamp of analysis
capture program drop _print_timestamp
program define _print_timestamp 
    di "{hline `=min(79, c(linesize))'}"
    di "Date and time: $S_DATE $S_TIME"
    di "Stata version: `c(stata_version)'"
    di "Updated as of: `c(born_date)'"
    di "Variant:       `=cond( c(MP),"MP",cond(c(SE),"SE",c(flavor)) )'"
    di "Processors:    `c(processors)'"
    di "OS:            `c(os)' `c(osdtl)'"
    di "Machine type:  `c(machine_type)'"
    local hostname : env HOSTNAME
    if !mi("`hostname'") {
        di "Hostname:      `hostname'"
    }
    di "{hline `=min(79, c(linesize))'}"
end

* Auxiliary function saving input in file called 'input.tex'
capture program drop save_input
program define save_input 
    /*
    description:	saves a number/statistic as a .tex file for use in overleaf
    usage:			save_input `number' "filepath"
    example:		save_input `coef' "number_of_cities"
    */
    args number filename
    file open newfile using "$cf_overleaf/outnum/worker-survey/`filename'.tex", write replace
    file write newfile "`number'%"
    file close newfile
end

* Auxiliary function to install any uninstalled ssc packages before using them
capture program drop verify_package
program verify_package
    /*
    description: 	checks whether a package is installed. if not, installs it from ssc or net.
    usage:			verify_package `package'
    example:		verify_package reghdfe
    */
    args package
    capture which `package'
    if _rc != 0 {
        di "Package `package' not found. Attempting to install..."
        capture ssc install `package'
        if _rc != 0 {
            di "SSC install failed. Trying net install..."
            capture net install `package'.pkg
            if _rc != 0 {
                di as error "Could not install `package'"
            }
            else {
                di "Package `package' installed successfully from net."
            }
        }
        else {
            di "Package `package' installed successfully from SSC."
        }
    }
    else {
        di "Package `package' already installed."
    }
end

* Run Python script using virtual environment
capture program drop run_python
program define run_python
    /*
    description: runs a Python script using the virtual environment
    usage:       run_python "script_path.py"
    example:     run_python "$dm/clean_data.py"
    */
    args script_path
    
    * Check if virtual environment exists
    capture confirm file "$cf/dm/.venv/bin/python3"
    if _rc != 0 {
        display as error "Virtual environment not found at: $dm/.venv"
        display as error "Run: ClimateAnalysisConfig, method(init) setup_python"
        exit 601
    }
    
    * Check if script exists
    capture confirm file "`script_path'"
    if _rc != 0 {
        display as error "Python script not found: `script_path'"
        exit 601
    }
    
    * Run the Python script with virtual environment
    display "Running: `script_path'"
    display "Using Python: $cf/dm/.venv/bin/python3"
    shell "$dm/.venv/bin/python3" -u "`script_path'"
    
    * Check for errors
    if _rc != 0 {
        display as error "Python script failed with return code: " _rc
        exit 1
    }
    else {
        display "Python script completed successfully"
    }
end