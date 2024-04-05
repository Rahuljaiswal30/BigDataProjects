#!/bin/bash
#Author : Rahul Jaiswal

#-------------------------------------------------------------------------------
# echo passed parameter and print them to a log file and terminal #
# usage:                                                           
# log_msg "INFO some Info message"
# log_msg "DEBUG some debug message"
# log_msg "WARN some warning message"
# log_msg "ERROR some error message"
# log_msg "FATAL some really fatal message"
#-------------------------------------------------------------------

log_msg(){
    msg=`echo "$*"`
    DATE=`date +%Y%m%d`
    host_name=`hostname`
    run_user=`whoami`

    if [ ! -d "${scriptlog}/${DATE}" ]; then
        echo "#################### Diectory doesn't exist!!"
        mkdir -p ${scriptlog}\/${DATE};
        echo "#################### Created dated directory " ${scriptlog}\/${DATE}
        chmod 777 ${scriptlog}\/${DATE};
    fi
    if [[ -z ${jobname} ]]; then
        sh_log_filename=${scriptlog}\/${DATE}\/${prog_name}_${log_stamp}_shell.log
    else
        sh_log_filename=${scriptlog}\/${DATE}\/${prog_name}_${jobname}_${log_stamp}_shell.log
    fi
    #print to the terminal if we have one
    echo "`date "+%Y.%m.%d-%H:%M:%S %Z"` : ""$msg" 2>&1 | tee -a ${sh_log_filename}

}
    