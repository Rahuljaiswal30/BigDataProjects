#!/bin/bash
# this script is used to load the bulk data from csv or other delimiter seperated text file into sql datadase 

start_time=$(date +%s)

appdir=/Workspace/BulkLoad/scripts/      #consider this is the linux path where our code is placed
scriptlog=/file/path/for/logs/          #create a path where you want to record the log status of the script execution.
log_stamp=$(date +%Y%m%d_%H%M%S)
DATE=`date +%Y%m%d`
."${appdir}/DTF_shellLogger.sh"     #this shell script is used to create the log file and records all the logs in the log file  

#getting all the value which is passed while running the script in bash and splitting the text and storing it in the value as per the requirement
jobname=$(echo $1 | cut -d '=' -f 2)
schemaTable=%(echo $2 | cut -d '=' -f 2)
datafile=$(echo $3 | cut -d '=' -f 2)

log_filename=${scriptlog}\/${DATE}\/${bulkLoad}_${jobname}_${log_stamp}.log     #generating the dynamic log file name
log_msg "${log_filename}"
touch ${log_filename}       #this commmand will create the log file

#logging/printing the messgage for information on the commmand line.
log_msg "passed arguments are $1 $2 $3"
log_msg "jobname:${jobname}"
log_msg "schemaTable:${schemaTable}"
log_msg "datafile:${datafile}"

#collect all the login credential which is required to connect to the SQL serer database 
database='<database>'
hostname='<server>'
port='<port_number>'
userId='<sv-dtfuser>'
password='<password>'
field_delimiter=',' #here it is comma

bcpcommand="bcp ${database}.${schemaTable} IN {datafile} -S ${hostname},${port} -U ${userId} -P ${password} -c -t '${field_delimiter}' -F 2 -u"
echo $bcpcommand
eval $bcpcommand

if [ $? -ne 0 ];then
    log_msg "Error: Issue executing BCP command"
    exit 1
fi

#f'{database}.{sql_server_table}',  # Fully qualified table name
#'in',  # Specifies an import operation
#csv_file_path,  # Input file path
#'-S', server,  # SQL Server instance
#'-U', username,  # SQL Server username
#'-P', password,  # SQL Server password
#'-c',  # Use character data type
#'-t',   ,  # Field ter#minator
#'-F', str(csv_file_first_row),  # First row to copy (useful if your CSV has headers)
