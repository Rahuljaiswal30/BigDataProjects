Name: BCP_utility.sh
Author : Rahul Jaiswal
Usage : Loading of data with very high volume in SQL server using BCP utility.


Explanation:
This script is designed to efficiently load large volumes of data into SQL Server from various delimited file formats such as CSV and TSV. Its primary purpose is to streamline the data loading process by automating the task of populating a staging table within the database. By significantly increasing the speed of data ingestion, it minimizes the time required for loading operations. Furthermore, it eliminates the need for manual intervention, replacing cumbersome manual processes with a single command and argument, thus enhancing operational efficiency and reducing manual workload.


How to use:
> open command promt
> login to the server with the user and passwor who has the access to the SQL server database.
> script: <path/of/the/script/bulkLoad.sh --jobname=<JOBNAME> --schemaTable=<schema.table> --datafile=<src_file_path/file.csv>