This project's objective is to create a redshift dataware house for analysis. In this project we 
convert the raw data residing in s3 bucket into star schema database which is in redshift cluster.

In this project there are mainly files, 
                1. sql_queries.py : stores all queries to create tables and load the data to tables
                2. create_tables.py : creates tables in redshift cluster
                3. etl.py : loads data from s3 to redshift cluster
               
Steps to execute the project:
1. First connect to the cluster by getting the end point of the cluster
2. Create tables by running create_tables.py
3. Load data into cluster from s3 by running etl.py


