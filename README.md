Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I'm tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

Files


How to
1. Git Clone the Repo into your local
2. Make sure you have set up your IAM role for redshift and redshift cluster in aws
3. Open the files in Jupyter Notebook or VS CODE
4. Open the dwh.cfg and input your endpoint into HOST, and make neccessary changes for DB_NAME, DB_USER, DB_PASSWORD, and DB_PORT, and ARN
5. Run create_tables.py in terminal to create schemas in your redshift cluster
6. Run etl.py in terminal to insert data into respective start shcema and fact tables 

Results




Read the data from the two events to understand the data type and also how do we distribute to each tables

cant do sortkey for varchar id 