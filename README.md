# Introduction 
A music streaming startup, Sparkify, has grown its user base and song database and wants to move its processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I'm tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

# Description
We will be using AWS to build an ETL pipeline for a database hosted on Redshift. To complete this project, we will need to load the data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. 

Before proceeding, we have to gain a comprehensive understanding of the song dataset and log dataset. This will enable us to effectively define our SQL statements in the `sql_queries.py` file. Two primary data-cleaning steps are involved:
1. Timestamp Format Adjustment: We will standardize the timestamp format using the epoch format, ensuring consistency and compatibility for further analysis.
2. Handling NULL Values: Rows containing NULL values in specific columns will be removed. This step guarantees the quality of our data and enhances its usability.

# Files
1. dwh.cfg - Configuration to access Redshift cluster.
2. sql_queries.py - Define the SQL statements and it will be imported into the two other files below.
3. create_table.py - Create face and dimension tables for the start schema in Redshift.
4. etl.py - load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift. 

# How It Works
Follow these steps to set up and run the project on your local machine. This guide assumes you are already familiar with AWS services like IAM roles, Redshift clusters, and S3 access.
1. Clone the repository to your local machine using the following command.
   
   > https://github.com/Lewis-Chua/Udacity-Project-Data-Warehouse.git
3. Make sure you have configured your AWS IAM role for Redshift and S3 access. Then you will have to create Redshift cluster. Refer to the following resources if you need assistance. 
- [How to create AWS IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create.html)
- [How to create Redshift cluster](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html)
4. Open the files in Jupyter Notebook or Visual Studio Code for convenient editing and execution. 
5. Edit the dwh.cfg file. Input your Redshift cluster's endpoint in the `HOST` field. Make any required adjustments for `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_PORT`, and `ARN`.
6. Run create_tables.py in the terminal to create schemas in your Redshift cluster
7. Run etl.py in the terminal to execute the ETL process
8. Open the Query editor in your Redshift cluster to start querying the data and gain insights. 

# Exploring Further
## 1. Data Validation
To ensure the accuracy and integrity of our data, we could perform data validation. Compare the result in both staging tables and final tables to confirm the rows match as expected. This helps us to identify any discrepancies or issues in the data transformation and loading pipeline. 
## 2. Quick Analytic 
- Conduct an analysis to determine the distribution of free and paid users on the platform. It could provide valuable information about user engagement and potential monetization strategies. 
- Analyze the ranking of songs played over a specific time period. This can help you identify the most popular songs and artists, contributing to recommendations and content optimization.
- Investigate the time periods when people listen to music most frequently. Understanding peak listening times can assist in targeted marketing efforts and scheduling content releases. 

