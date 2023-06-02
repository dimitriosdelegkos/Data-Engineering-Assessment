# Documentation

## Pipeline setup
The pipeline consists of 3 stages:
* Extract
* Transform 
* Load
<!-- end of the list -->
In the first stage i ingest the data from the two provided CSV files. In the next stage, i transform the data, using various techniques (data cleansing, re-formatting, join, data filtering, feature engineering). Finally, i load the transformed data into the database.

## Dependencies
The 3 stages of the pipeline are executed sequentially (orchestrated by Apache Airflow), starting from the extraction phase, continuing with the transformation and loading respectively.

## Instructions

These are the instructions in order to run the
pipeline on a local machine using Docker Compose

- Clone this repo
- Navigate (via cmd) to the folder where the cloned directory is
- Complete the prerequisite steps
- Run the service
```
 docker-compose up -d
```
- Check http://localhost:8080

### Prerequisite steps

Once the pipeline is accessible from the http://localhost:8080, create 3 tables in PostgreSQL, using this code:
```
create table nusers (userid Varchar(20) Primary key);

create table sessions (session_id varchar(20) Primary key,
					   transaction varchar(1),
                       transaction_timestamp timestamp,
                       userid varchar(20),
                       foreign key (userid) references nusers(userid));
                      
 create table session_details (session_id varchar(20),
							 page varchar(10),
                             stimestamp timestamp,
                             foreign key (session_id) references sessions(session_id));
```

## Connect to database

In order to load the processed data into the database, you have to configure the connection.<br>
Once the pipeline is accessible from the http://localhost:8080:

Go to Admin -> Connections and Edit "postgres_default" by setting these values:
- **Host** : postgres
- **Schema** : airflow
- **Login** : airflow
- **Password** : airflow


