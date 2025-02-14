## Swiggy Snowflake Project

## Project files

[Data Engineering Simplified](https://data-engineering-simplified.medium.com/swiggy-end-to-end-data-engineering-project-3f1af55005bf)


## Concepts:
- ER Diagram to understand the source system.
- Initial load and Delta load.

## Notes:
- Dimension modeling, often referred to as the star schema in the data warehousing domain.
- Adheres to the principles of Second Normal Form (2NF) in database design.
- A star schema typically consists of a central fact table surrounded by multiple dimension tables.
- Fact tables derive from transactional data, while dimension tables originated from master data.
- Copy commands in Snowflake transfers files (CSV or JSON) from a stage location into Snowflake tables.
- Stream objects are used to track the activity in all the tables in different layers (stage, clean, business). Here Stream object supports two modes: APPEND-ONLY & ALL-MODE
- APPEND-ONLY mode captures only insert operation, ALL-MODE captures insert, update and delete operations performed on the base table associated with the stream.
- First we created schema of all the layers (stage, clean, business).
- Then we created table in stage layer.
- Then we created stream object of that table in stage layer.
- Then we created table in clean layer attached to schema.
- Then we performed merge operation to get the data from stage to clean location via stage stream object.
- Merge Operation Logic: Whenever an insert operation occurs on the stage table (base object). The stream object captures the changes. The metadata columns in the stream object indicate whether the change is an insert or an update on the base table.
- When the record in the base table is updated, the stream object generates two records for each update.
- DELETE/TRUE represents the previous row state, While INSERT/TRUE reflects the current row state in the business table.
- The Merge statement compares the Clean Layer Location table with the Location stream and performs an UPSERT operation.
- From the stage stream object we ran the merge operation to populate location and this location clean schema is also having a stream object.
- So any changes done in the location table of clean schema will be tracked by clean location stream object.
- As a next step we gonna create Location Dimension table for consumption layer. Location table from consumption layer will also take the data from stream object as stream object only shows the change and not the entire dataset.
- This is the benefit as it only performs the upsert operation and reflects the newly updated data not the entire dataset. This is the actual meaning of UPSERT operation.
- This is helpful during the delta load.
- Abbrevations followed in entire project: _hk = HASH KEY, _sk = SURROGATE KEY, _fk = FOREIGN KEY, _id = PRIMARY KEY, _pk = PRIMARY KEY.
- Upto this part we are just doing an initial load and not doing delta load so everytime when we perform merge process it will show the number rows inserted, It won't show the number of rows updated as we are not doing any delta load.
- At the time of delta load it might contain the data needs to be updated so at that time it might show the updated rows.
- After merging the data in consumption layer, we will be able to see the data lineage. As consumption layer is the business ready layer it won't have any new down stream.
- But if we see the data lineage chart for the table then we will be able to see that data is flowing from stage to clean and clean to consumption.
- So if you see the data lineage for location table carefully, you will be able to see that it exactly matches with all the operations we performed.
- We loaded the csv format data into stage table, updated the stream. From stage stream we copied the data into clean table.
- In clean layer we created table and stream both. Now in consumption layer we loaded data from clean stream.
- In stage schema we didn't follow any primary key we just kept source data as it is by adding four extra columns.
- In clean layer we added some additional field along with datatype changes.
- And in Consumption schema, location dimension table is following SCD2.
- So table in clean schema will always match source table however location dimension table in consumption schema will track all the changes.
- And number of record in clean and location dimension will always vary.
- As a next step we will perform delta load for location entity.
- Now when we perform delta load, it will add new rows from csv file to stage location.
- So it will update only newly added rows into stream object. That's how stream object tracks the activity for the table it has been created.
- Now if we run the merge query again it will not copy all the records from stage to clean, it will just look up over the location stream object, then it will copy only newly updated records. After completion of successful load it will delete the updated records from location stream object.
- Now in the same pattern, location stream object in clean layer will show two newly added recrods.
- Now we will run the merge statement from clean to consumption layer.
- This will add two newly added records to location table in consumption layer and remove those two records from stream object in clean layer.
- Now we will run second delta load for location table, this contains the change in only one column and all other values are duplicate. So it will load new rows in stage layer as it without any change.
- But when we run the merge statement to load the data from stage to clean it will check the conditions and as per the conditions the records which fulfills the condition it will upsert it otherwise it will ignore the records.
- Now if we see the stream object of location in clean layer it has duplicate records. Let's just check the records for 'MUMBAI'.
- It contains 2 records because active_flag hasbeen updated, previously it was Yes and it got updated with No. So it will delete the old record and insert the new record. So this is how stream object keeps track of all the records.
- Now we will run the merge statement from clean to consumption layer.
- It will insert 3 records and update 4 records. Because 3 records are added newly but 4 were already present just one cell has new value.
- We have implemented slowly change data in consumption layer, if there is any update in any record then it will add the timestamp to it. That's how we know that record hasbeen updated recently.
- Now before we move forward with implementing the restaurant entity we will try to load invalid data.
- This invalid data contains pipe separated values. When we run the copy command it will load entire data into first column as the schema has been created using csv format and incoming data is in pipe format so our schema doesn't support that.
- So first we will run copy command, it will copy the data from file to stage layer.
- Then we will check the stream object in stage location to see the activity and then check the data in stage layer.
- Then we will run the merge command to copy data from stage to clean, it will remove the data activity from stage stream obj and add the records in clean layer.
- Then we will run the merge command to copy data from clean to consumption. it will remove the data activity from clean stream obj and add the records in consumption layer.
- Now when we run the merge command for clean layer to ingest the invalid data it will fail as the format doesn't match the expectations.
- As the records are not loaded in clean layer, it will persist into stage stream object for location.
- And we can't move forward. So this is the issue and without fixing this we can't move forward.
- To fix these type of issues, in organizations we create the faulty record lookup tables on daily bases. If this type of activity occurs then the faulty records will be directly loaded to faulty lookup tables. And we can see the reason for the fault.
- Now we created temp table and loaded all the records from location stream object from stage layer. We also deleted the records from location stage table.
- As bad data separator didn't worked, We will try by adding junk data and see it will fail or not.
- As in merge statement we have used the numeric values and our file contains junk values as string, so it won't load the data from stage to clean layer.
- Now we will follow the same process, we will dump the invalid data to temp table, so it will clean up the location stream object and we can move forward.
- When we run the entire process through orchestration tool we need to make sure that we run the copy command only when we have a clean data otherwise merge statement will continue to fail.
- So when we build a data pipeline there will be many such scenarios where we need to simulate in advanced and accordingly we have to write the merge statement. Or we can check via SQL statement first weather it is possible or not.
- One way is to typecast the data first, if the typecasting before loading is not possible instead of on_error we can say continue (in the copy command), that way we can solve the issue.
- Now we will follow the same process for remaining entities, restaurant, customer, customer-address ...
- Every data project dealing with large volumes of data includes and initial or first time load as we are doing in this project with 5 rows.
- Subsequent data, such as newly added data or updated restaurant information, is typically processed as delta processing. Often referred as an incremental load.
- When delta data is processed daily, it is commonly referred as batch processing in the enter price data warehouse domain.
- If the data is processed more frequently, such as every hour or every 15 minutes, it is referred as micro-batching.
- If every change is pushed to warehouse system real time through charge data capture strategy, then it is named as real time streaming.
- In data warehousing fact table contains the transactional data, for our project order, order item and delivery are the transactional entities, here the concept of granularity is important.
- Every data warehouse project needs date dimensions. Use of Common Table Expression while creating date dimensions. Here the insert statement primarily taking the minimum date value from order tables and once it gets that one it recursively follows the approach to get the date dimensions.
- Here all the transaction tables are created upto clean layer only, and date dimension table is created using the date accquired recursively from order entity.
- 
## Articles:
- [Swiggy Tamming The Elephant](https://bytes.swiggy.com/taming-the-elephant-4c06cad7cf48)

