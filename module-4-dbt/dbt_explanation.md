## analyses
- a place for SQL files that we don't want to expose.
- We can use it for data quality reports. 
- not used generally.

## dbt_project.yml
- most important file in dbt
- tell dbt the defaults
- needed to run dbt commands
- your profile should match with the one in the '.dbt/profile.yml'

## macros
- similar to python functions (reusable logic)
- they help to encapsulate the logic in one place
- they can be tested (more easily since a little chunk)

## README.md 
- documentation of the project
- installation setup guides, contact infos etc.

## seeds 
- a space to upload csv and flat files to add them to dbt
- quick approach (better to fix at source)

## snapshots
- think like a picture of the table at a certain time
- useful to keep history of a column that overwrites itself

## tests
- a place to put assertions in SQL format
- a place for singular tests
- if this SQL command returns > 0 rows, dbt build fails

## models
- dbt suggests 3 sub-folders:
### staging
- put all SQL sources (raw table from database)
- staging files are 1 to 1 copy of your data with minimal cleaning stage:
    - data types
    - renaming columns
- recommended to keep it 1:1 (rows columns)
### intermediate
- anything that is not raw nor you want to expose
- no guidelines, nice for heavy duty cleaning or complex logic
### marts
- If it is in marts it is ready for consumption
- tables ready for dashboards
- properly modeled, clean tables (like star schemas) 

