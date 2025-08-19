# Example of running Valohai execution on Snowpark

This code example uses the [Insurance dataset](https://www.kaggle.com/datasets/sridharstreaks/insurance-data-for-machine-learning?resource=download) from Kaggle and originated from [tx-smitht's repository](https://github.com/tx-smitht/sf-train-inference-pipeline.git) with adjustment to work better with Valohai.

In this example, we will run a Valohai pipeline that has 2 steps:
- Step 1:
  - Load the data from the csv file
  - Insert 10000 rows into a table called `SOURCE_OF_TRUTH`
  - Insert the rest of the data into a table called `INCOMING_DATA_SOURCE`
- Step 2:
  - Use the data in `SOURCE_OF_TRUTH` to train and test a model to predict the insurance charges
  - Log the model to Snowflake's Model registry

This example uses `snowflake-ml-python` for training the model, but it can also be easily replaced with other popular libraries as you have full control over the environment in Valohai.
# Preparing the database in Snowflake

```sql
CREATE or REPLACE TABLE INSURANCE.ML_PIPE.SOURCE_OF_TRUTH (
	AGE NUMBER(38,0),
	GENDER VARCHAR(16777216),
	BMI FLOAT,
	CHARGES FLOAT,
	CHILDREN NUMBER(38,0),
	SMOKER VARCHAR(16777216),
	REGION VARCHAR(16777216),
	MEDICAL_HISTORY VARCHAR(16777216),
	FAMILY_MEDICAL_HISTORY VARCHAR(16777216),
	EXERCISE_FREQUENCY VARCHAR(16777216),
	OCCUPATION VARCHAR(16777216),
	COVERAGE_LEVEL VARCHAR(16777216)
);

CREATE or REPLACE TABLE INSURANCE.ML_PIPE.INCOMING_DATA_SOURCE (
	AGE NUMBER(38,0),
	GENDER VARCHAR(16777216),
	BMI FLOAT,
	CHARGES FLOAT,
	CHILDREN NUMBER(38,0),
	SMOKER VARCHAR(16777216),
	REGION VARCHAR(16777216),
	MEDICAL_HISTORY VARCHAR(16777216),
	FAMILY_MEDICAL_HISTORY VARCHAR(16777216),
	EXERCISE_FREQUENCY VARCHAR(16777216),
	OCCUPATION VARCHAR(16777216),
	COVERAGE_LEVEL VARCHAR(16777216)
);


CREATE OR REPLACE EVENT TABLE INSURANCE.ML_PIPE.MODEL_TRACES;
ALTER ACCOUNT SET EVENT_TABLE = INSURANCE.ML_PIPE.MODEL_TRACES;
```

**Note:** If neccessary, grant access to the user role used in Valohai Environment

```sql
GRANT ALL ON SCHEMA INSURANCE.ML_PIPE TO ROLE <your-role>;
```