# Example of running Valohai execution on Snowpark

This code example uses the [Insurance dataset](https://www.kaggle.com/datasets/sridharstreaks/insurance-data-for-machine-learning?resource=download) from Kaggle and originated from [tx-smitht's repository](https://github.com/tx-smitht/sf-train-inference-pipeline.git) with adjustment to work better with Valohai.

In this example, there are 4 available steps that can be used to construct pipelines
- Data loading:
  - Load the data from the csv file
  - Insert 10000 rows into a table called `SOURCE_OF_TRUTH`
  - Insert the rest of the data into a table called `INCOMING_DATA_SOURCE`
  
- Model training:
  - Use the data in `SOURCE_OF_TRUTH` to train and test a model to predict the insurance charges
  - Log the model to Snowflake's Model registry
  - Model files are also saved in Valohai store
  
- Mock streaming data:
  - Use the some of the data in `INCOMING_DATA_SOURCE` and insert it to `LANDING_TABLE` to mock the process of data coming in.
  
- Running inference:
  - The model will be used to run inference on the incoming data
  - The result is saved in the `INSURANCE_GOLD` where the predicted price is added for comparison
  
- Deploy a streamlit app as a long runing service in Snowpark Container Service:
  - Trigger a long runing service in Snowpark Container Service, which host a Streamlit app that connects to Snowflake database and display the data.
  - This example uses Streamlit, but you can also host FastAPI, Flash or any other framework app.

This example uses `snowflake-ml-python` for training the model, but it can also be easily replaced with other popular libraries as you have full control over the environment in Valohai.

# Prerequisites

## Streamlit app docker image

- Use snowflake credentials to login to docker CLI
- Build docker image of the `insurance_streamlit` app, using the `Dockerfile` file.
- Push the image into Snowflake registry
## Preparing the database in Snowflake

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

CREATE or REPLACE TABLE INSURANCE.ML_PIPE.LANDING_TABLE (
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

-- Create the stream on the landing table
CREATE OR REPLACE STREAM STREAM_ON_LANDING ON TABLE LANDING_TABLE;

-- Create a gold table for the records and their predictions to land
CREATE OR REPLACE TABLE INSURANCE_GOLD(
    AGE NUMBER(38,0),
	GENDER VARCHAR(16777216),
	BMI FLOAT,
	CHILDREN NUMBER(38,0),
	SMOKER VARCHAR(16777216),
	REGION VARCHAR(16777216),
	MEDICAL_HISTORY VARCHAR(16777216),
	FAMILY_MEDICAL_HISTORY VARCHAR(16777216),
	EXERCISE_FREQUENCY VARCHAR(16777216),
	OCCUPATION VARCHAR(16777216),
	COVERAGE_LEVEL VARCHAR(16777216),
    METADATA$ROW_ID VARCHAR(16777216),
    METADATA$ISUPDATE BOOLEAN,
    METADATA$ACTION VARCHAR(16777216),
    METADATA_UPDATED_AT DATE,
    CHARGES FLOAT,
    PREDICTED_CHARGES FLOAT
);
```

**Note:** If neccessary, grant access to the user role used in Valohai Environment

```sql
GRANT ALL ON SCHEMA INSURANCE.ML_PIPE TO ROLE <your-role>;
```