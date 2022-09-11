# Run Batch with AWS
Follow these steps to execute the batch on Amazon Web Services (AWS) using the _EMR Serverless_ service.

## Requirements
- An account with [Amazon Web Services](https://aws.amazon.com/)
- The [AWS CLI](https://aws.amazon.com/cli/) must be configured and updated

## Usage
- Note that these steps follow the [Amazon EMR Serverless User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html#gs-cli)
- Also note that following these steps will incur some cost on your AWS account
- All commands are to be executed in bash from the project root

### Prepare Storage in S3
- To execute the batch on EMR Serverless, we need to prepare an S3 bucket
- The bucket will hold our spark application .jar file and our input files
- Our batch will write its outputs to the same S3 bucket

```bash
# S3 parameters
S3_BUCKET_NAME=my-unique-bucket-name
S3_ACL_SETTING=private 
S3_BUCKET_REGION=eu-west-1
S3_BUCKET_ARN=arn:aws:s3:::$S3_BUCKET_NAME

# Local file parameters
SPARK_TRACKER=./out/tracker/assembly.dest/out.jar
SRC_CITIES=./data/in/brazil_covid19_cities.csv
SRC_STATES=./data/in/brazil_covid19.csv

# Bucket creation
aws s3api create-bucket \
	--bucket $S3_BUCKET_NAME \
	--acl $S3_ACL_SETTING \
	--create-bucket-configuration LocationConstraint=$S3_BUCKET_REGION \
	--region $S3_BUCKET_REGION
	
# .jar file upload
aws s3api put-object \
	--bucket $S3_BUCKET_NAME \
	--key spark/tracker.jar \
	--body $SPARK_TRACKER
	
# Covid19 data by cities
aws s3api put-object \
	--bucket $S3_BUCKET_NAME \
	--key data/in/brazil_covid19_cities.csv \
	--body $SRC_CITIES
	
# Covid19 data by state
aws s3api put-object \
	--bucket $S3_BUCKET_NAME \
	--key data/in/brazil_covid19.csv \
	--body $SRC_STATES
```

### Configure Execution Role & Policies in IAM
- We need to create an execution role with proper policies to run our batch on EMR Serverless
- Note that this step requires the `S3_BUCKET_NAME` variable to be set

```bash
# More parameters 
AWS_ACCOUNT_ID=$(aws sts get-caller-identity | grep -oP '(?<="Account":\s")[0-9]*')
IAM_ROLE=EMRServerlessS3RuntimeRole
IAM_ROLE_ARN=arn:aws:iam::$AWS_ACCOUNT_ID:role/$IAM_ROLE
IAM_POLICY=EMRServerlessS3AndGlueAccessPolicy
IAM_POLICY_ARN=arn:aws:iam::$AWS_ACCOUNT_ID:policy/$IAM_POLICY

# Create a runtime role 
aws iam create-role \
    --role-name $IAM_ROLE\
    --assume-role-policy-document file://aws/emr-serverless-trust-policy.json

# Create an Access Policy Document
cat <<EOF >./aws/emr-access-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadAccessForEMRSamples",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::*.elasticmapreduce",
                "arn:aws:s3:::*.elasticmapreduce/*"
            ]
        },
        {
            "Sid": "FullAccessToOutputBucket",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "$S3_BUCKET_ARN",
                "$S3_BUCKET_ARN/*"
            ]
        },
        {
            "Sid": "GlueCreateAndReadDataCatalog",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:CreateDatabase",
                "glue:GetDataBases",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:CreatePartition",
                "glue:BatchCreatePartition",
                "glue:GetUserDefinedFunctions"
            ],
            "Resource": ["*"]
        }
    ]
}
EOF

# Create an access policy
aws iam create-policy \
    --policy-name $IAM_POLICY \
    --policy-document file://aws/emr-access-policy.json
    
# Attach the access policy to the execution role
aws iam attach-role-policy \
    --role-name $IAM_ROLE \
    --policy-arn $IAM_POLICY_ARN
```
### Execute on AWS
- With everything configured we can now create our EMR Serverless application
- Note that EMR Serverless is not available in all regions 
- The EMR Serverless application should be hosted in the same reason as our S3 bucket

```bash
# Some more parameters 
EMR_APP_NAME=my-application-name 
EMR_APP_LABEL=emr-6.6.0   # Supports Spark 3.3.0
EMR_APP_TYPE="SPARK"
EMR_APP_REGION=$S3_BUCKET_REGION

# Create an EMR Serverless Application
aws emr-serverless create-application \
  --release-label $EMR_APP_LABEL \
  --type $EMR_APP_TYPE \
  --name $EMR_APP_NAME \
  --region $EMR_APP_REGION
  
# Retrieve the application ID 
# NOTE: This only works if you have exactly 1 EMR Serverless application in the region
EMR_APP_ID=$(aws emr-serverless list-applications --region $EMR_APP_REGION | grep -oP '(?<="id":\s")[a-z0-9]*')

# View the status of the application
# Spark job can be submitted once "state" is "CREATED"
aws emr-serverless get-application \
  --application-id $EMR_APP_ID \
  --region $EMR_APP_REGION
  
# Submit the aggregate job
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role $IAM_ROLE_ARN \
  --name $EMR_APP_NAME \
  --region $EMR_APP_REGION \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "'s3://${S3_BUCKET_NAME}/spark/tracker.jar'",
      "entryPointArguments": [
        "agg",
        "'s3://$S3_BUCKET_NAME/data/in/brazil_covid19_cities.csv'",
        "new_brazil_covid19.csv"
      ],
      "sparkSubmitParameters": "--class TrackerCli --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
    }
  }'

# Submit the compare job
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role $IAM_ROLE_ARN \
  --name $EMR_APP_NAME \
  --region $EMR_APP_REGION \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "'s3://${S3_BUCKET_NAME}/spark/tracker.jar'",
      "entryPointArguments": [
        "cmp",
        "'s3://$S3_BUCKET_NAME/data/in/brazil_covid19.csv'",
        "'s3://$S3_BUCKET_NAME/data/out/new_brazil_covid19.csv'"
      ],
      "sparkSubmitParameters": "--class TrackerCli --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1"
    }
  }'
  
# Download the results
aws s3 cp s3://$S3_BUCKET_NAME/data/out/new_brazil_covid19.csv ./data/out/
aws s3 cp s3://$S3_BUCKET_NAME/data/out/diff_report.json ./data/out/
```

