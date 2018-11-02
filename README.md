# s3copy2china
Copy S3 files from global regions to China automatically.
S3 notification + Lambda + Dynamodb.

When S3 object is created or deleted, Lambda function is invoked. 
If object size <= 5MB, download object to Lambda /tmp. Upload it to S3 China bucket.
If object size > 5MB, invoke other Lambda to call get object API by range with part size, then upload parts to China bucket in parallel. Record information to Dynamodb. Complete multi-part upload task when all parts are transferred.

Set Lambda timeout to 5 minutes.

3 Lambda functions:
S3CopyToChina-MPU.py: Many Lambda functions invoked by "S3CopyToChina-Main". Transfer S3 object parts in parallel.
S3CopyToChina-Main.py: First Lambda invoked by S3 notification. 
S3CopyToChina-Monitor.py: Monitor tasks status by checking Dynamodb.
