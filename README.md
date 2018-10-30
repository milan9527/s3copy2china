# s3copy2china
Copy S3 files from global regions to China automatically.
S3 notification + Lambda + Dynamodb.

When S3 object is created or deleted, Lambda function is invoked. 
If object size <= 5MB, download object to Lambda /tmp. Upload it to S3 China bucket.
If object size > 5MB, get object parts by range with 5MB size. Upload all parts to global S3 bucket. Record information to Dynamodb. After parts are uploaded, another Lambda function is invoked to upload multi parts to China S3 bucket in parallel. Complete multi-part upload task when all parts are transferred.

Set Lambda timeout to 5 minutes.
