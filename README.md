# For AWS ServerLess Projects.
## When need to parse data from huge zip archives.     

### Works on upload event:
	- download and analyze particular zip part contained header
	- create sqs tasks for each file in zip. (include position in zip for Range download)
	-							
	- uses pickle to serialize self and invoke self to continue working
```								
				Lambda
Add triggers from			Resources that the function's role has access:
- S3     				- Amazon CloudWatch ( default )
				        - Amazon SQS  ( for tasks )
	   			        - AWS Lambda  
					(  invoke ourself if work time near over, look "Lambda : InvokeFunction" )
                                        - Amazon S3   (  read only - access ) 
```													
## SQS Task Look (json):
*{ "bucket":"pro-a-v-zips-east-1", "zipfilname":"Data.zip", "data":{ "file_name": "part_000001.xml", "start_pos": 35046, "uncompressed_size": 107062, "compressed_size": 8353}}*

## P.S. Uses only 80Mb RAM but in case 3Gb RAM setup works 3 times faster 

