# this code was written to perform the analyses described in 
# Nation’s Business and the Environment: 
# The U.S. Chamber’s Changing Relationships with DDT, “Ecologists,” Regulations, and Renewable Energy
# as published in 
# [publication]

# this is an implementation of this: https://cloud.google.com/vision/docs/pdf#vision_text_detection_pdf_gcs-python

import sys
import os
import re
from google.cloud import vision
from google.cloud import storage
from google.protobuf import json_format

#with .pdf files uploaded to a google cloud storage bucket in sequential format yyyy.mm
#sends files for OCR via google cloud vision API
#resulting in large batch of _result ## .json files from which text must be extracted
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "c:\location\credentials.json" #credentials file location
client = vision.ImageAnnotatorClient()
mime_type = 'application/pdf'
feature = vision.types.Feature(type=vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION)
batch_size = 10 #note: higher numbers are possible but Google often throws errors

gcs_source_uri = 'gs://bucketname/yyyy.mm.pdf' #Google drive file location, using years and months of publication
gcs_source = vision.types.GcsSource(uri=gcs_source_uri)
input_config = vision.types.InputConfig(gcs_source=gcs_source, mime_type=mime_type)
	
gcs_destination_uri = 'gs://bucketname/yyyy.mm.pdf_result ' #
gcs_destination = vision.types.GcsDestination(uri=gcs_destination_uri)
output_config = vision.types.OutputConfig(gcs_destination=gcs_destination, batch_size=batch_size)

async_request = vision.types.AsyncAnnotateFileRequest(features=[feature], input_config=input_config, output_config=output_config)
operation = client.async_batch_annotate_files(requests=[async_request])
operation.result(timeout=18000)

storage_client = storage.Client()
match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
bucket_name = match.group(1)
prefix = match.group(2)
bucket = storage_client.get_bucket(bucket_name)

blob_list = list(bucket.list_blobs(prefix=prefix))
original_stdout = sys.stdout
a = len(blob_list)

#writes text from generated .json to combined .txt files
for j in range(a):
	sys.stdout = original_stdout
	output = blob_list[j]
	name = blob_list[j].name+'.txt'
	#adds zeroes for natural sorting in windows
	if len(name) < 44:
		name = name[0:23] +'0' + name[23:]
	#downloads text
	json_string = output.download_as_string()
	response = json_format.Parse(
			json_string, vision.types.AnnotateFileResponse())
	b = len(response.responses)
	
	#writes text from each page to a file
	with open(name, 'w') as f:	
		sys.stdout = f		
		for i in range(b):
			first_page_response = response.responses[i]
			annotation = first_page_response.full_text_annotation
			print(annotation.text.encode('utf-8', 'ignore'))
		sys.stdout = original_stdout
print('Exported all text from jsons!')
