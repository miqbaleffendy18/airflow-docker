import warnings
warnings.filterwarnings('ignore')

import os
import re
import boto3

def _s3_client():
	# Use static credentials if present; otherwise fall back to boto3 default
	# credential chain (e.g. IRSA service account token on Kubernetes).
	kwargs = {'region_name': os.environ['AWS_DEFAULT_REGION']}
	access_key = os.environ.get('AWS_ACCESS_KEY_ID')
	secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
	if access_key and secret_key:
		kwargs['aws_access_key_id'] = access_key
		kwargs['aws_secret_access_key'] = secret_key
	return boto3.client('s3', **kwargs)

def upload_data_to_s3(folder_name, file_name):
	s3 = _s3_client()
	try:
		s3.upload_file(file_name, os.environ['bucket_name'], folder_name + re.search(r'[^/]+$', file_name).group())
		return 'Success Upload to S3'
	except Exception as e:
		raise Exception('Failed Upload to S3')
		return str(e)

def load_data_from_s3(folder_name,file_name):
	s3 = _s3_client()
	try:
		obj = s3.get_object(Bucket=os.environ['bucket_name'], Key=folder_name + re.search(r'[^/]+$', file_name).group())
		print('Success Load Data from S3...!!!')
		return obj
	except Exception as e:
		raise Exception('Failed Load Data from S3...!!!')
		return str(e)

def download_model_from_s3(bucket_name, folder_name, local_folder_name):
    """
    Fungsi untuk mengunduh model dari bucket S3 ke lokal.

    Args:
        bucket_name (str): Nama bucket S3.
        model_key (str): Path file model di dalam bucket S3.
        local_model_path (str): Path lokal untuk menyimpan model yang diunduh.
    """
    # Unduh model dari S3 ke lokal
    s3 = _s3_client()

    s3.download_file(bucket_name, folder_name, local_folder_name)