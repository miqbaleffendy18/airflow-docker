import json
import boto3
import base64
import os

kms_client = boto3.client('kms')
s3_client = boto3.client('s3')
kms_key = os.environ['AWS_KMS_KEY']

def encrypt_secret(secret_value: str) -> str:

    response = kms_client.encrypt(
        KeyId = kms_key,
        Plaintext = secret_value.encode('utf-8')
    )

    encrypted_secret_value = base64.b64encode(response['CiphertextBlob']).decode('utf-8')
    return encrypted_secret_value

def encrypt_json_secret(secret_value: dict) -> str:

    for key, val in secret_value.items():
        if key in ['user', 'password']:
            secret_value[key] = encrypt_secret(val)

    return secret_value

def re_encrypt_secret(encrypted_value: str) -> str:

    response = kms_client.re_encrypt(
        CiphertextBlob = base64.b64decode(encrypted_value),
        DestinationKeyId = kms_key
    )

    encrypted_secret_value = base64.b64encode(response['CiphertextBlob']).decode('utf-8')
    return encrypted_secret_value


def re_encrypt_json_secret(encrypted_value: dict) -> str:

    for key, val in encrypted_value.items():
        if key in ['user', 'password']:
            encrypted_value[key] = re_encrypt_secret(val)

    return encrypted_value

def process_file(file_path, mode = 'encrypt'):

    try:
        s3_response = s3_client.get_object(
            Bucket='evm-etl',
            Key=file_path + 'variables.json'
        )
        content = s3_response['Body'].read().decode('utf-8')
        data_json = json.loads(content)

        result = {}

        if mode == 'encrypt':        

            for id, secret_value in data_json.items():

                if isinstance(secret_value, dict):
                    result[id] = encrypt_json_secret(secret_value)
                else:
                    result[id] = encrypt_secret(secret_value)

        elif mode == 're_encrypt':

            for id, encrypted_value in data_json.items():

                if isinstance(encrypted_value, dict):
                    result[id] = re_encrypt_json_secret(encrypted_value)
                else:
                    result[id] = re_encrypt_secret(encrypted_value)

        s3_client.put_object(
            Bucket='evm-etl',
            Key=file_path + 'variables_encrypted.json',
            Body=json.dumps(result),
            ContentType='application/json'
        )

        s3_client.delete_object(
            Bucket='evm-etl',
            Key=file_path + 'variables.json'
        )

        print(f"✅ Encryption complete. Output written to {file_path + 'variables_encrypted.json'}")

    except Exception as e:
        print(f"Error during encryption: {e}")
        return None


if __name__ == "__main__":
    file_path = os.environ['FILE_PATH']
    mode = os.environ.get('MODE', 'encrypt')

    process_file(file_path, mode)
