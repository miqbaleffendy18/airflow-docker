from airflow.models import Variable
from cryptography.fernet import Fernet

def load_key() -> Fernet:
    key = Variable.get("VAR_SECRET")
    return Fernet(key.encode())

def decrypt_var(ciphertext: str) -> str:
    fernet = load_key()
    return fernet.decrypt(ciphertext.encode()).decode()