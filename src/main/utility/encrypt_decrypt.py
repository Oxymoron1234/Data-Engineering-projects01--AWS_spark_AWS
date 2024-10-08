import os, sys
from resources.dev import config
from base64 import urlsafe_b64encode, urlsafe_b64decode
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
#from logging_config import logger
# Constants

try:
    SALT_SIZE = 16
    KEY_SIZE = 32  # AES-256
    ITERATIONS = 100000
    key = config.key
    ak = config.aws_access_key
    sk = config.aws_secret_key
    if not (key and ak and sk):
        raise Exception(F"Error while fetching details for key")
except Exception as e:
    print(f"Error occured. Details : {e}")
    # logger.error("Error occurred. Details: %s", e)
    sys.exit(0)


# Function to generate a salt
def generate_salt():
    return os.urandom(SALT_SIZE)


# Function to derive a key from a password
def derive_key(password: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=KEY_SIZE,
        salt=salt,
        iterations=ITERATIONS,
        backend=default_backend()
    )
    return kdf.derive(password.encode())


# Function to encrypt data
def encrypt_data(plaintext: str , password = key) -> bytes:
    salt = generate_salt()
    key = derive_key(password, salt)

    iv = os.urandom(16)  # Initialization vector
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    ciphertext = encryptor.update(plaintext.encode()) + encryptor.finalize()
    return urlsafe_b64encode(salt + iv + ciphertext)


# Function to decrypt data
def decrypt_data(encrypted_data: bytes , password = key) -> str:
    data = urlsafe_b64decode(encrypted_data)

    salt = data[:SALT_SIZE]
    iv = data[SALT_SIZE:SALT_SIZE + 16]
    ciphertext = data[SALT_SIZE + 16:]

    key = derive_key(password, salt)
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    decrypted = decryptor.update(ciphertext) + decryptor.finalize()
    return decrypted.decode()




# Encrypt credentials
encrypted_aws_access_key = encrypt_data(ak)
encrypted_aws_secret_key = encrypt_data(sk)

# print(f"Encrypted aws access key:{ak} ---- {encrypted_aws_access_key}")
# print(f"Encrypted aws secret key:{sk} ---- {encrypted_aws_secret_key}")



