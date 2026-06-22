"""
Key loading utility for all microservices.
Loads RSA keys from .pem files stored in the root directory.
"""

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_private_key(service_name: str):
    """Load the private key for a service."""
    try:
        with open(f'private_keys/{service_name}_private.pem', 'rb') as f:
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=None,
                backend=default_backend()
            )
        return private_key
    except FileNotFoundError:
        raise FileNotFoundError(f"Private key not found: {service_name}_private.pem. Run keygen.py first.")

def load_public_key(service_name: str):
    """Load the public key for a service."""
    try:
        with open(f'public_keys/{service_name}_public.pem', 'rb') as f:
            public_key = serialization.load_pem_public_key(
                f.read(),
                backend=default_backend()
            )
        return public_key
    except FileNotFoundError:
        raise FileNotFoundError(f"Public key not found: public_keys/{service_name}_public.pem. Run keygen.py first.")
