#!/usr/bin/env python3
"""
Key generation utility for distributed system services.
Generates RSA key pairs and stores them as .pem files.
"""

import os
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

SERVICES = ['gateway', 'promocao', 'ranking', 'notification', 'client1', 'client2']

def generate_keys():
    """Generate RSA key pairs for all services."""
    
    # Ensure public_keys directory exists
    if not os.path.exists('public_keys'):
        os.makedirs('public_keys')
    
    for service in SERVICES:
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        # Save private key
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        with open(f'{service}_private.pem', 'wb') as f:
            f.write(private_pem)
        
        # Save public key
        public_key = private_key.public_key()
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        with open(f'public_keys/{service}_public.pem', 'wb') as f:
            f.write(public_pem)
        
        print(f"[✓] Generated keys for {service}")

if __name__ == '__main__':
    generate_keys()
    print("\n[✓] All keys generated successfully!")
