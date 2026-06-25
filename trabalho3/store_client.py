# Store Client (external system)
# Simulates a store that registers promotions by consuming the MS Gateway REST API.
# The store digitally signs each promotion before sending it, and the backend only accepts
# promotions whose signature is validated with the corresponding public key.
#
# Flow:
#   1. Store builds the promotion data (PromotionReceivedEvent)
#   2. Store signs the event with its private key
#   3. Store sends POST /promocoes to the Gateway with the signed payload
#   4. Gateway publishes to RabbitMQ -> MS Promotion validates the signature -> publishes promocao.publicada
#   5. Store receives HTTP confirmation (201) from the Gateway

import sys
import requests
from enum import IntEnum
from protocol import PromotionReceivedEvent, asdict
from keys import load_private_key

BASE_URL = "http://localhost:5000"

class Category(IntEnum):
    LIVRO = 1
    JOGO = 2
    ELETRONICO = 3
    ROUPA = 4
    ALIMENTO = 5
    ESPORTE = 6
    CASA = 7
    VIAGEM = 8

CATEGORY_INFO = {
    Category.LIVRO:      ('livro', 'Livros'),
    Category.JOGO:       ('jogo', 'Jogos'),
    Category.ELETRONICO: ('eletronico', 'Eletrônicos'),
    Category.ROUPA:      ('roupa', 'Roupas'),
    Category.ALIMENTO:   ('alimento', 'Alimentos'),
    Category.ESPORTE:    ('esporte', 'Esportes'),
    Category.CASA:       ('casa', 'Casa'),
    Category.VIAGEM:     ('viagem', 'Viagens'),
}

def register_promotion(private_key, promotion_id, category, product_name, store_email):
    """Builds, signs and sends a promotion to the Gateway via REST."""

    event = PromotionReceivedEvent(
        promotion_id=promotion_id,
        category=category,
        product_name=product_name,
        store_email=store_email
    )

    # Digital signature: ensures authenticity and integrity of the event.
    # The Gateway/MS Promotion will validate this with the store's public key.
    event.sign_event(private_key)

    payload = asdict(event)

    print(f"\n[store] Sending signed promotion '{promotion_id}' to the Gateway...")

    try:
        response = requests.post(f"{BASE_URL}/promocoes", json=payload, timeout=5)
    except requests.exceptions.ConnectionError:
        print("[!] Could not connect to the Gateway. Is it running on localhost:5000?")
        return None

    if response.status_code == 201:
        data = response.json()
        print(f"[✓] Promotion accepted by the Gateway! Confirmed ID: {data.get('promotion_id')}")
        return data
    else:
        print(f"[!] Gateway rejected the promotion (status {response.status_code}): {response.text}")
        return None


def menu():
    private_key = load_private_key('store')

    print("=== Store Client (external system) ===")
    print("Each promotion registered here will be signed with the store's private key.")
    
    store_email = ""
    while not store_email:
        store_email = input("Store e-mail (to receive notifications): ").strip()
        if not store_email:
            print("[!] Store e-mail cannot be empty. Please enter a valid e-mail address.")

    while True:
        print("\n--- New promotion ---")
        promotion_id = input("Promotion ID: ").strip()
        
        print("Available categories:")
        for c in Category:
            cid, label = CATEGORY_INFO[c]
            print(f"  {c.value}: {label}")
        category_index_raw = input("Category index (enter the number): ").strip()

        try:
            category_index = int(category_index_raw)
        except ValueError:
            print("[!] Invalid index. Please enter a number corresponding to a category.")
            continue

        try:
            selected = Category(category_index)   # direct conversion provided by IntEnum
        except ValueError:
            print("[!] Category index out of range. Try again.")
            continue

        category_id = CATEGORY_INFO[selected][0]
        product_name = input("Product name: ").strip()

        if not all([promotion_id, category_id, product_name, store_email]):
            print("[!] All fields are mandatory. Try again.")
            continue

        register_promotion(private_key, promotion_id, category_id, product_name, store_email)

        again = input("\nRegister another promotion? (y/n): ").strip().lower()
        if again != 'y':
            print("Closing store client.")
            break


if __name__ == '__main__':
    try:
        menu()
    except KeyboardInterrupt:
        print('\nInterrupted')
        sys.exit(0)