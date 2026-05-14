import os
import sys
import pika
import json
import threading
from protocol import PromotionReceivedEvent, VoteEvent, asdict
from keys import load_private_key, load_public_key


def create_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )


def main():
    private_key = load_private_key('gateway')
    promocao_public_key = load_public_key('promocao')

    exchange_name = 'direct_logs'

    promocoes_validas = []

    def start_consumer():
        connection = create_connection()
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(
            exchange=exchange_name,
            queue=queue_name,
            routing_key='promocao.publicada'
        )

        def callback(ch, method, properties, body):
            data = json.loads(body)
            event = PromotionReceivedEvent(**data)

            if not event.is_signature_valid(promocao_public_key):
                print(f"[!] Invalid signature for promotion {event.promotion_id}. Discarding.")
                return

            promocoes_validas.append(data)

        channel.basic_consume(
            queue=queue_name,
            auto_ack=True,
            on_message_callback=callback
        )

        channel.start_consuming()

    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()


    pub_connection = create_connection()
    pub_channel = pub_connection.channel()

    pub_channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    print("[*] Gateway ready. Consumer running in background.")

    while True:
        print("\n=== Menu ===")
        print("1. Cadastrar nova promoção")
        print("2. Listar promoções publicadas")
        print("3. Votar em uma promoção")
        print("0. Sair")

        choice = input("Escolha uma opção: ")

        if choice == '1':
            promotion_id = input("ID da promoção: ")
            category = input("Categoria da promoção: ")
            product_name = input("Nome do produto: ")

            event = PromotionReceivedEvent(
                promotion_id=promotion_id,
                category=category,
                product_name=product_name
            )

            event.sign_event(private_key)

            pub_channel.basic_publish(
                exchange=exchange_name,
                routing_key='promocao.recebida',
                body=json.dumps(asdict(event))
            )

            print("[x] Promoção enviada!")

        elif choice == '2':
            if promocoes_validas:
                print("\nPromoções publicadas:")
                for promo in promocoes_validas:
                    print(
                        f"  - ID: {promo['promotion_id']}, "
                        f"Categoria: {promo['category']}, "
                        f"Produto: {promo['product_name']}"
                    )
            else:
                print("Nenhuma promoção publicada ainda.")

        elif choice == '3':
            promotion_id = input("ID da promoção para votar: ")
            vote = input("Voto (+1 ou -1): ")

            try:
                vote_value = int(vote)
                if vote_value not in [1, -1]:
                    print("Voto deve ser +1 ou -1")
                    continue

                categoria = next(
                    (p['category'] for p in promocoes_validas if p['promotion_id'] == promotion_id),
                    ""
                )

                produto = next(
                    (p['product_name'] for p in promocoes_validas if p['promotion_id'] == promotion_id),
                    "Unknown Product"
                )

                event = VoteEvent(
                    promotion_id=promotion_id,
                    category=categoria,
                    vote=vote_value,
                    product_name=produto
                )

                event.sign_event(private_key)

                pub_channel.basic_publish(
                    exchange=exchange_name,
                    routing_key='promocao.voto',
                    body=json.dumps(asdict(event))
                )

                print("[x] Voto registrado!")

            except ValueError:
                print("Voto inválido. Use +1 ou -1")

        elif choice == '0':
            print("Saindo...")
            break

        else:
            print("Opção inválida. Tente novamente.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)