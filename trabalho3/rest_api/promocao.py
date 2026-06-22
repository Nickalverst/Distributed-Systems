#  Microsserviço Promocao
# Responsável pelo gerenciamento das promoções no sistema. Esse serviço recebe
# eventos indicando que novas promoções foram recebidas. Ao receber um evento, o serviço registra
# a promoção e publica um novo evento informando que a promoção foi disponibilizada no sistema. 
# O microsserviço Promocao consome os eventos promocao.recebida e publica eventos promocao.publicada.

# Teremos apenas 1 exchange, com uma fila para cada microsserviço. Direcionamos as mensagens via routing key.
# O MS Promocao consome eventos promocao.recebida e publica eventos promocao.publicada.
import os
import sys
import pika
import json
from protocol import PromotionReceivedEvent, PromotionPublishedEvent, asdict
from keys import load_private_key, load_public_key

def main():
    private_key = load_private_key('promocao')
    store_public_key = load_public_key('store')

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    exchange_name = 'direct_logs'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    promocoes = []

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    def callback(ch, method, properties, body):
        data = json.loads(body)
        event = PromotionReceivedEvent(**data)
        
        if not event.is_signature_valid(store_public_key):
            print(f"[!] Invalid signature for promotion {event.promotion_id}. Discarding.")
            return
        
        print(f"[✓] Store signature valid. Promoção recebida: {event.promotion_id}")

        promocoes.append(event)

        published_event = PromotionPublishedEvent(
            promotion_id=event.promotion_id,
            category=event.category,
            product_name=event.product_name,
            store_email=event.store_email
        )

        published_event.sign_event(private_key)

        channel.basic_publish(
            exchange='direct_logs',
            routing_key='promocao.publicada',
            body=json.dumps(asdict(published_event))
        )
        print("[x] Promoção publicada!")

    channel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key='promocao.recebida')

    channel.basic_consume(queue=queue_name,
                        auto_ack=True,
                        on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
        try:
            main()
        except KeyboardInterrupt:
            print('Interrupted')
            try:
               sys.exit(0)
            except SystemExit:
               os._exit(0)