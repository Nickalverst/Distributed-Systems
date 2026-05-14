# Microsserviço Notificação
# Responsável por distribuir notificações sobre promoções publicadas no sistema.
# Esse serviço consome eventos relacionados à publicação de novas promoções e à
# identificação de promoções em destaque. Ao receber um evento, o serviço deve validar
# a assinatura digital da mensagem para garantir sua autenticidade e integridade. Após a
# validação, o serviço identifica a categoria associada à promoção e publica uma notificação
# correspondente no RabbitMQ.
# Esse serviço consome os eventos promocao.publicada e promocao.destaque, e
# publica eventos promocao.categoria1, promocao.categoria2, ..., promocao.categoriaN.
# Para cada nova promoção em destaque, ele deve publicar um novo evento na categoria
# correspondente com a palavra “hot deal”.

import os
import sys
import json
import pika
from protocol import BaseEvent, NotificationEvent, asdict
from keys import load_private_key, load_public_key

def main():
    private_key = load_private_key('notification')
    promocao_public_key = load_public_key('promocao')
    ranking_public_key = load_public_key('ranking')

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    exchange_name = 'direct_logs'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    queue_name = 'notification_queue'
    channel.queue_declare(queue=queue_name)

    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='promocao.publicada')
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='promocao.destaque')

    print('[*] Notification Microservice waiting for events. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        data = json.loads(body)

        event = BaseEvent(**data)

        # Determine if this notification is a hot deal based on the routing key it came from
        is_hot_deal = (method.routing_key == 'promocao.destaque')
        
        # Choose the appropriate public key for verification
        public_key = ranking_public_key if is_hot_deal else promocao_public_key
        
        if not event.is_signature_valid(public_key):
            print(f"[!] Invalid signature for promotion {event.promotion_id}. Discarding.")
            return

        notification = NotificationEvent(
            promotion_id=event.promotion_id,
            category=event.category,
            is_hot_deal=is_hot_deal,
            product_name=event.product_name
        )
        
        target_routing_key = f"promocao.{notification.category}"

        print(f"[x] Routing notification to '{target_routing_key}' (Hot Deal: {is_hot_deal})")

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=target_routing_key,
            body=json.dumps(asdict(notification))
        )

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)