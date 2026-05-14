# O processo cliente consumidor é responsável por receber notificações sobre promoções
# de interesse. Quando um usuário decide seguir uma determinada categoria de produto,
# ele passa a consumir notificações de promoções desta categoria no RabbitMQ.
# (0,2) Cada cliente deve manifestar seu interesse em receber notificações de eventos sobre
# promoções de diferentes categorias e promoções em destaque. Por exemplo, um cliente
# interessado em promoções de livros, jogos e de promoções em destaque consumirá os
# eventos promocao.livro, promocao.jogo e promocao.destaque, respectivamente. Ao
# receber uma mensagem de notificação, esta será exibida no terminal.
# Para simplificar, as categorias de interesse dos usuários podem estar definidas no código
# do cliente (hard coded).
# O sistema deve utilizar uma exchange do tipo direct ou topic no RabbitMQ. Os eventos
# devem utilizar routing keys hierárquicas, permitindo que consumidores se inscrevam
# em diferentes categorias de eventos utilizando padrões de binding. Cada cliente pode criar
# sua própria fila e associá-la às routing keys correspondentes às categorias de interesse.

import os
import sys
import json
import pika
from protocol import NotificationEvent
from keys import load_public_key

def main():
    notification_public_key = load_public_key('notification')

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    exchange_name = 'direct_logs'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    categorias_interesse = ['promocao.livro', 'promocao.jogo']

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    for categoria in categorias_interesse:
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=categoria)

    print(f"[*] Client 1 waiting for {categorias_interesse}. To exit press CTRL+C")

    def callback(ch, method, properties, body):
        data = json.loads(body)
        event = NotificationEvent(**data)

        status = "[!!] HOT DEAL [!!]" if event.is_hot_deal else "New Promotion"
        print(f"\n[{status}]")
        print(f"Category: {event.category}")
        print(f"Promo ID: {event.promotion_id}")
        print(f"Product: {event.product_name}")
        print("-" * 30)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nUser Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)