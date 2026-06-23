# Microsserviço Ranking
# Responsável pelo processamento dos votos associados às promoções. Ao
# receber um evento, o serviço deve inicialmente validar a assinatura digital da
# mensagem para garantir sua autenticidade e integridade. Para cada evento validado, ele
# processa o voto (positivo ou negativo) da promoção correspondente, atualiza o contador
# de votos e recalcula o score de popularidade da promoção específica. Caso o score
# ultrapasse um limite definido pelo sistema, a promoção deve ser considerada uma
# promoção em destaque (hot deal). Quando isso ocorrer, o serviço assina e publica um
# novo evento indicando que a promoção foi destacada. Todos os eventos publicados por
# esse serviço devem ser assinados digitalmente antes de serem enviados ao RabbitMQ.
# O ranking consome o evento promocao.voto, assina digitalmente e publica o
# evento promocao.destaque.

import os
import sys
import json
import pika
from protocol import VoteEvent, BaseEvent, asdict
from keys import load_private_key, load_public_key

def main():
    private_key = load_private_key('ranking')
    gateway_public_key = load_public_key('gateway')

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    exchange_name = 'direct_logs'
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    queue_name = 'ranking_queue'
    channel.queue_declare(queue=queue_name)

    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='promocao.voto')

    # Local storage for votes and featured tracking
    votes_db = {}
    featured_promotions = set()

    print('[*] Ranking Microservice waiting for votes. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        data = json.loads(body)
        
        event = VoteEvent(**data)
        
        if not event.is_signature_valid(gateway_public_key):
            print(f"[!] Invalid signature for vote on {event.promotion_id}. Discarding.")
            return
        
        print(f"[✓] Signature valid. Vote processed for {event.promotion_id}")
        
        promo_id = event.promotion_id
        
        if promo_id not in votes_db:
            votes_db[promo_id] = 0

        # Set the number of votes required to become a hot deal
        current_threshold = event.total_active_users / 2 + 1

        votes_db[promo_id] += event.vote
        print(f"    Total votes: {votes_db[promo_id]}    Current HOT DEAL threshold: {current_threshold}")

        if votes_db[promo_id] >= current_threshold and promo_id not in featured_promotions:
            featured_promotions.add(promo_id)
            print(f"[!] {promo_id} reached HOT DEAL status! Publishing event...")
            
            featured_event = BaseEvent(
                promotion_id=promo_id,
                category=event.category,
                product_name=event.product_name
            )
            
            channel.basic_publish(
                exchange=exchange_name,
                routing_key='promocao.destaque',
                body=json.dumps(asdict(featured_event))
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