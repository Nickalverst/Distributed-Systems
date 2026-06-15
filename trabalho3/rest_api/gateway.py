import os
import sys
import pika
import json
import threading
from protocol import PromotionReceivedEvent, VoteEvent, asdict
from keys import load_private_key, load_public_key
from flask import Flask, request, jsonify, Response, stream_with_context
from queue import Queue

app = Flask(__name__)

private_key = load_private_key('gateway')
promocao_public_key = load_public_key('promocao')
exchange_name = 'direct_logs'
promocoes_validas = []
interesses = {}
clientes_sse = {}
clientes_lock = threading.Lock()

def create_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )

pub_connection = create_connection()
pub_channel = pub_connection.channel()
pub_channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

@app.route('/promocoes', methods=['POST', 'GET'])
def cadastrar_promocao():
    if request.method == 'POST':
        data = request.get_json()

        event = PromotionReceivedEvent(
            promotion_id=data['promotion_id'],
            category=data['category'],
            product_name=data['product_name'],
            loja_email=data.get('loja_email', '')
        )

        event.sign_event(private_key)

        pub_channel.basic_publish(
            exchange=exchange_name,
            routing_key='promocao.recebida',
            body=json.dumps(asdict(event))
        )

        return jsonify({'status': 'ok', 'promotion_id': event.promotion_id}), 201

    elif request.method == 'GET':
        return jsonify(promocoes_validas), 200

@app.route('/promocoes/<id>/votos', methods=['POST'])
def registrar_voto(id):
    data = request.get_json()
    vote_value = int(data.get('vote'))

    categoria = next(
        (p['category'] for p in promocoes_validas if p['promotion_id'] == id),
        ""
    )

    produto = next(
        (p['product_name'] for p in promocoes_validas if p['promotion_id'] == id),
        "Unknown Product"
    )

    event = VoteEvent(
        promotion_id=id,
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

    return jsonify({'status': 'ok'}), 201

@app.route('/interesses', methods=['POST'])
def cadastrar_interesse():
    data = request.get_json()
    usuario_id = data['user_id']
    categoria = data['category']
    interesses.setdefault(usuario_id, set()).add(categoria)

    print(f"Interesse cadastrado: {data}")

    return jsonify({'status': 'ok'}), 201

@app.route('/interesses/<categoria>', methods=['DELETE'])
def remover_interesse(categoria):
    data = request.get_json()
    usuario_id = data['user_id']
    interesses.get(usuario_id, set()).discard(categoria)
    return jsonify({'status': 'ok'}), 200

@app.route('/sse', methods=['GET'])
def sse():
    q = Queue()
    user_id = request.args.get('user_id')
    with clientes_lock:
        clientes_sse[user_id] = q

    def stream():
        try:
            while True:
                data = q.get()
                yield f"data: {json.dumps(data)}\n\n"
        except GeneratorExit:
            with clientes_lock:
                clientes_sse.pop(user_id, None)

    return Response(stream_with_context(stream()), mimetype='text/event-stream')

def main():
    def start_consumer():
        connection = create_connection()
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='promocao.publicada')
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='promocao.destaque')
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='promocao.categoria')
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='notificacao.hotdeal')

        def callback(ch, method, properties, body):
            data = json.loads(body)
            routing_key = method.routing_key

            if routing_key == 'promocao.publicada':
                event = PromotionReceivedEvent(**data)
                if not event.is_signature_valid(promocao_public_key):
                    print(f"[!] Invalid signature. Discarding.")
                    return

                promocoes_validas.append(data)

                # Só quem segue a categoria recebe
                with clientes_lock:
                    for user_id, q in clientes_sse.items():
                        if event.category in interesses.get(user_id, set()):
                            q.put({
                                'type': 'new_promotion',
                                'promotion_id': event.promotion_id,
                                'category': event.category,
                                'product_name': event.product_name
                            })

            elif routing_key in ('promocao.destaque', 'notificacao.hotdeal'):
                # Hot deal — todos os conectados recebem, sem filtro
                with clientes_lock:
                    for q in clientes_sse.values():
                        q.put({
                            'type': 'hot_deal',
                            'promotion_id': data['promotion_id'],
                            'category': data['category'],
                            'product_name': data['product_name']
                        })

            elif routing_key == 'promocao.categoria':
                # Notificação de categoria vinda do MS Notificação — filtra por interesse
                with clientes_lock:
                    for user_id, q in clientes_sse.items():
                        if data['category'] in interesses.get(user_id, set()):
                            q.put({
                                'type': 'category_notification',
                                'promotion_id': data['promotion_id'],
                                'category': data['category'],
                                'product_name': data['product_name'],
                                'is_hot_deal': data.get('is_hot_deal', False)
                            })

        channel.basic_consume(
            queue=queue_name,
            auto_ack=True,
            on_message_callback=callback
        )

        channel.start_consuming()

    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    app.run(port=5000, debug=False)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)