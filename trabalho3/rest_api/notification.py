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
import resend
from protocol import BaseEvent, NotificationEvent, asdict
from keys import load_private_key, load_public_key

# Configuração da API Resend (https://resend.com)
# Crie uma conta gratuita, gere uma API key e exporte como variável de ambiente:
#   export RESEND_API_KEY="re_xxxxxxxxxxxx"
resend.api_key = os.environ.get("RESEND_API_KEY")

# Em sandbox/teste, o Resend só permite enviar para o e-mail da conta verificada,
# então o remetente deve ser o domínio de teste fornecido por eles.
EMAIL_FROM = os.environ.get("RESEND_FROM_EMAIL", "PromoRadar <onboarding@resend.dev>")

# To: teste@caleintound.resend.app
def send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Envia um e-mail via Resend. Retorna True em caso de sucesso, False caso contrário."""
    if not resend.api_key:
        print("[!] RESEND_API_KEY não configurada. E-mail não enviado.")
        return False

    try:
        resend.Emails.send({
            "from": EMAIL_FROM,
            "to": to_email,
            "subject": subject,
            "html": html_body,
        })
        print(f"[email] Enviado para {to_email} | Assunto: {subject}")
        return True
    except Exception as e:
        print(f"[!] Falha ao enviar e-mail para {to_email}: {e}")
        return False

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
        
        string_hotdeal_signature = " - HOT DEAL" if is_hot_deal else " - regular promotion"
        print(f"\n-------------- NEW NOTIFICATION EVENT{string_hotdeal_signature} --------------")
        
        if not event.is_signature_valid(public_key):
            print(f"[!] Invalid signature for promotion {event.promotion_id}. Discarding.")
            return

        store_email = event.store_email
        if not store_email:
            print(f"[!] No store_email for promotion {event.promotion_id}. Skipping email.")
        else:
            if is_hot_deal:
                subject = f"[!] Sua promoção '{event.product_name}' virou HOT DEAL!"
                html_body = f"""
                    <h2>Parabéns! Sua promoção está em destaque [!]</h2>
                    <p>A promoção <strong>{event.product_name}</strong>
                    (categoria: {event.category}) atingiu o número de votos necessário
                    e agora aparece como <strong>hot deal</strong> para os consumidores.</p>
                    <p>ID da promoção: <code>{event.promotion_id}</code></p>
                """
            else:
                subject = f"[✓] Sua promoção '{event.product_name}' foi aprovada!"
                html_body = f"""
                    <h2>Sua promoção foi publicada [✓]</h2>
                    <p>A promoção <strong>{event.product_name}</strong>
                    (categoria: {event.category}) foi validada e já está
                    visível para os consumidores.</p>
                    <p>ID da promoção: <code>{event.promotion_id}</code></p>
                """

            send_email(store_email, subject, html_body)

        notification = NotificationEvent(
            promotion_id=event.promotion_id,
            category=event.category,
            is_hot_deal=is_hot_deal,
            product_name=event.product_name,
        )

        target_routing_key = f"promocao.{notification.category}"

        print(f"[x] Routing notification to '{target_routing_key}' (Hot Deal: {is_hot_deal})")

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=target_routing_key,
            body=json.dumps(asdict(notification))
        )

        # Hot deal é um destaque global: além de notificar quem segue a categoria,
        # publica também em notificacao.hotdeal para o Gateway repassar via SSE
        # a TODOS os clientes interessados em destaques, independente de categoria.
        #print("!!!!!!! DELETED ROUTING TO 'notificacao.hotdeal' (global hot deal channel) !!!!!!!!")
        if is_hot_deal:
            print(f"[x] Also routing to 'notificacao.hotdeal' (global hot deal channel)")
            channel.basic_publish(
                exchange=exchange_name,
                routing_key='notificacao.hotdeal',
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