from flask import Flask
from confluent_kafka import Consumer, Producer, KafkaException
import threading
import logging
import uuid
import json

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

def error_cb(err):
    print('Error: %s' % err)

def log_cb(log):
    print('Log: %s' % log)

def create_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'validate_payment',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'error_cb': error_cb,
        # 'log_cb': log_cb
        'debug': 'topic'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['payment'])
    return consumer


def consume_topic(consumer):
    consumer.subscribe(['order-events'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
               raise KafkaException(msg.error())
        
            
            print('Received message: {}'.format(msg.value().decode('utf-8')))


            # Extract the order_id from the message
            order_id = json.loads(msg.value().decode('utf-8'))['id']
            
            publish_event(order_id)
            
    except KeyboardInterrupt:
        print('Aborted by user')
        pass
    finally:
        consumer.close()


@app.route('/')
def index():
    return 'Payment validation service'

@app.route('/validate_payment', methods=['GET'])
def validate_payment():
    app.logger.info('INFO')
    print('print statement')


    # consumer = create_consumer()
    # consume_topic(consumer)
    return 'Payment is valid'


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def publish_event(orderid):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    event_data = {
        'validation_id': str(uuid.uuid4()),
        'order_id': str(orderid),
        'event': 'payment_validated',
        'details': {

        }
    }

    # Serialize the event data to JSON
    event_data_json = json.dumps(event_data)

    # Publish the event to the 'payment-events' topic
    # producer.produce('payment-events', 
    #                  event_data_json.encode('utf-8'), callback=delivery_report)
    producer.produce('payment-events', event_data_json.encode('utf-8'))


    producer.flush()


def run_consumer():
    consumer = create_consumer()
    consume_topic(consumer)

def run_flask_app():
    print('run_flask_app() called...')
    app.run(debug=True)

    run_consumer()


if __name__ == '__main__':
    # app.run(debug=True)
    # consumer_thread = threading.Thread(target=run_consumer)
    # consumer_thread.start()
    print('Starting Payment Validator service...')

    run_flask_app()
else:
    print('app.py is being imported')
    run_flask_app()
