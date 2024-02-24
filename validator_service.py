from confluent_kafka import Consumer, KafkaException
import threading
import time


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
            pass
    # except KeyboardInterrupt:
    #     print('Aborted by user')
    #     pass
    finally:
        consumer.close()

def run_consumer():
    consumer = create_consumer()
    consume_topic(consumer)

if __name__ == '__main__':
    # app.run(debug=True)
    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.start()

    print('Starting Payment Validator service...')
