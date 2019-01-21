# To run
# python3 data-producer.py BTC-USD testName 127.0.0.1:9092


# need to import parameters
import argparse

# life cycle: kill abnormally exited connections
import atexit

import json
import logging
import requests
import schedule
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


# logger
# %s is a place holder it converts variable to "s" (string)
logger_format = "%(asctime)s -15s %(message)s"
logging.basicConfig(format=logger_format)
# create an instance
logger = logging.getLogger("data-producer")
logger.setLevel(logging.DEBUG)
# logger.setLevel(logging.INFO)//change to info when done debugging


# Default kafka topic write to
topic_name = 'analyzer'

# Default kafka broker location, topic_name
kafka_broker = '127.0.0.1:9092'

API_BASE = "https://api.gdax.com"

def check_symbol(symbol):
# 3"" means doc, like */ in java and they exit inside of a function
    """
    helper function to retrieve asset data and send it to kafka
    :param symbol: the symbol of asset eg BTC-USD
    :param producer: instance of a kafka producer
    :return: None
    """
    logger.debug('Cheking symbol')
    try:
        # get products
        response = requests.get(API_BASE + '/products')
        product_ids = [product['id'] for product in response.json()]
        if symbol not in product_ids:
            logger.warn('symbol %s not supported')
            exit()
    except Exception as e:
        logger.warn('fail to fetch products: %s', e)


def fetch_price(symbol, producer, topic_name):
    """
    retrieve data and sent to kafka
    """
    logger.debug('function fetch_price for %s', symbol)
    try:
        response = requests.get('%s/products/%s/ticker'% (API_BASE, symbol))
        price = response.json()['price']
        timestamp = time.time()
        # create a json
        payload = {
            'Symbol': str(symbol),
            'LastTradePrice': str(price),
            'Timestamp': str(timestamp)
        }
        # serialization of json(convert json to str end encode)
        producer.send(topic=topic_name, value=json.dumps(payload).encode('utf-8'))
        logger.debug('Retrieved %s info %s', symbol, payload)
        logger.debug('Sent price for %s to Kafka' % symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send price to kafka, caused by: %s', (timeout_error.message))
    except Exception as e:
        logger.warn('Failed to fetch price: %s', (e))





def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by:% s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)




# works like main() in java: only execute when executing this python file
if __name__ == '__main__':
    # Setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol you want to pull')
    parser.add_argument('topic_name', help='the kafka topic push to')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')

    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # Check if the symbol is supported.
    check_symbol(symbol)

    # init a simple kafka producer
    # see KafkaProducer source code in github
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # Schedule and run the fetch_price function every second
    # function name + parameters
    schedule.every(1).second.do(fetch_price, symbol, producer, topic_name)

    # Setup shutdown hook:release producer function
    atexit.register(shutdown_hook, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)
