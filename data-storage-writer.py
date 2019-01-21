"""sink data from kafka into hbase"""

import argparse
import atexit
import json
import happybase
import logging

from kafka import KafkaConsumer

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage-writer')
logger.setLevel(logging.DEBUG)

# shut down kafka_consumer, hbase_connection
def shutdown_hook(kafka_consumer, hbase_connection):
	"""
	A shutdown hook to be called before the shutdown.
	"""
	try:
		kafka_consumer.close()
		hbase_connection.close()
	except Exception as e:
		logger.warn('Failed to close kafka consumer or hbase connection for %s', e)

# option not good because there are two may timestamps, also involves hmaster to create col
# row key - Symbol, column - timestamp
#            family:timestamp1     family:timestamp2 ....
# BTC-USD     6000                     6100

# row key - Symbol + timestamp
#                             family:price family:symbol family:timestamp
# BTC-USD:timestamp1            6000
# BTC-USD:timestamp2            6100
# BTC-USD:timestamp3            6200
#

def persist_data(data, hbase_connection, data_table):
	"""
	Persist data into hbase.
	"""
	try:
		logger.debug('Start to persist data to hbase: %s', data)
		parsed = json.loads(data)
		symbol = parsed.get('Symbol')
		price = parsed.get('LastTradePrice')
		timestamp = parsed.get('Timestamp')
		# connect to db table
		table = hbase_connection.table(data_table)

		row_key = "%s-%s" % (symbol, timestamp)
		# save to table
		# from happybaseDoc api
		table.put(row_key, {'family:symbol': symbol,
							'family:trade_time':timestamp,
							'family:trade_price':price})

		logger.info('Persistend data to hbase for symbol: %s, price: %s, timestamp: %s', symbol, price, timestamp)

	except Exception as e:
		logger.error('Failed to persist data to hbase for %s', e)

if __name__ == '__main__':
	# parser: parse command-line arguments into vars
	parser = argparse.ArgumentParser()
	# kafka
	parser.add_argument('topic_name')
	parser.add_argument('kafka_broker')
	# hbase
	parser.add_argument('data_table')
	parser.add_argument('hbase_host')

	# Parse arguments.
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	data_table = args.data_table
	hbase_host = args.hbase_host

	# Initiate a simple kafka consumer.
	kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

	# Initiate a hbase connection.
	hbase_connection = happybase.Connection(hbase_host)

	# Create table if not exists.
	hbase_tables = [table.decode() for table in hbase_connection.tables()]
	# TODO indexing
	if data_table not in hbase_tables:
		hbase_connection.create_table(data_table, {'family': dict()})

	# Setup proper shutdown hook.
	atexit.register(shutdown_hook, kafka_consumer, hbase_connection)

	# Start consuming kafka and writing to hbase.
	# get content and exclude headers etc.
	for msg in kafka_consumer:
		persist_data(msg.value, hbase_connection, data_table)
