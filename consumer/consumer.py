from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import datetime


if __name__ == '__main__':

    #specify config file and topic
    config_file = "librdkafka.config"
    topic = "assignment1-sensor-reading"
    config = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': config['bootstrap.servers'],
        'sasl.mechanisms': config['sasl.mechanisms'],
        'security.protocol': config['security.protocol'],
        'sasl.username': config['sasl.username'],
        'sasl.password': config['sasl.password'],
        'group.id': datetime.now().strftime("%m/%d/%Y  %H:%M:%S"),
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                data_key = msg.key()
                data_value = msg.value()
                if(data_key is not None and data_value is not None):
                    
                    #convert bytes to json
                    data_date = data_key.decode('utf8')
                    data = data_value.decode('utf8')
                    
                    #create a file with today's date as the name 
                    date_string = datetime.now().strftime("%m.%d.%Y")
                    file_name = "./data/" + date_string + ".txt" 
                    
                    #open file and append breadcrumbs to it
                    FILE = open(file_name,"a")
                
                    FILE.write(data)
                    FILE.write("\n")
                    FILE.close()
                
                
                #increase breadcrumb count
                total_count += 1
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(data_key, data_value, total_count))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
