import urllib.request
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from datetime import datetime




if __name__ == '__main__':
    
    #server url
    url = 'http://rbi.ddns.net/getBreadCrumbData'
    with urllib.request.urlopen(url) as response:
        data_bytes = response.read().decode("utf-8")
    
    #create a list of dictionaries
    data_list = json.loads(data_bytes)
        
    #topic to broadcast to
    topic = "assignment1-sensor-reading"
    #configuration file
    config_file = "/home/aaldu2/producer/code/librdkafka.config"
    #read config file
    config = ccloud_lib.read_ccloud_config(config_file)


    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': config['bootstrap.servers'],
        'sasl.mechanisms': config['sasl.mechanisms'],
        'security.protocol': config['security.protocol'],
        'sasl.username': config['sasl.username'],
        'sasl.password': config['sasl.password'],
    })

    #delivered data-points
    delivered_data = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_data
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_data += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    

    for entry in data_list:
        data_key = datetime.now().strftime("%m,%d,%Y %H:%M:%S")
        data_value = json.dumps(entry)
        print("Producing record: {}\t{}\n".format(data_key, data_value))
        producer.produce(topic, key=data_key, value=data_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_data, topic))
