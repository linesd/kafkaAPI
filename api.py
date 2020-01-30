from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from helpers import base64_encode_image, base64_decode_image, process_image
import pprint

class KafkaAPI():
    def __init__(self, server = 'localhost:9092', group_id = 'mygroup', client_id= 'client-1'):
        self.producer_conf = {
            'bootstrap.servers': server
        }
        self.consumer_conf = {
            'bootstrap.servers': server,
            'group.id': group_id,
            'client.id': client_id,
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        self.admin_client = AdminClient(self.producer_conf)

    def create_topic(self, new_topics=[], num_partitions = 1, rep_factor = 1):

        # topics to add
        topics = [NewTopic(topic, num_partitions=num_partitions, replication_factor=rep_factor)
                  for topic in new_topics]
        # Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

        # Call create_topics to asynchronously create topics. A dict
        # of <topic,future> is returned.
        fs = self.admin_client.create_topics(topics)

        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

    def delete_topics(self, topic):

        fs = self.admin_client.delete_topics([topic], operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))

    def list_topics(self):
        pprint.pprint(self.admin_client.list_topics().topics)

    def send_command(self, topic, string):
        self._produce(topic, string)

    def receive_command(self, topic):
        return self._consume(topic)

    def send_img(self, topic, img):
        i = process_image(img)
        i = base64_encode_image(i)
        self._produce(topic, i)

    def receive_img(self, topic, dtype, shape):
        img =  self._consume(topic, is_img=True)
        return base64_decode_image(img, dtype=dtype, shape=shape)

    def _produce(self, topic, message):
        p = Producer(self.producer_conf)
        p.produce(topic, '{0}'.format(message), callback=self._acked)
        p.poll(0.5)

    def _consume(self, topic, is_img = False):
        c = Consumer(self.consumer_conf)
        c.subscribe([topic])

        try:
            while True:
                msg = c.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    if is_img:
                        print('Image received')
                    else:
                        print('Received message: {0}'.format(msg.value()))
                    break
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached {0}/{1}'
                          .format(msg.topic(), msg.partition()))
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            c.close()

        return msg.value()

    def _acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: {0}: {1}"
                  .format(msg.value(), err.str()))
        else:
            print("Message produced: {0}".format(msg.value()))
