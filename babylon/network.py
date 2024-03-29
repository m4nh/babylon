import pika
import json
import time
import numpy as np
import zlib

class BMessage(object):
    PRIMITIVES_TYPES = ('str', 'float', 'int', 'bool')

    def __init__(self, sender='', receiver='', action='' ):
        self._sender = sender
        self._receiver = receiver
        self._action = str(action)
        self._timestamp = time.time()
        self._fields = {}
        self._payload = {}

    def getSender(self):
        return self._sender

    def getReceiver(self):
        return self._receiver

    def getAction(self):
        return self._action

    def addField(self, key, value):
        if isinstance(value, np.ndarray):
            self._fields[key] = "NPARRAY_{}_{}".format(value.dtype, value.shape)
            self._payload[key] = value.tolist()
        if type(value).__name__ in BMessage.PRIMITIVES_TYPES:
            self._fields[key] = str(type(value).__name__)
            self._payload[key] = value

    def getField(self, key):
        if key not in self._fields:
            return None
        tp = self._fields[key]
        if tp in BMessage.PRIMITIVES_TYPES:
            return self._payload[key]
        if 'NPARRAY' in tp:
            chunks = tp.split('_')
            nptype = 'np.{}'.format(chunks[1])
            size = chunks[2].replace('(','').replace(')','').split(',')
            try:
                arr = np.array(self._payload[key]).reshape(tuple(map(int, size)))
                return arr
            except:
                return np.array(self._payload[key])


    @staticmethod
    def fromStream(s):
        obj = json.loads(s)
        message = BMessage(sender=obj['sender'], receiver=obj['receiver'], action=obj['action'])
        message._timestamp = obj['timestamp']
        message._fields = obj['fields']
        message._payload = obj['payload']
        return message

    def toStream(self):
        msg = {
            'sender': self._sender,
            'receiver': self._receiver,
            'action': self._action,
            'timestamp': time.time(),
            'fields': self._fields,
            'payload': self._payload
        }
        return json.dumps(msg)


class SimpleChannel(object):

    def __init__(self, host='localhost', port=5672, queue_size = 1, topic_name='simple_channel'):
        self._host = host
        self._port = port
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host, port=self._port))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange='topic_logs', exchange_type='topic', arguments={ "x-max-length": queue_size })
        result = self._channel.queue_declare('', arguments={ "x-max-length": queue_size })
        self._queue_name = result.method.queue
        self._topic_name = topic_name
        self._channel.queue_bind(exchange='topic_logs', queue=self._queue_name, routing_key=topic_name)
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self._internalCallback, auto_ack=True)
        self._callbacks = []

    def _internalCallback(self, ch, method, properties, body):
        message = BMessage.fromStream(body)
        for cb in self._callbacks:
            cb(message)

    def publish(self, message: BMessage):
        self._channel.basic_publish(exchange='topic_logs', routing_key=self._topic_name, body=message.toStream())

    def close(self):
        self._connection.close()

    def addMessageCallback(self, cb):
        self._callbacks.append(cb)

    def infiniteLoop(self):
        self._channel.start_consuming()