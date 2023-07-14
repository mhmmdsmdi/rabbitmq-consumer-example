#consumer
import pika, sys, os
import json
import itertools

HOST = ''
UserName = ''
Password = ''
QueueName = ''

def main():
    credentials = pika.PlainCredentials(UserName,Password)
    counter = itertools.count(start=1)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST, port='5672', credentials= credentials))
    channel = connection.channel()
    channel.exchange_declare(QueueName, durable=True, exchange_type='topic')

    def callbackFunctionForQueueA(ch,method,properties,body):
        print(body)

    channel.basic_consume(queue=QueueName, on_message_callback=callbackFunctionForQueueA, auto_ack=True)
    #this will be command for starting the consumer session
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)