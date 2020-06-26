import time

import pika

max_messages_fetch = 1000

org_threshold = {
    "queue_org_1": 1000,
    "queue_org_2": 1000,
    "queue_org_3": 1000,
}

queue_priority = {
    "queue_org_1" : 2,
    "queue_org_2" : 1,
    "queue_org_3" : 1,
}

queue_messages_fetch = {
    "queue_org_1": 10,
    "queue_org_2": 5,
    "queue_org_3": 5
}


def queue_length(connection, q_name='sample_task'):

    try:

        channel = connection.channel()
        queue = channel.queue_declare(
            queue=q_name, durable=True,
            exclusive=False, auto_delete=False
        )
        return (queue.method.message_count)
    except Exception as e:
        print("Exception - %s" %e)
        return 0

q_name='sample_task'

# Make this list Dynamic
queues = ["queue_org_1", "queue_org_2", "queue_org_3"]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_qos(prefetch_count= max_messages_fetch * 3 )

queue = channel.queue_declare(
    queue=q_name, durable=True,
    exclusive=False, auto_delete=False
)

global messages
messages = []

def monitor_org_queues():

    for queue_name in queues:

        count = queue_length(connection, queue_name)

        if count > org_threshold[queue_name] and queue_messages_fetch[queue_name]+10 < max_messages_fetch:
            print ("Changing messages prefetch Q - %s, From: %s - To: %s " %(queue_name, queue_messages_fetch[queue_name], queue_messages_fetch[queue_name]+10))
            queue_messages_fetch[queue_name] += 50


# Starvation -
#
#     Org1 - 50
#     Org2 - 50
#     Org3 - 10000


def pull_message_from_queue(q_name):
    global messages
    counter = 0
    threshold = queue_messages_fetch[q_name]

    for method, properties, body in channel.consume(q_name):
        counter += 1
        messages.append( (body, properties, method.delivery_tag) )
        if counter == threshold:
            channel.cancel()
            break

def ack_message(tag):
    channel.basic_ack(tag)


def move_to_common_bus():
    global messages
    for (body, properties, tag) in messages:
        channel.publish(body=body, properties=properties, exchange='', routing_key='sample_task')
        ack_message(tag)
    print("Moved %s messages to common bus" %len(messages))
    messages = []

def move_messages_to_common_bus():
    for queue_name in queues:
        pull_message_from_queue(queue_name)

    move_to_common_bus()

while True:
    monitor_org_queues()
    move_messages_to_common_bus()
    time.sleep(1)









# counter = 0
# for method, properties, body in channel.consume(q_name):
#     counter +=1
#     print (counter, "\n\n", method, "\n\n", properties, "\n\n", body)
#     channel.basic_ack(method.delivery_tag)  # ACK
#
#







    # time.sleep(2)

    # 5421
    #
    # <Basic.Deliver(['consumer_tag=ctag1.bf99ab5e73654574b47209056a769cf8', 'delivery_tag=5421', 'exchange=', 'redelivered=False', 'routing_key=sample_task']) >,
    #
    # <BasicProperties(['content_encoding=utf-8', 'content_type=application/json','correlation_id=37c72d14-cebc-4928-9c5f-f1daaa4acaee', 'delivery_mode=2','headers={\'origin\': u\'gen80688@HVARDAY-M-426J\', \'lang\': u\'py\', \'task\': u\'tasks.sample_task\', \'group\': None, \'root_id\': u\'cccf5298-02ec-4d77-bf08-2f65a3f16656\', \'expires\': None, \'retries\': 0, \'timelimit\': [None, None], \'argsrepr\': u\'()\', \'eta\': None, \'parent_id\': u\'cccf5298-02ec-4d77-bf08-2f65a3f16656\', \'shadow\': None, \'id\': u\'37c72d14-cebc-4928-9c5f-f1daaa4acaee\', \'kwargsrepr\': u"{\'value\': \'org_3\'}"}','reply_to=b968aa25-15fe-3151-85f9-4d3e2416fd4f']) >,
    #                                                                                                                                                                          '\n\n',
    # '[[], {"value": "org_3"}, {"chord": null, "callbacks": null, "errbacks": null, "chain": null}]')
