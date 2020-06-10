from celery import Celery
from kombu import Exchange, Queue
from multiprocessing import Manager
from multiprocessing import Queue as mpQueue

import time

# Celery Config

celery_app = Celery()
celeryconfig = {}
celeryconfig['BROKER_URL'] = 'amqp://'
# celeryconfig['CELERY_RESULT_BACKEND'] = 'redis://localhost'
celeryconfig['CELERY_QUEUES'] = (
    Queue('sample_task', routing_key='sample_task',
          # queue_arguments={'x-max-priority': 10}
          ),
)
celeryconfig['CELERY_ACKS_LATE'] = True
celeryconfig['CELERYD_PREFETCH_MULTIPLIER'] = 1
celery_app.config_from_object(celeryconfig)

# celery worker -c 1 -A tasks -Q tasks --loglevel=info
# ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill
# amqp://jm-user1:sample@localhost/jm-vhost

# rabbitmqctl stop
# rabbitmq-server -detached

###

# Idea of the sample project is to simulate transcoding of a video into
# different dimensions using celery priority task queues, and actually
# test if it is priority based.

# example: transcode_360p.apply_async(queue='tasks', priority=1)

###
queue_threshold = {
    "queue_org_1": 10,
    "queue_org_2": 5,
    "queue_org_3": 5
}

queue_priority = {
    "queue_org_1" : 2,
    "queue_org_2" : 1,
    "queue_org_3" : 1,
}

# queue_counter = Manager().dict()
# value_queue = mpQueue()

@celery_app.task
def sample_task(value):
    # time.sleep(1)
    print("Value is %s" %value)

# def reset():
#     print("Resetting global variables")
#     global queue_counter
#     queue_counter.clear()
#
# def push_to_common_bus():
#     # from celery.contrib import rdb; rdb.set_trace()
#     print("Pushing to common bus")
#     while not value_queue.empty():
#         value = value_queue.get()
#         sample_task.s(value=value).apply_async(queue='sample_task')
#     reset()

# def monitor_queues():



# @celery_app.task
# def orchestrator_task(values):
#
#     queue_name, value = values
#     global queue_counter
#     global value_queue
#
#     queue_counter[queue_name] = queue_counter.get(queue_name, 0) + 1
#     queue_counter['total'] = queue_counter.get('total', 0) + 1
#     value_queue.put(value)
#
#     print ("Counter - %s " %queue_counter)
#     if queue_counter[queue_name] == queue_threshold[queue_name]:
#         push_to_common_bus()