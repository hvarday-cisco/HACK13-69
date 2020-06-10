from tasks import sample_task
import time

def produce_task(org_name):
    queue_name="queue_%s" %org_name
    sample_task.s(value=org_name).apply_async(queue=queue_name, serializer='json')

while True:

    for j in range(500):

        for i in range(3):
            produce_task("org_%s" %(i+1))

    time.sleep(1)