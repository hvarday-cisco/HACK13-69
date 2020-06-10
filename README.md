Tasks -

    1. Producers
        - 3 Queues, push random number of messages to the queues
            - 5 messages per second to be pushed initially

    2. Ochestrator
        - Priority Queues (for time being Dict)     [ PREFETCH MESSAGES ]
            - In future, we'll enhance depending on Queue Length

        - Consumer which consumes from all the 3 Queues taking into consideration threshold of each
            queue, accumulate all the messages and push it to common bus using a producer.

    3. Common bus
        - Depending on the number of messages in the queue, it will spawn another worker (ASG)

    4. Common bus consumer
        - Normal consumer

____________________________________________________________________________________________________

Producer -

     > python main.py

Orchestrator -

    > python orchestrator.py

Celery Task -

    > celery worker -c 1 -A tasks -Q sample_task --loglevel=info

Queues -

    Org Specific Queues

        queue_org_1
        queue_org_2
        queue_org_3

    Common Bus

        sample_task
____________________________________________________________________________________________________
