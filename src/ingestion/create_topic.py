from confluent_kafka.admin import AdminClient, NewTopic 

admin_client = AdminClient({'bootstrap.servers':'localhost:9092'})

topic = NewTopic(
    "market-data", 
    num_partitions=6, 
    replication_factor=1
)
try:
    admin_client.create_topics([topic])
except Exception as e:
    raise e 

