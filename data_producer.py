from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import json
import threading
import requests
import uuid

# Function to fetch data from Random User Generator API
def get_random_user_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

# Function to format fetched data
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

# Function to periodically send data to Kafka
def my_periodic_function(data, producer, topic, start, interval):
    print(f"[LOG] SENDING DATA to BROKER to topic: {topic}!")
    try:
        for i in range(start, start + interval):
            producer.send(topic, json.dumps(data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        print(f"An error occurred while sending data: {e}")

# Kafka broker configuration
kafka_broker = "localhost:9092"
kafka_topic = "users_created"  # Replace with your Kafka topic
interval = 50

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_broker)

# Start timer to periodically fetch and send data
count = 0
def start_timer():
    global count
    threading.Timer(5, start_timer).start()
    print("Timer tick count:", count)
    random_user_data = get_random_user_data()
    formatted_data = format_data(random_user_data)
    my_periodic_function(formatted_data, producer, kafka_topic, count, interval)
    if count < 1000:
        count += interval
    else:
        return

start_timer()
