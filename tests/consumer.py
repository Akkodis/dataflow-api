import time
import requests
from kafka_schema_registry import prepare_producer
from ksql import KSQLAPI

platform_address = "localhost"
dataflowapi_port = "12345"
platform_port = "8088"

client = KSQLAPI("http://"+platform_address+":"+platform_port, timeout=300)

def stream_exists(stream_name):
    if(stream_name in streams_list()):    
        return True
    return False

def streams_list():
    arr_res = client.ksql('show streams')
    streams = next(item for item in arr_res if item['streams']!="")['streams']
    return list(map(lambda stream: stream['name'], streams))

SAMPLE_SCHEMA = {
    "type": "record",
    "name": "TestType",
    "fields" : [
        {"name": "age", "type": "int"},
        {"name": "name", "type": ["null", "string"]},
        {"name": "properties",
                "type": [
                    "null",
                    {
                        "type": "map",
                        "values": [
                            "null",
                            "string"
                        ]
                    }
                ],
                "default": None}
    ]
}


producer = prepare_producer(
    ['localhost:9092'],
    f'http://localhost:8081',
    'cits-small',
    1,
    1,
    value_schema=SAMPLE_SCHEMA,
)

# Url to request a topic containing cits messages with subtype cam and with in the quadkey 123012301230123
url = "http://"+platform_address+':'+dataflowapi_port+'/dataflow-api/topics/cits/query?dataSubType=denm&instance_type=small&quadkey=12022301011101&licenseGeoLimit=europe&licenseType=profit'

# This header should NOT be added by the application, but by APISIX  
headers = {
    "X-Userinfo": "eyJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwianRpIjoiNTIxMjYzZTQtNjMxYy00MjMzLTlkMjktNGMxMzk1NGJlNzhmIiwic2lkIjoiZTMzMGNmNTctMTVlYy00Njc0LTljOTQtMmVkNGY4YzUwNzM5Iiwic3ViIjoiYTQ1YjhiNDYtMzg5NC00YTIxLThkZGYtZWY5NDQxMmNmMGFjIiwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwicHJlZmVycmVkX3VzZXJuYW1lIjoidXNlcjEiLCJhenAiOiI1Z21ldGFfbG9naW4iLCJzZXNzaW9uX3N0YXRlIjoiZTMzMGNmNTctMTVlYy00Njc0LTljOTQtMmVkNGY4YzUwNzM5IiwiYWN0aXZlIjp0cnVlLCJ0eXAiOiJCZWFyZXIiLCJ1c2VybmFtZSI6InVzZXIxIiwiZXhwIjoxNjQ5OTI1MTI5LCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImFjciI6IjEiLCJpc3MiOiJodHRwczpcL1wvaWRlbnRpdHktNWdtZXRhLndlc3RldXJvcGUuY2xvdWRhcHAuYXp1cmUuY29tOjg0NDNcL3JlYWxtc1wvNWdtZXRhIiwiYXVkIjoiYWNjb3VudCIsImlhdCI6MTY0OTkyNDgyOSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iLCJkZWZhdWx0LXJvbGVzLTVnbWV0YSJdfSwiY2xpZW50X2lkIjoiNWdtZXRhX2xvZ2luIn0="
}

# The request returns the generated topic
r = requests.post(url, headers=headers)
if r.status_code != 200 or r.text != "USER1_1000_CITS_SMALL":
    print("Error in topic creation.")
    print(r.text)
    exit(1)
topic = r.text

if(not stream_exists("USER1_1000_CITS_SMALL")):
    print("Error in kafka creation.")
    exit(1)

print("Topic created.")
time.sleep(10)

r = requests.delete("http://"+platform_address+':'+dataflowapi_port+'/dataflow-api/topics/'+topic, headers=headers)
if r.status_code != 200:
    print("Error in topic deletion.")
    print(r.text)
    exit(1)

if(stream_exists("USER1_1000_CITS_SMALL")):
    print("Error in kafka deletion.")
    exit(1)

print("Topic deleted.")

print("Tests completed.")
exit(0)
