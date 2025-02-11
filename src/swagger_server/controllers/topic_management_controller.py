import connexion
import six

from swagger_server import util
from ksql import KSQLAPI

from base64 import b64decode
import json

import os

######### VICOM #########
import sys
import requests
#import dataflow_catalogue_controller
#########################
import sqlalchemy as db
import json

from swagger_server.controllers.dataflow_catalogue_controller import get_data_flows, increase_interested_parties_counter, decrease_interested_parties_counter

db_ip = os.environ["DB_IP"]
db_port = os.environ["DB_PORT"]
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_name = os.environ["DB_NAME"]

###ONLY FOP PIPELINES###
test_mode = os.environ.get("TEST_MODE", "false").lower() == "true"
########################

engine = db.create_engine('mysql+pymysql://'+db_user+':'+db_password+'@'+db_ip+':'+db_port+'/'+db_name, isolation_level="READ UNCOMMITTED")
# connection = engine.connect()
metadata = db.MetaData()
dataflows = db.Table('dataflows', metadata, autoload=True, autoload_with=engine)
topics = db.Table('topics', metadata, autoload=True, autoload_with=engine)

platform_address = os.environ["KAFKA_IP"]
plaform_port = os.environ["KAFKA_PORT"]
client = KSQLAPI("http://"+platform_address+":"+plaform_port, timeout=300)

def create_topic(instance_type, data_type, data_sub_type=None, data_format=None, country=None, quadkey=None, source_id=None, source_type=None, license_type=None, license_geo_limit=None, extra_parameters=None):  # noqa: E501
    """catalogue a Kafka topic for the user and returns the topic

     # noqa: E501

    :param data_type: Data type of the flow
    :type data_type: str
    :param x_user_id: 
    :type x_user_id: 
    :param data_sub_type: Data subType of the flow
    :type data_sub_type: str
    :param data_format: Data format of the flow
    :type data_format: str
    :param country: Source&#x27;s country
    :type country: str
    :param quadkey: Source&#x27;s quadkey
    :type quadkey: str
    :param source_id: Source&#x27;s Id
    :type source_id: int
    :param source_type: Source&#x27;s type (vehicle or infrastructure)
    :type source_type: str
    :param license_type: Type of license (use the license API to get the available values)
    :type license_type: str
    :param license_geo_limit: Geographical limit in the license (use the license API to get the available values)
    :type license_geo_limit: str
    :param extra_parameters: 
    :type extra_parameters: Dict[str, str]

    :rtype: str
    """
    connection_local = engine.connect()

    ######### VICOM #########

    if(not test_mode): 
        try:
            mec_ids = get_mec_ids(quadkey) # Get MEC IDs from discovery
        except Exception as err:
            print(f"{err}")
            return "Error getting MEC IDs.", 405

        if not mec_ids:
            return "No serving MECs in the selected tile or invalid tile.", 405

        mec_ids_string = "" # Create a string with all the MECs for the selected tile, used in the topic naming

        for mec_id in mec_ids:
            mec_ids_string += "_" + str(mec_id)

    #########################

    x_user_id = getUserNameFromHeader(connexion.request.headers['X-Userinfo'])
    if data_type != 'event': # UPLOAD DATAFLOW

        stream_name = (data_type + "-" + instance_type).upper()
        topic_name = data_type + "-" + instance_type

        if not stream_exists(stream_name): # Create the stream for the base topic <datatype>-<instance_type> comming from the connector
            try:
                create_stream_from_topic(stream_name, topic_name)
            except Exception as err:
                print(f"{err}")
                return "Invalid dataType or instance_type.", 405

        inc = 0

        if(test_mode):
            mec_ids_string = ""

        dest_stream = x_user_id.upper() + '_' + str(len(topics_by_user())+1000+inc) + '_' + data_type.upper() + '_' + instance_type.upper() + mec_ids_string
        #dest_stream = x_user_id.upper() + '_' + data_type.upper() + '_' + str(len(topics_by_user())+1000+inc)
        while stream_exists(dest_stream): # Check if the topic (stream) to be created already exists
            inc += 1
            dest_stream = x_user_id.upper() + '_' + str(len(topics_by_user())+1000+inc) + '_' + data_type.upper() + '_' + instance_type.upper() + mec_ids_string
            #dest_stream = x_user_id.upper() + '_' + data_type.upper() + '_' + str(len(topics_by_user())+1000+inc)

        # Query the stream previously created filtering the third party selected values
        query = 'CREATE STREAM "' + dest_stream + '" AS SELECT * \n' \
            'FROM "' + stream_name + '" ' 

        first = True
        if (data_sub_type is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['dataSubType']='" + data_sub_type + "' \n"
            first = False
        if (data_format is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['dataFormat']='" + data_format + "' \n"
            first = False
        if (country is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['locationCountry']='" + country + "' \n"
            first = False
        if (quadkey is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['locationQuadkey'] LIKE '" + quadkey + "%' \n"
            first = False
        if (source_id is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['sourceId']='" + str(source_id) + "' \n"
            first = False
        if (source_type is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['sourceType']='" + source_type + "' \n"
            first = False
        if (license_type is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['licenseType']='" + str(license_type) + "' \n"
            first = False
        if (license_geo_limit is not None):
            query += 'WHERE ' if first else 'AND '
            query += "`PROPERTIES`['licenseGeolimit']='" + license_geo_limit + "' \n"
            first = False
        if (extra_parameters is not None):
            for key, value in extra_parameters.items():
                pieces = key.split(",")
                if len(pieces) == 1:
                    query += 'WHERE ' if first else 'AND '
                    query += "`PROPERTIES`['"+key+"']='" + value + "' \n"
                    first = False
                else:
                    if(pieces[1] == "max"):
                        pieces[1] = '<='
                    else: 
                        if(pieces[1] == 'min'):
                            pieces[1] = '>='
                        else:
                            pieces[1] = '='
                    query += 'WHERE ' if first else 'AND '
                    query += "`PROPERTIES`['"+pieces[0]+"']"+pieces[1]+"'" + value + "' \n"
                    first = False

        try:
            client.ksql(query)
        except Exception as err:
            print(f"{err}")
            return "Invalid dataType or instance_type.", 405
    else: # DOWNLOAD DATAFLOW
        topic_name = data_type

        inc = 0

        if(test_mode):
            mec_ids_string = ""

        dest_stream = x_user_id.upper() + '_' + str(len(topics_by_user())+1000+inc) + '_' + 'EVENT' + mec_ids_string
        while stream_exists(dest_stream):
            inc += 1
            dest_stream = x_user_id.upper() + '_' + str(len(topics_by_user())+1000+inc) + '_' + 'EVENT' + mec_ids_string

        try:
            create_stream_from_topic2(dest_stream, dest_stream) # Create a stream for the event topic created for the user
        except Exception as err:
            print(f"{err}")
            return "Invalid dataType or instance_type.", 405

        if not stream_exists(topic_name): # Query the stream previously created to move the data to the general event topic
            query = 'CREATE STREAM "' + dest_stream + '_2' + '" \n' \
                "WITH (kafka_topic='" + topic_name + "') \n" \
                'AS SELECT * FROM "' + dest_stream + '" '

            try:
                client.ksql(query)
            except Exception as err:
                print(f"{err}")
                return "Invalid dataType or instance_type.", 405

    ######### VICOM #########

    if(not test_mode): 
        for mec_id in mec_ids: # Create a connector for every MEC serving in the selected tile
            if data_type == 'event':
                connector_name = "event-" + str(mec_id)
            else:
                connector_name = data_type + "-" + instance_type + "-" + str(mec_id)

            if not connector_exists(connector_name):
                create_datatype_connector(data_type, instance_type, str(mec_id))

    #########################

    #Insert the information about the topic in the DB
    topic_json = {
        "topicName": str(dest_stream),
        "dataType": data_type,
        "dataSubType": data_sub_type,
        "dataFormat": data_format,
        "sourceId": source_id,
        "sourceType": source_type,
        "licenseType": license_type,
        "licenseGeolimit": license_geo_limit,
        "locationQuadkey": quadkey,
        "locationCountry": country,
        "extraAttributes": extra_parameters
    }
    query = db.sql.insert(topics).values(topic_json)
    connection_local.execute(query)

    #Increase the counter of each dataflow that was used by the topic
    l = get_data_flows(topic_json["dataType"], topic_json["dataSubType"], topic_json["dataFormat"], topic_json["locationCountry"], topic_json["locationQuadkey"], topic_json["sourceId"], topic_json["sourceType"], topic_json["licenseType"], topic_json["licenseGeolimit"], topic_json["extraAttributes"])
    for dataflowId in l:
        increase_interested_parties_counter(dataflowId)    

    return dest_stream


def delete_topic(topic_name):  # noqa: E501
    """Delete a topic for the user

     # noqa: E501

    :param topic_name: Name of the topic to delete
    :type topic_name: str
    :param x_user_id: 
    :type x_user_id: 

    :rtype: None
    """
    connection_local = engine.connect()

    if not topic_exists(topic_name):
        return {
            "error": "Topic not found"
        }, 404

    #x_user_id = getUserNameFromHeader(connexion.request.headers['X-Userinfo']).upper()

    # result = queryIdByTopicName(topic_name)
    # if 'status' in result:
    #     return result

    # client.ksql('terminate "' + result + '"') 
    #client.ksql('drop stream "'+ topic_name + '" delete topic')

    ######### VICOM #########
    
    if 'EVENT' in topic_name:
        client.ksql('drop stream "'+ topic_name + '_2"')
        client.ksql('drop stream "'+ topic_name + '" delete topic')

        topics_ = topics_list()

        topic_name_split = topic_name.split('_')
        topic_name_length = len(topic_name_split)

        for i in range(3, topic_name_length):
            mec_id = topic_name_split[i]
            if i == (topic_name_length-1):
                mec_string = '_' + topic_name_split[i]
            else:
                mec_string = '_' + topic_name_split[i] + '_'

            if sum(('EVENT' in topic) and (mec_string in topic) for topic in topics_) == 0:
                connector_name = 'event-' + mec_id

                client.ksql('drop connector "' + connector_name + '"')
    else:
        client.ksql('drop stream "'+ topic_name + '" delete topic')

        topics_ = topics_list()

        topic_name_split = topic_name.split('_')
        topic_name_length = len(topic_name_split)
        data_type = topic_name_split[2]
        instance_type = topic_name_split[3]

        generic_topic_name = data_type + "_" + instance_type

        for i in range(4, topic_name_length):
            mec_id = topic_name_split[i]
            if i == (topic_name_length-1):
                mec_string = '_' + topic_name_split[i]
            else:
                mec_string = '_' + topic_name_split[i] + '_'

            if sum((generic_topic_name in topic) and (mec_string in topic) for topic in topics_) == 0:
                connector_name = data_type.lower() + "-" + instance_type.lower() + "-" + mec_id

                client.ksql('drop connector "' + connector_name + '"')

    #dataflow_catalogue_controller.count_data_flows(data_type, quakey)

    #########################

    #Find information about the topic to delete
    query = db.select([topics]).where(topics.columns.topicName == topic_name)
    result = connection_local.execute(query).fetchone()
    
    #Reduce the counter of each dataflow that was used by the topic
    l = get_data_flows(result["dataType"], result["dataSubType"], result["dataFormat"], result["locationCountry"], result["locationQuadkey"], result["sourceId"], result["sourceType"], result["licenseType"], result["licenseGeolimit"], result["extraAttributes"])
    for dataflowId in l:
        decrease_interested_parties_counter(dataflowId)

    #Delete the topic from the DB
    query = db.sql.delete(topics).where(topics.columns.topicName == topic_name)
    connection_local.execute(query)

    return {
        "status": 200
    }


def find_query_by_topic_name(topic_name):  # noqa: E501
    """Returns the query that generated the topic

     # noqa: E501

    :param topic_name: Name of the topic
    :type topic_name: str
    :param x_user_id: 
    :type x_user_id: 

    :rtype: str
    """

    x_user_id = getUserNameFromHeader(connexion.request.headers['X-Userinfo']).upper()

    if not topic_name.startswith(x_user_id+"_"):
        return {
            "error": "The topic does not belong to the user"
        }, 400

    arr_res = client.ksql('show queries')
    l = []
    for j in arr_res:
        for i in j['queries']:
            if i['sinkKafkaTopics'][0].startswith(x_user_id+"_") and i['sinkKafkaTopics'][0] == topic_name:
                try:
                    return {
                        "topic": i['sinkKafkaTopics'][0],
                        "query": i['queryString'].split("WHERE",1)[1].split("EMIT", 1)[0].strip()
                    }
                except:
                    return {
                        "topic": i['sinkKafkaTopics'][0],
                        "query": ""
                    }
    return {
        "error": "Topic not found"
    }, 404


def topics_by_user():  # noqa: E501
    """Returns the topics associated with that user

     # noqa: E501

    :param x_user_id: 
    :type x_user_id: 

    :rtype: List[str]
    """

    x_user_id = getUserNameFromHeader(connexion.request.headers['X-Userinfo']).upper()

    arr_res = client.ksql('show queries')
    l = []
    for j in arr_res:
        for i in j['queries']:
            if i['sinkKafkaTopics'][0].startswith(x_user_id+"_"):
                l.append(i['sinkKafkaTopics'][0])
    return l


##################################
######        UTILS         ######
##################################

def getUserNameFromHeader(header):
    header_decode = b64decode(header)
    header_json = json.loads(header_decode)
    return header_json["preferred_username"]

def queryIdByTopicName(topic_name):
    x_user_id = getUserNameFromHeader(connexion.request.headers['X-Userinfo']).upper()

    if not topic_name.startswith(x_user_id+"_"):
        return {
            "error": "The topic does not belong to the user"
        }, 400

    arr_res = client.ksql('show queries')
    l = []
    for j in arr_res:
        for i in j['queries']:
            if i['sinkKafkaTopics'][0].startswith(x_user_id+"_") and i['sinkKafkaTopics'][0] == topic_name:
                return i['id']
    return {
        "error": "Topic not found"
    }, 404

# AVRO schema for topic need to created before running this function
def create_stream_from_topic(stream_name, topic_name):
    create_stream = 'CREATE STREAM "' + stream_name + '" \n' \
                    "WITH (kafka_topic='" + topic_name + "', partitions=1, value_format='AVRO');" # partitions property added so that the topic is automatically created
    client.ksql( create_stream )


# Uses an existing AVRO schema, specified with the ID
def create_stream_from_topic2(stream_name, topic_name):
    create_stream = 'CREATE STREAM "' + stream_name + '" \n' \
                    "WITH (kafka_topic='" + topic_name + "', partitions=1, value_format='AVRO', value_schema_id=1);" # value_schema_id to bypass schema not found error
    client.ksql( create_stream )

def stream_exists(stream_name):
    if(stream_name in streams_list()):    
        return True
    return False

def streams_list():
    arr_res = client.ksql('show streams')
    streams = next(item for item in arr_res if item['streams']!="")['streams']
    return list(map(lambda stream: stream['name'], streams))

##################################
######        VICOM         ######
##################################

def topics_list():
    arr_res = client.ksql('show topics')
    topics = next(item for item in arr_res if item['topics']!="")['topics']
    return list(map(lambda topic: topic['name'], topics))

def topic_exists(topic_name):
    if(topic_name in topics_list()):    
        return True
    return False

def connectors_list():
    arr_res = client.ksql('show connectors')
    connectors = next(item for item in arr_res if item['connectors']!="")['connectors']
    return list(map(lambda connector: connector['name'], connectors))

def connector_exists(connector_name):
    if(connector_name in connectors_list()):    
        return True
    return False

def create_datatype_connector(data_type, instance_type, mec_id):
    activemq_username = 5gmeta-platform
    activemq_password = 5gmeta-platform
    activemq_ip, activemq_port = get_messagebroker_info(mec_id) # type: ignore
    activemq_url = "tcp://" + activemq_ip + ":" + activemq_port

    if data_type == "event":
        connector_name = data_type + "-" + mec_id
        connector_type = "Sink"
        kafka_topic = "event"
        query = "'connect.jms.kcql'= 'INSERT INTO " + kafka_topic + " SELECT * FROM " + kafka_topic + " WITHTYPE TOPIC WITHFORMAT JSON',\n" \
        "'topics'= '" + kafka_topic +"'\n"
    else:
        connector_name = data_type + "-" + instance_type + "-" + mec_id
        connector_type = "Source"
        kafka_topic = data_type + "-" + instance_type
        query = "'connect.jms.kcql'= 'INSERT INTO " + kafka_topic + " SELECT * FROM " + kafka_topic + " WITHTYPE TOPIC '\n"

    create_connector = "CREATE " + connector_type.upper() + " CONNECTOR `" + connector_name + "` WITH (\n" \
        "'name'= '" + connector_name + "',\n" \
        "'tasks.max'= '1',\n" \
        "'connector.class'= 'com.datamountaineer.streamreactor.connect.jms." + connector_type.lower() + ".JMS" + connector_type + "Connector',\n" \
        "'connect.progress.enabled'= 'true',\n" \
        "'connect.jms.initial.context.factory'= 'org.apache.activemq.jndi.ActiveMQInitialContextFactory',\n" \
        "'connect.jms.connection.factory'= 'ConnectionFactory',\n" \
        "'connect.jms.url'= '" + activemq_url + "',\n" \
        "'connect.jms.username'= '" + activemq_username + "', \n" \
        "'connect.jms.password'= '" + activemq_password + "',\n" \
        + query + ");"

    client.ksql(create_connector)

#discovery_url = "https://5gmeta-platform.eu/discovery-api"
discovery_url = "http://discovery-api.cloud-platform.svc.cluster.local:8080/discovery-api"

def get_mec_ids(quadkey):
    r = requests.get(discovery_url + "/mec/tile/" + quadkey)
    json_response = r.json()

    mec_ids = [mec['id'] for mec in json_response]

    return mec_ids

def get_messagebroker_info(mec_id):
    r = requests.get(discovery_url + "/mec/" + mec_id + "/nbservices")
    json_response = r.json()

    for service in json_response:
        if service['service_name'] == 'message-broker':
            messagebroker_ip = str(service['ip'])
            messagebroker_port = str(service['port'])

            return messagebroker_ip, messagebroker_port
