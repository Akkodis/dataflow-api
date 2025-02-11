import connexion
import six

from swagger_server.models.data_flow import DataFlow  # noqa: E501
from swagger_server.models.data_info import DataInfo
from swagger_server.models.data_type_info import DataTypeInfo
from swagger_server.models.data_source_info import DataSourceInfo
from swagger_server.models.license_info import LicenseInfo
from swagger_server.models.source_location_info import SourceLocationInfo

from swagger_server import util

import sqlalchemy as db
import json

import os

db_ip = os.environ["DB_IP"]
db_port = os.environ["DB_PORT"]
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_name = os.environ["DB_NAME"]

engine = db.create_engine('mysql+pymysql://'+db_user+':'+db_password+'@'+db_ip+':'+db_port+'/'+db_name, isolation_level="READ UNCOMMITTED")
# connection = engine.connect()
metadata = db.MetaData()
dataflows = db.Table('dataflows', metadata, autoload=True, autoload_with=engine)

def increase_interested_parties_counter(dataflowId):
    connection_local = engine.connect()

    query = db.select([dataflows.columns.counter]).where(dataflows.columns.dataflowId == dataflowId)
    c = connection_local.execute(query).fetchone()["counter"] + 1

    query = db.update(dataflows).values({"counter": c}).where(dataflows.columns.dataflowId == dataflowId)
    connection_local.execute(query)

def decrease_interested_parties_counter(dataflowId):
    connection_local = engine.connect()

    query = db.select([dataflows.columns.counter]).where(dataflows.columns.dataflowId == dataflowId)
    c = connection_local.execute(query).fetchone()["counter"] - 1

    query = db.update(dataflows).values({"counter": c}).where(dataflows.columns.dataflowId == dataflowId)
    connection_local.execute(query)

def get_data_flows(data_type, data_sub_type=None, data_format=None, country=None, quadkey=None, source_id=None, source_type=None, license_type=None, license_geo_limit=None, extra_parameters=None):  # noqa: E501
    """Returns the list of ids of DataFlows that match the query

     # noqa: E501

    :param data_type: Data type of the flow
    :type data_type: str
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

    :rtype: List[int]
    """
    connection_local = engine.connect()

    query = db.select([dataflows.columns.dataflowId]).where(dataflows.columns.dataType == data_type)
    if(data_sub_type):
        query = query.where(dataflows.columns.dataSubType == data_sub_type)
    if(data_format):
        query = query.where(dataflows.columns.dataFormat == data_format)
    if(country):
        query = query.where(dataflows.columns.locationCountry == country)
    if(quadkey):
        query = query.where(dataflows.columns.locationQuadkey.startswith(quadkey))
    if(source_id):
        query = query.where(dataflows.columns.sourceId == source_id)
    if(source_type):
        query = query.where(dataflows.columns.sourceType == source_type)
    if(license_type):
        query = query.where(dataflows.columns.licenseType == license_type)
    if(license_geo_limit):
        query = query.where(dataflows.columns.licenseGeolimit == license_geo_limit)


    for key, value in extra_parameters.items():
        pieces = key.split(",")
        print(db.func.json_extract(dataflows.columns.extraAttributes, '$.'+key))
        if len(pieces) == 1:
            if(value.isnumeric()):
                query = query.where(db.func.json_extract(dataflows.columns.extraAttributes, '$.'+key) == int(value))
            else:
                query = query.where(db.func.json_extract(dataflows.columns.extraAttributes, '$.'+key) == value)   
        else:
            key = pieces[0]
            if(pieces[1] == "max"):
                query = query.where(db.func.json_extract(dataflows.columns.extraAttributes, '$.'+key) <= int(value))
            if(pieces[1] == "min"):
                query = query.where(db.func.json_extract(dataflows.columns.extraAttributes, '$.'+key) >= int(value))

    result = connection_local.execute(query).fetchall()
    if (result is None):
        return {
            "dataflows": []
        }
    l = []
    for t in result:
        l.append(t["dataflowId"])
    return l

def count_data_flows(data_type, data_sub_type=None, data_format=None, country=None, quadkey=None, source_id=None, source_type=None, license_type=None, license_geo_limit=None, extra_parameters=None):  # noqa: E501
    """Returns the number of dataFlows that match the query

     # noqa: E501

    :param data_type: Data type of the flow
    :type data_type: str
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

    :rtype: int
    """
    return len(get_data_flows(data_type, data_sub_type, data_format, country, quadkey, source_id, source_type, license_type, license_geo_limit, extra_parameters))


def get_metadata(data_flow_id):  # noqa: E501
    """Returns the metadata of the choosen DataFlow

     # noqa: E501

    :param data_flow_id: Id of the dataFlow
    :type data_flow_id: int

    :rtype: DataFlow
    """
    connection_local = engine.connect()

    query = db.select([dataflows]).where(dataflows.columns.dataflowId == data_flow_id)
    result = connection_local.execute(query).fetchone()
    if (result is None):
        return {
            "error": "Item not found"
        }, 404
    dataTypeInfo = DataTypeInfo(result["dataType"], result["dataSubType"])
    dataInfo = DataInfo(result["dataflowDirection"], result["dataFormat"], result["dataSampleRate"], result["extraAttributes"]) 
    licenseInfo = LicenseInfo(result["licenseType"], result["licenseGeolimit"])
    sourceLocationInfo = SourceLocationInfo(result["locationCountry"], result["locationLatitude"], result["locationLongitude"], result["locationQuadkey"])
    dataSourceInfo = DataSourceInfo(result["sourceId"], result["sourceType"], result["timeRegistration"], result["timeLastUpdate"], result["timeZone"], result["timeStratumLevel"], sourceLocationInfo)

    return DataFlow(data_flow_id, dataTypeInfo, dataInfo, licenseInfo, dataSourceInfo, result['quality'])

def get_datatypes(quadkey):
    connection_local = engine.connect()

    query = db.select([dataflows.columns.dataType]).distinct().where(dataflows.columns.locationQuadkey.startswith(quadkey))
    result = connection_local.execute(query).fetchall()

    if (result is None):
        return []
    
    l = []
    for t in result:
        l.append(t["dataType"])
    return l

def get_possible_value(data_type):  # noqa: E501
    """Returns the possible values for each field for a specific dataType

     # noqa: E501

    :param data_type: Data type of the flow
    :type data_type: str

    :rtype: str
    """
    connection_local = engine.connect()

    result = {}
    for field in dataflows.columns:
        if(field in [dataflows.columns.timeLastUpdate, dataflows.columns.timeRegistration, dataflows.columns.dataflowId, dataflows.columns.dataType]):
            continue
        query = db.sql.select([field]).distinct().where(dataflows.columns.dataType == data_type)
        r = connection_local.execute(query).fetchall()
        l = []
        for rr in r:
            l.append(rr[field.key])
        result[field.key] = l

    return result