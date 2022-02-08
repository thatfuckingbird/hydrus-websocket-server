# A basic example client + tests

import cbor2
import websockets
import asyncio

from . import schemas

# Need the code below for now for the Predicate data structure and CBOR encoding
# Won't be needed in the future when it's generated from schema
from hydrus.client import ClientSearch

class EmptyObject(object):
    pass
HG.client_controller = EmptyObject()
HG.client_controller.new_options = None
predicates = [ ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_NUM_TAGS, ( '', '<', 20 ) ) ]
location_search_context = ClientSearch.LocationSearchContext( current_service_keys = [ CC.LOCAL_FILE_SERVICE_KEY ] )
search_context = ClientSearch.FileSearchContext( location_search_context = location_search_context, predicates = predicates )

def cbor_encoder(encoder, value):
    encodable_value = None
    if not encodable_value and hasattr(value, 'GetSerialisableTuple'):
        ser_tup = value.GetSerialisableTuple()
        if ser_tup:
            encodable_value = ser_tup
    if not encodable_value and hasattr(value, 'ToTuple'):
        tup = value.ToTuple()
        if tup:
            encodable_value = tup
    if not encodable_value and hasattr(value, 'GetSerialisableDictionary'):
        ser_dict = value.GetSerialisableDictionary()
        if ser_dict:
            encodable_value = value.GetSerialisableDictionary()
    if not encodable_value:
        raise ValueError(f"Can't serialize this bitch {type(value)}")
    return encoder.encode(encodable_value)

# The actual client code

async def main():
    async with websockets.connect("ws://localhost:47341") as websocket:
        #await websocket.send(cbor2.dumps(['file_query_ids', search_context], default=cbor_encoder))
        await websocket.send(cbor2.dumps(['version'], default=cbor_encoder))
        reply = await websocket.recv()
        print(cbor2.loads(reply))

asyncio.run(main())

# TODO

"""
# Import a file
job = ClientImportFiles.FileImportJob("/file/path/here.jpg", FileImportOptions.FileImportOptions())
try:
    result = job.DoWork()
    print(result)
except:
    pass
"""
