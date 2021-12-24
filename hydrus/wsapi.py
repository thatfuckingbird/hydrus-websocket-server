import asyncio
import websockets
import ssl
import cbor2
import functools

from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusSerialisable

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

simple_read_commands = set(['boned_stats', 'services', 'options'])
simple_write_commands = set(['regenerate_tag_parents_cache'])

async def websocket_handler(config, websocket):
    try:
        while True:
            message = await websocket.recv()
            message_decoded = cbor2.loads(message)
            if not message_decoded: continue

            if config.access_key and not message_decoded[0] == config.access_key:
                HydrusData.DebugPrint('[WEBSOCKET API] Request rejected due to mismatched access key')
                continue
            if config.access_key:
                message_decoded = message_decoded[1:]
                if not message_decoded: continue

            command = message_decoded[0]
            if command in simple_read_commands:
                await websocket.send(cbor2.dumps(HG.client_controller.Read(command), default=cbor_encoder))
            elif command in simple_write_commands:
                await websocket.send(cbor2.dumps(HG.client_controller.Write(command)))
            elif command == 'file_query_ids':
                result = HG.client_controller.Read('file_query_ids', HydrusSerialisable.CreateFromSerialisableTuple(message_decoded[1]), apply_implicit_limit = False)
                await websocket.send(cbor2.dumps(result))
            elif command == 'version':
                await websocket.send(cbor2.dumps(HC.SOFTWARE_VERSION))
    except websockets.exceptions.ConnectionClosedOK:
        HydrusData.DebugPrint("[WEBSOCKET API] Websocket connection closed (OK)")

async def websocket_main(config):
    HydrusData.DebugPrint('[WEBSOCKET API] WebSocket API starting')
    ssl_context = None
    if config.certfile:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(config.certfile, keyfile=config.keyfile)
    async with websockets.serve(functools.partial(websocket_handler, config), config.address, config.port, ssl=ssl_context):
        await asyncio.Future()
