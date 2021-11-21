import sys
import argparse
import signal
import asyncio
import websockets
import cbor2
import ssl

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusPaths
from hydrus.core import HydrusData
from hydrus.core import HydrusSerialisable
from hydrus.client import ClientController
from hydrus.client.db import ClientDB
from hydrus.client.importing import ClientImportFiles
from hydrus.client.importing.options import FileImportOptions
from hydrus.client import ClientSearch
from hydrus.client import ClientConstants as CC

controller = None
access_key = None

class FakeFrameSplash(object):
    def SetText(self, msg):
        HydrusData.DebugPrint(f"[WEBSOCKET API] Splash text: {msg}")

    def SetSubtext(self, msg):
        HydrusData.DebugPrint(f"[WEBSOCKET API] Splash subtext: {msg}")

    def SetTitleText(self, msg):
        HydrusData.DebugPrint(f"[WEBSOCKET API] Splash title text: {msg}")

def boot_hydrus(db_path):
    global controller

    HydrusData.DebugPrint(f'[WEBSOCKET API] Initializing Hydrus (db path: {db_path})...')

    HydrusPaths.MakeSureDirectoryExists(db_path)

    HG.no_daemons = True
    HG.db_journal_mode = 'WAL'
    HG.db_cache_size = 200
    HG.db_transaction_commit_period = 30
    HG.db_synchronous = 1

    ClientDB.BlockingSafeShowMessage = HydrusData.DebugPrint

    controller = ClientController.Controller(db_path)
    controller.frame_splash_status = FakeFrameSplash()
    controller.SafeShowCriticalMessage = lambda title, message: HydrusData.DebugPrint(f'[WEBSOCKET API] [CRITICAL] {title}: {message}')

    # The client pubsub system is tied into the Qt event loop
    # But now there is no QApplication instance or event loop, work around it
    controller.pub = controller._pubsub.pubimmediate

    # This is worrying
    def empty_func(*args, **kwargs):
        pass
    controller.CallBlockingToQt = empty_func

    controller.CheckAlreadyRunning()
    controller.RecordRunningStart()
    controller.InitModel()

    HydrusData.DebugPrint("[WEBSOCKET API] Hydrus successfully initialized")

def shutdown_hydrus():
    if controller:
        controller.ShutdownModel()
        controller.CleanRunningFile()
    sys.exit(0)

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

async def websocket_handler(websocket):
    try:
        while True:
            message = await websocket.recv()
            message_decoded = cbor2.loads(message)
            if not message_decoded: continue

            if access_key and not message_decoded[0] == access_key:
                HydrusData.DebugPrint('[WEBSOCKET API] Request rejected due to mismatched access key')
                continue
            if access_key:
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
    except websockets.exceptions.ConnectionClosedOK:
        HydrusData.DebugPrint("[WEBSOCKET API] Websocket connection closed (OK)")

async def websocket_main(args):
    ssl_context = None
    if args.certfile:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(args.certfile, keyfile=args.keyfile)
    async with websockets.serve(websocket_handler, args.address, args.port, ssl=ssl_context):
        await asyncio.Future()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('db-path', metavar='path', type=str, help='database path')
    parser.add_argument('--address', metavar='addr', type=str, default="", help='listening address')
    parser.add_argument('--port', metavar='num', type=int, default=47341, help='listening port')
    parser.add_argument('--certfile', metavar='file', type=str, default=None, help='SSL cert file')
    parser.add_argument('--keyfile', metavar='file', type=str, default=None, help='SSL key file')
    parser.add_argument('--access-key', metavar='key', type=str, default=None, help='Access key')
    args = parser.parse_args()
    access_key = args.access_key

    signal.signal(signal.SIGINT, lambda sig, frame: shutdown_hydrus())
    signal.signal(signal.SIGTERM, lambda sig, frame: shutdown_hydrus())

    boot_hydrus(getattr(args, 'db-path'))

    HydrusData.DebugPrint(f"[WEBSOCKET API] Starting websocket server on port {args.port}")
    asyncio.run(websocket_main(args))

    shutdown_hydrus()
