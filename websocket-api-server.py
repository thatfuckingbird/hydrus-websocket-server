import sys
import argparse
import signal
import asyncio

from hydrus import wsapi
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusPaths
from hydrus.core import HydrusData
from hydrus.client import ClientController
from hydrus.client.db import ClientDB

controller = None

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('db-path', metavar='path', type=str, help='database path')
    parser.add_argument('--address', metavar='addr', type=str, default="", help='listening address')
    parser.add_argument('--port', metavar='num', type=int, default=47341, help='listening port')
    parser.add_argument('--certfile', metavar='file', type=str, default=None, help='SSL cert file')
    parser.add_argument('--keyfile', metavar='file', type=str, default=None, help='SSL key file')
    parser.add_argument('--access-key', metavar='key', type=str, default=None, help='access key')
    args = parser.parse_args()

    signal.signal(signal.SIGINT, lambda sig, frame: shutdown_hydrus())
    signal.signal(signal.SIGTERM, lambda sig, frame: shutdown_hydrus())

    boot_hydrus(getattr(args, 'db-path'))

    HydrusData.DebugPrint(f"[WEBSOCKET API] Starting websocket server on port {args.port}")
    asyncio.run(wsapi.websocket_main(args))

    shutdown_hydrus()
