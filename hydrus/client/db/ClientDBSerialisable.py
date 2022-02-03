import json
import os
import sqlite3
import time
import typing

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusDBBase
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusSerialisable

from hydrus.client import ClientConstants as CC
from hydrus.client.db import ClientDBModule
from hydrus.client.db import ClientDBServices

YAML_DUMP_ID_SINGLE = 0
YAML_DUMP_ID_REMOTE_BOORU = 1
YAML_DUMP_ID_FAVOURITE_CUSTOM_FILTER_ACTIONS = 2
YAML_DUMP_ID_GUI_SESSION = 3
YAML_DUMP_ID_IMAGEBOARD = 4
YAML_DUMP_ID_IMPORT_FOLDER = 5
YAML_DUMP_ID_EXPORT_FOLDER = 6
YAML_DUMP_ID_SUBSCRIPTION = 7
YAML_DUMP_ID_LOCAL_BOORU = 8

def ExportBrokenHashedJSONDump( db_dir, dump, dump_descriptor ):
    
    timestamp_string = time.strftime( '%Y-%m-%d %H-%M-%S' )
    
    filename = '({}) at {}.json'.format( dump_descriptor, timestamp_string )
    
    path = os.path.join( db_dir, filename )
    
    with open( path, 'wb' ) as f:
        
        if isinstance( dump, str ):
            
            dump = bytes( dump, 'utf-8', errors = 'replace' )
            
        
        f.write( dump )
        
    
def DealWithBrokenJSONDump( db_dir, dump, dump_object_descriptor, dump_descriptor ):
    
    HydrusData.Print( 'I am exporting a broken dump. The full object descriptor is: {}'.format( dump_object_descriptor ) )
    
    timestamp_string = time.strftime( '%Y-%m-%d %H-%M-%S' )
    hex_chars = os.urandom( 4 ).hex()
    
    filename = '({}) at {} {}.json'.format( dump_descriptor, timestamp_string, hex_chars )
    
    path = os.path.join( db_dir, filename )
    
    with open( path, 'wb' ) as f:
        
        if isinstance( dump, str ):
            
            dump = bytes( dump, 'utf-8', errors = 'replace' )
            
        
        f.write( dump )
        
    
    message = 'A serialised object failed to load! Its description is "{}".'.format( dump_descriptor )
    message += os.linesep * 2
    message += 'This error could be due to several factors, but is most likely a hard drive fault (perhaps your computer recently had a bad power cut?).'
    message += os.linesep * 2
    message += 'The database has attempted to delete the broken object, and the object\'s dump written to {}. Depending on the object, your client may no longer be able to boot, or it may have lost something like a session or a subscription.'.format( path )
    message += os.linesep * 2
    message += 'Please review the \'help my db is broke.txt\' file in your install_dir/db directory as background reading, and if the situation or fix here is not obvious, please contact hydrus dev.'
    
    HydrusData.ShowText( message )
    
    raise HydrusExceptions.SerialisationException( message )
    
def GenerateBigSQLiteDumpBuffer( dump ):
    
    try:
        
        dump_bytes = bytes( dump, 'utf-8' )
        
    except Exception as e:
        
        HydrusData.PrintException( e )
        
        raise Exception( 'While trying to save data to the database, it could not be decoded from UTF-8 to bytes! This could indicate an encoding error, such as Shift JIS sneaking into a downloader page! Please let hydrus dev know about this! Full error was written to the log!' )
        
    
    if len( dump_bytes ) >= 1000000000: # 1 billion, not 1GB https://sqlite.org/limits.html
        
        raise Exception( 'A data object could not save to the database because it was bigger than a buffer limit of 1,000,000,000 bytes! If your session has a page with >500k files/URLs, reduce its size NOW or you will lose it from your session on next load! On a download page, try clicking the arrow on the file log and clearing out successful imports. Otherwise, please report this to hydrus dev!' )
        
    
    try:
        
        dump_buffer = sqlite3.Binary( dump_bytes )
        
    except Exception as e:
        
        HydrusData.PrintException( e )
        
        raise Exception( 'While trying to save data to the database, it would not form into a buffer! Please let hydrus dev know about this! Full error was written to the log!' )
        
    
    return dump_buffer
    
class MaintenanceTracker( object ):
    
    my_instance = None
    
    def __init__( self ):
        
        self._last_hashed_serialisable_maintenance = 0
        self._total_new_hashed_serialisable_bytes = 0
        
    
    @staticmethod
    def instance() -> 'MaintenanceTracker':
        
        if MaintenanceTracker.my_instance is None:
            
            MaintenanceTracker.my_instance = MaintenanceTracker()
            
        
        return MaintenanceTracker.my_instance
        
    
    def HashedSerialisableMaintenanceDue( self ):
        
        return HydrusData.TimeHasPassed( self._last_hashed_serialisable_maintenance + 86400 ) or self._total_new_hashed_serialisable_bytes > 512 * 1048576
        
    
    def NotifyHashedSerialisableMaintenanceDone( self ):
        
        self._last_hashed_serialisable_maintenance = HydrusData.GetNow()
        self._total_new_hashed_serialisable_bytes = 0
        
    
    def RegisterNewHashedSerialisable( self, num_bytes ):
        
        self._total_new_hashed_serialisable_bytes += num_bytes
        
    
class ClientDBSerialisable( ClientDBModule.ClientDBModule ):
    
    def __init__( self, cursor: sqlite3.Cursor, db_dir, cursor_transaction_wrapper: HydrusDBBase.DBCursorTransactionWrapper, modules_services: ClientDBServices.ClientDBMasterServices ):
        
        ClientDBModule.ClientDBModule.__init__( self, 'client serialisable', cursor )
        
        self._db_dir = db_dir
        self._cursor_transaction_wrapper = cursor_transaction_wrapper
        self.modules_services = modules_services
        
    
    def _GetCriticalTableNames( self ) -> typing.Collection[ str ]:
        
        return {
            'main.json_dict',
            'main.json_dumps',
            'main.yaml_dumps'
        }
        
    
    def _GetInitialTableGenerationDict( self ) -> dict:
        
        return {
            'main.json_dict' : ( 'CREATE TABLE IF NOT EXISTS {} ( name TEXT PRIMARY KEY, dump BLOB_BYTES );', 400 ),
            'main.json_dumps' : ( 'CREATE TABLE IF NOT EXISTS {} ( dump_type INTEGER PRIMARY KEY, version INTEGER, dump BLOB_BYTES );', 400 ),
            'main.json_dumps_named' : ( 'CREATE TABLE IF NOT EXISTS {} ( dump_type INTEGER, dump_name TEXT, version INTEGER, timestamp INTEGER, dump BLOB_BYTES, PRIMARY KEY ( dump_type, dump_name, timestamp ) );', 400 ),
            'main.json_dumps_hashed' : ( 'CREATE TABLE IF NOT EXISTS {} ( hash BLOB_BYTES PRIMARY KEY, dump_type INTEGER, version INTEGER, dump BLOB_BYTES );', 442 ),
            'main.yaml_dumps' : ( 'CREATE TABLE IF NOT EXISTS {} ( dump_type INTEGER, dump_name TEXT, dump TEXT_YAML, PRIMARY KEY ( dump_type, dump_name ) );', 400 )
        }
        
    
    def DeleteJSONDump( self, dump_type ):
        
        self._Execute( 'DELETE FROM json_dumps WHERE dump_type = ?;', ( dump_type, ) )
        
    
    def DeleteJSONDumpNamed( self, dump_type, dump_name = None, timestamp = None ):
        
        if dump_name is None:
            
            self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ?;', ( dump_type, ) )
            
        elif timestamp is None:
            
            self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ?;', ( dump_type, dump_name ) )
            
        else:
            
            self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', ( dump_type, dump_name, timestamp ) )
            
        
    
    def DeleteYAMLDump( self, dump_type, dump_name = None ):
        
        if dump_name is None:
            
            self._Execute( 'DELETE FROM yaml_dumps WHERE dump_type = ?;', ( dump_type, ) )
            
        else:
            
            if dump_type == YAML_DUMP_ID_LOCAL_BOORU: dump_name = dump_name.hex()
            
            self._Execute( 'DELETE FROM yaml_dumps WHERE dump_type = ? AND dump_name = ?;', ( dump_type, dump_name ) )
            
        
        if dump_type == YAML_DUMP_ID_LOCAL_BOORU:
            
            service_id = self.modules_services.GetServiceId( CC.LOCAL_BOORU_SERVICE_KEY )
            
            self._Execute( 'DELETE FROM service_info WHERE service_id = ? AND info_type = ?;', ( service_id, HC.SERVICE_INFO_NUM_SHARES ) )
            
            HG.client_controller.pub( 'refresh_local_booru_shares' )
            
        
    
    def GetAllExpectedHashedJSONHashes( self ) -> typing.Collection[ bytes ]:
        
        all_expected_hashes = set()
        
        # not the GetJSONDumpNamesToBackupTimestamps call, which excludes the latest save!
        names_and_timestamps = self._Execute( 'SELECT dump_name, timestamp FROM json_dumps_named WHERE dump_type = ?;', ( HydrusSerialisable.SERIALISABLE_TYPE_GUI_SESSION_CONTAINER, ) ).fetchall()
        
        for ( name, timestamp ) in names_and_timestamps:
            
            session_container = self.GetJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_GUI_SESSION_CONTAINER, dump_name = name, timestamp = timestamp )
            
            all_expected_hashes.update( session_container.GetPageDataHashes() )
            
        
        return all_expected_hashes
        
    
    def GetHashedJSONDumps( self, hashes ):
        
        shown_missing_dump_message = False
        shown_broken_dump_message = False
        
        hashes_to_objs = {}
        
        for hash in hashes:
            
            result = self._Execute( 'SELECT version, dump_type, dump FROM json_dumps_hashed WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
            
            if result is None:
                
                if not shown_missing_dump_message:
                    
                    message = 'A hashed serialised object was missing! Its hash is "{}".'.format( hash.hex() )
                    message += os.linesep * 2
                    message += 'This error could be due to several factors, but is most likely a hard drive fault (perhaps your computer recently had a bad power cut?).'
                    message += os.linesep * 2
                    message += 'Your client may have lost one or more session pages.'
                    message += os.linesep * 2
                    message += 'Please review the \'help my db is broke.txt\' file in your install_dir/db directory as background reading, and if the situation or fix here is not obvious, please contact hydrus dev.'
                    
                    HydrusData.ShowText( message )
                    
                    shown_missing_dump_message = True
                    
                
                HydrusData.Print( 'Was asked to fetch named JSON object "{}", but it was missing!'.format( hash.hex() ) )
                
                continue
                
            
            ( version, dump_type, dump ) = result
            
            try:
                
                if isinstance( dump, bytes ):
                    
                    dump = str( dump, 'utf-8' )
                    
                
                serialisable_info = json.loads( dump )
                
            except:
                
                self._Execute( 'DELETE FROM json_dumps_hashed WHERE hash = ?;', ( sqlite3.Binary( hash ), ) )
                
                self._cursor_transaction_wrapper.CommitAndBegin()
                
                ExportBrokenHashedJSONDump( self._db_dir, dump, 'hash {} dump_type {}'.format( hash.hex(), dump_type ) )
                
                if not shown_broken_dump_message:
                    
                    message = 'A hashed serialised object failed to load! Its hash is "{}".'.format( hash.hex() )
                    message += os.linesep * 2
                    message += 'This error could be due to several factors, but is most likely a hard drive fault (perhaps your computer recently had a bad power cut?).'
                    message += os.linesep * 2
                    message += 'The database has attempted to delete the broken object, and the object\'s dump written to your database directory. Your client may have lost one or more session pages.'
                    message += os.linesep * 2
                    message += 'Please review the \'help my db is broke.txt\' file in your install_dir/db directory as background reading, and if the situation or fix here is not obvious, please contact hydrus dev.'
                    
                    HydrusData.ShowText( message )
                    
                    shown_broken_dump_message = True
                    
                
                HydrusData.Print( 'Was asked to fetch named JSON object "{}", but it was malformed!'.format( hash.hex() ) )
                
            
            obj = HydrusSerialisable.CreateFromSerialisableTuple( ( dump_type, version, serialisable_info ) )
            
            hashes_to_objs[ hash ] = obj
            
        
        return hashes_to_objs
        
    
    def GetGUISession( self, name, timestamp = None ):
        
        session_container = self.GetJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_GUI_SESSION_CONTAINER, dump_name = name, timestamp = timestamp )
        
        hashes = session_container.GetPageDataHashes()
        
        hashes_to_page_data = self.GetHashedJSONDumps( hashes )
        
        session_container.SetHashesToPageData( hashes_to_page_data )
        
        if not session_container.HasAllPageData():
            
            HydrusData.ShowText( 'The session "{}" had missing page data on load! It will still try to load what it can into GUI, but every missing page will print its information! It may have been a logical error that failed to save or keep the correct page, or you may have had a database failure. If you have had any other database or hard drive issues recently, please check out "help my db is broke.txt" in your install_dir/db directory. Hydev would also like to know the details here!'.format( name ) )
            
        
        return session_container
        
    
    def GetJSONDump( self, dump_type ):
        
        result = self._Execute( 'SELECT version, dump FROM json_dumps WHERE dump_type = ?;', ( dump_type, ) ).fetchone()
        
        if result is None:
            
            return result
            
        else:
            
            ( version, dump ) = result
            
            try:
                
                if isinstance( dump, bytes ):
                    
                    dump = str( dump, 'utf-8' )
                    
                
                serialisable_info = json.loads( dump )
                
            except:
                
                self._Execute( 'DELETE FROM json_dumps WHERE dump_type = ?;', ( dump_type, ) )
                
                self._cursor_transaction_wrapper.CommitAndBegin()
                
                DealWithBrokenJSONDump( self._db_dir, dump, ( dump_type, version ), 'dump_type {} version {}'.format( dump_type, version ) )
                
            
            obj = HydrusSerialisable.CreateFromSerialisableTuple( ( dump_type, version, serialisable_info ) )
            
            if dump_type == HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER:
                
                session_containers = self.GetJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER_SESSION_CONTAINER )
                
                obj.SetSessionContainers( session_containers )
                
            elif dump_type == HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER:
                
                tracker_containers = self.GetJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER_TRACKER_CONTAINER )
                
                obj.SetTrackerContainers( tracker_containers )
                
            
            return obj
            
        
    
    def GetJSONDumpNamed( self, dump_type, dump_name = None, timestamp = None ):
        
        if dump_name is None:
            
            results = self._Execute( 'SELECT dump_name, version, dump, timestamp FROM json_dumps_named WHERE dump_type = ?;', ( dump_type, ) ).fetchall()
            
            objs = []
            
            for ( dump_name, version, dump, object_timestamp ) in results:
                
                try:
                    
                    if isinstance( dump, bytes ):
                        
                        dump = str( dump, 'utf-8' )
                        
                    
                    serialisable_info = json.loads( dump )
                    
                    objs.append( HydrusSerialisable.CreateFromSerialisableTuple( ( dump_type, dump_name, version, serialisable_info ) ) )
                    
                except:
                    
                    self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', ( dump_type, dump_name, object_timestamp ) )
                    
                    self._cursor_transaction_wrapper.CommitAndBegin()
                    
                    DealWithBrokenJSONDump( self._db_dir, dump, ( dump_type, dump_name, version, object_timestamp ), 'dump_type {} dump_name {} version {} timestamp {}'.format( dump_type, dump_name[:10], version, object_timestamp ) )
                    
                
            
            return objs
            
        else:
            
            if timestamp is None:
                
                result = self._Execute( 'SELECT version, dump, timestamp FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? ORDER BY timestamp DESC;', ( dump_type, dump_name ) ).fetchone()
                
            else:
                
                result = self._Execute( 'SELECT version, dump, timestamp FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', ( dump_type, dump_name, timestamp ) ).fetchone()
                
            
            if result is None:
                
                raise HydrusExceptions.DataMissing( 'Could not find the object of type "{}" and name "{}" and timestamp "{}".'.format( dump_type, dump_name, str( timestamp ) ) )
                
            
            ( version, dump, object_timestamp ) = result
            
            try:
                
                if isinstance( dump, bytes ):
                    
                    dump = str( dump, 'utf-8' )
                    
                
                serialisable_info = json.loads( dump )
                
            except:
                
                self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', ( dump_type, dump_name, object_timestamp ) )
                
                self._cursor_transaction_wrapper.CommitAndBegin()
                
                DealWithBrokenJSONDump( self._db_dir, dump, ( dump_type, dump_name, version, object_timestamp ), 'dump_type {} dump_name {} version {} timestamp {}'.format( dump_type, dump_name[:10], version, object_timestamp ) )
                
            
            return HydrusSerialisable.CreateFromSerialisableTuple( ( dump_type, dump_name, version, serialisable_info ) )
            
        
    
    def GetJSONDumpNames( self, dump_type ):
        
        names = [ name for ( name, ) in self._Execute( 'SELECT DISTINCT dump_name FROM json_dumps_named WHERE dump_type = ?;', ( dump_type, ) ) ]
        
        return names
        
    
    def GetJSONDumpNamesToBackupTimestamps( self, dump_type ):
        
        names_to_backup_timestamps = HydrusData.BuildKeyToListDict( self._Execute( 'SELECT dump_name, timestamp FROM json_dumps_named WHERE dump_type = ? ORDER BY timestamp ASC;', ( dump_type, ) ) )
        
        for ( name, timestamp_list ) in list( names_to_backup_timestamps.items() ):
            
            timestamp_list.pop( -1 ) # remove the non backup timestamp
            
            if len( timestamp_list ) == 0:
                
                del names_to_backup_timestamps[ name ]
                
            
        
        return names_to_backup_timestamps
        
    
    def GetJSONSimple( self, name ):
        
        result = self._Execute( 'SELECT dump FROM json_dict WHERE name = ?;', ( name, ) ).fetchone()
        
        if result is None:
            
            return None
            
        
        ( dump, ) = result
        
        if isinstance( dump, bytes ):
            
            dump = str( dump, 'utf-8' )
            
        
        value = json.loads( dump )
        
        return value
        
    
    def GetTablesAndColumnsThatUseDefinitions( self, content_type: int ) -> typing.List[ typing.Tuple[ str, str ] ]:
        
        return []
        
    
    def GetYAMLDump( self, dump_type, dump_name = None ):
        
        if dump_name is None:
            
            result = { dump_name : data for ( dump_name, data ) in self._Execute( 'SELECT dump_name, dump FROM yaml_dumps WHERE dump_type = ?;', ( dump_type, ) ) }
            
            if dump_type == YAML_DUMP_ID_LOCAL_BOORU:
                
                result = { bytes.fromhex( dump_name ) : data for ( dump_name, data ) in list(result.items()) }
                
            
        else:
            
            if dump_type == YAML_DUMP_ID_LOCAL_BOORU: dump_name = dump_name.hex()
            
            result = self._Execute( 'SELECT dump FROM yaml_dumps WHERE dump_type = ? AND dump_name = ?;', ( dump_type, dump_name ) ).fetchone()
            
            if result is None:
                
                if result is None:
                    
                    raise HydrusExceptions.DataMissing( dump_name + ' was not found!' )
                    
                
            else:
                
                ( result, ) = result
                
            
        
        return result
        
    
    def GetYAMLDumpNames( self, dump_type ):
        
        names = [ name for ( name, ) in self._Execute( 'SELECT dump_name FROM yaml_dumps WHERE dump_type = ?;', ( dump_type, ) ) ]
        
        if dump_type == YAML_DUMP_ID_LOCAL_BOORU:
            
            names = [ bytes.fromhex( name ) for name in names ]
            
        
        return names
        
    
    def HaveHashedJSONDump( self, hash ):
        
        result = self._Execute( 'SELECT 1 FROM json_dumps_hashed WHERE hash = ?;', ( sqlite3.Binary( hash ), ) ).fetchone()
        
        return result is not None
        
    
    def HaveHashedJSONDumps( self, hashes ):
        
        for hash in hashes:
            
            if not self.HaveHashedJSONDump( hash ):
                
                return False
                
            
        
        return True
        
    
    def MaintainHashedStorage( self, force_start = False ):
        
        maintenance_tracker = MaintenanceTracker.instance()
        
        if not force_start:
            
            if not maintenance_tracker.HashedSerialisableMaintenanceDue():
                
                return
                
            
        
        all_expected_hashes = self.GetAllExpectedHashedJSONHashes()
        
        all_stored_hashes = self._STS( self._Execute( 'SELECT hash FROM json_dumps_hashed;' ) )
        
        all_deletee_hashes = all_stored_hashes.difference( all_expected_hashes )
        
        if len( all_deletee_hashes ) > 0:
            
            self._ExecuteMany( 'DELETE FROM json_dumps_hashed WHERE hash = ?;', ( ( sqlite3.Binary( hash ), ) for hash in all_deletee_hashes ) )
            
        
        maintenance_tracker.NotifyHashedSerialisableMaintenanceDone()
        
        return len( all_deletee_hashes )
        
    
    def OverwriteJSONDumps( self, dump_types, objs ):
        
        for dump_type in dump_types:
            
            self.DeleteJSONDumpNamed( dump_type )
            
        
        for obj in objs:
            
            self.SetJSONDump( obj )
            
        
    
    def SetHashedJSONDumps( self, hashes_to_objs ):
        
        for ( hash, obj ) in hashes_to_objs.items():
            
            if self.HaveHashedJSONDump( hash ):
                
                continue
                
            
            ( dump_type, version, serialisable_info ) = obj.GetSerialisableTuple()
            
            try:
                
                dump = json.dumps( serialisable_info )
                
            except Exception as e:
                
                HydrusData.ShowException( e )
                HydrusData.Print( obj )
                HydrusData.Print( serialisable_info )
                
                raise Exception( 'Trying to json dump the hashed object ' + str( obj ) + ' caused an error. Its serialisable info has been dumped to the log.' )
                
            
            maintenance_tracker = MaintenanceTracker.instance()
            
            maintenance_tracker.RegisterNewHashedSerialisable( len( dump ) )
            
            dump_buffer = GenerateBigSQLiteDumpBuffer( dump )
            
            try:
                
                self._Execute( 'INSERT INTO json_dumps_hashed ( hash, dump_type, version, dump ) VALUES ( ?, ?, ?, ? );', ( sqlite3.Binary( hash ), dump_type, version, dump_buffer ) )
                
            except:
                
                HydrusData.DebugPrint( dump )
                HydrusData.ShowText( 'Had a problem saving a hashed JSON object. The dump has been printed to the log.' )
                
                try:
                    
                    HydrusData.Print( 'Dump had length {}!'.format( HydrusData.ToHumanBytes( len( dump_buffer ) ) ) )
                    
                except:
                    
                    pass
                    
                
                raise
                
            
        
    
    def SetJSONDump( self, obj, force_timestamp = None ):
        
        if isinstance( obj, HydrusSerialisable.SerialisableBaseNamed ):
            
            ( dump_type, dump_name, version, serialisable_info ) = obj.GetSerialisableTuple()
            
            store_backups = False
            backup_depth = 1
            
            if dump_type == HydrusSerialisable.SERIALISABLE_TYPE_GUI_SESSION_CONTAINER:
                
                if not obj.HasAllDirtyPageData():
                    
                    raise Exception( 'A session with name "{}" was set to save, but it did not have all its dirty page data!'.format( dump_name ) )
                    
                
                hashes_to_page_data = obj.GetHashesToPageData()
                
                self.SetHashedJSONDumps( hashes_to_page_data )
                
                if force_timestamp is None:
                    
                    store_backups = True
                    backup_depth = HG.client_controller.new_options.GetInteger( 'number_of_gui_session_backups' )
                    
                
            
            try:
                
                dump = json.dumps( serialisable_info )
                
            except Exception as e:
                
                HydrusData.ShowException( e )
                HydrusData.Print( obj )
                HydrusData.Print( serialisable_info )
                
                raise Exception( 'Trying to json dump the object ' + str( obj ) + ' with name ' + dump_name + ' caused an error. Its serialisable info has been dumped to the log.' )
                
            
            if force_timestamp is None:
                
                object_timestamp = HydrusData.GetNow()
                
                if store_backups:
                    
                    existing_timestamps = sorted( self._STI( self._Execute( 'SELECT timestamp FROM json_dumps_named WHERE dump_type = ? AND dump_name = ?;', ( dump_type, dump_name ) ) ) )
                    
                    if len( existing_timestamps ) > 0:
                        
                        # the user has changed their system clock, so let's make sure the new timestamp is larger at least
                        
                        largest_existing_timestamp = max( existing_timestamps )
                        
                        if largest_existing_timestamp > object_timestamp:
                            
                            object_timestamp = largest_existing_timestamp + 1
                            
                        
                    
                    deletee_timestamps = existing_timestamps[ : - backup_depth ] # keep highest n values
                    
                    deletee_timestamps.append( object_timestamp ) # if save gets spammed twice in one second, we'll overwrite
                    
                    self._ExecuteMany( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', [ ( dump_type, dump_name, timestamp ) for timestamp in deletee_timestamps ] )
                    
                else:
                    
                    self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ?;', ( dump_type, dump_name ) )
                    
                
            else:
                
                object_timestamp = force_timestamp
                
            
            dump_buffer = GenerateBigSQLiteDumpBuffer( dump )
            
            try:
                
                self._Execute( 'INSERT INTO json_dumps_named ( dump_type, dump_name, version, timestamp, dump ) VALUES ( ?, ?, ?, ?, ? );', ( dump_type, dump_name, version, object_timestamp, dump_buffer ) )
                
            except:
                
                HydrusData.DebugPrint( dump )
                HydrusData.ShowText( 'Had a problem saving a JSON object. The dump has been printed to the log.' )
                
                try:
                    
                    HydrusData.Print( 'Dump had length {}!'.format( HydrusData.ToHumanBytes( len( dump_buffer ) ) ) )
                    
                except:
                    
                    pass
                    
                
                raise
                
            
        else:
            
            ( dump_type, version, serialisable_info ) = obj.GetSerialisableTuple()
            
            if dump_type == HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER:
                
                deletee_session_names = obj.GetDeleteeSessionNames()
                dirty_session_containers = obj.GetDirtySessionContainers()
                
                if len( deletee_session_names ) > 0:
                    
                    for deletee_session_name in deletee_session_names:
                        
                        self.DeleteJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER_SESSION_CONTAINER, dump_name = deletee_session_name )
                        
                    
                
                if len( dirty_session_containers ) > 0:
                    
                    for dirty_session_container in dirty_session_containers:
                        
                        self.SetJSONDump( dirty_session_container )
                        
                    
                
                if not obj.IsDirty():
                    
                    return
                    
                
            elif dump_type == HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER:
                
                deletee_tracker_names = obj.GetDeleteeTrackerNames()
                dirty_tracker_containers = obj.GetDirtyTrackerContainers()
                
                if len( deletee_tracker_names ) > 0:
                    
                    for deletee_tracker_name in deletee_tracker_names:
                        
                        self.DeleteJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER_TRACKER_CONTAINER, dump_name = deletee_tracker_name )
                        
                    
                
                if len( dirty_tracker_containers ) > 0:
                    
                    for dirty_tracker_container in dirty_tracker_containers:
                        
                        self.SetJSONDump( dirty_tracker_container )
                        
                    
                
                if not obj.IsDirty():
                    
                    return
                    
                
            
            try:
                
                dump = json.dumps( serialisable_info )
                
            except Exception as e:
                
                HydrusData.ShowException( e )
                HydrusData.Print( obj )
                HydrusData.Print( serialisable_info )
                
                raise Exception( 'Trying to json dump the object ' + str( obj ) + ' caused an error. Its serialisable info has been dumped to the log.' )
                
            
            self._Execute( 'DELETE FROM json_dumps WHERE dump_type = ?;', ( dump_type, ) )
            
            dump_buffer = GenerateBigSQLiteDumpBuffer( dump )
            
            try:
                
                self._Execute( 'INSERT INTO json_dumps ( dump_type, version, dump ) VALUES ( ?, ?, ? );', ( dump_type, version, dump_buffer ) )
                
            except:
                
                HydrusData.DebugPrint( dump )
                HydrusData.ShowText( 'Had a problem saving a JSON object. The dump has been printed to the log.' )
                
                raise
                
            
        
    
    def SetJSONComplex( self,
        overwrite_types_and_objs: typing.Optional[ typing.Tuple[ typing.Iterable[ int ], typing.Iterable[ HydrusSerialisable.SerialisableBase ] ] ] = None,
        set_objs: typing.Optional[ typing.List[ HydrusSerialisable.SerialisableBase ] ] = None,
        deletee_types_to_names: typing.Optional[ typing.Dict[ int, typing.Iterable[ str ] ] ] = None
    ):
        
        if overwrite_types_and_objs is not None:
            
            ( dump_types, objs ) = overwrite_types_and_objs
            
            self.OverwriteJSONDumps( dump_types, objs )
            
        
        if set_objs is not None:
            
            for obj in set_objs:
                
                self.SetJSONDump( obj )
                
            
        
        if deletee_types_to_names is not None:
            
            for ( dump_type, names ) in deletee_types_to_names.items():
                
                for name in names:
                    
                    self.DeleteJSONDumpNamed( dump_type, dump_name = name )
                    
                
            
        
    
    def SetJSONSimple( self, name, value ):
        
        if value is None:
            
            self._Execute( 'DELETE FROM json_dict WHERE name = ?;', ( name, ) )
            
        else:
            
            dump = json.dumps( value )
            
            dump_buffer = GenerateBigSQLiteDumpBuffer( dump )
            
            try:
                
                self._Execute( 'REPLACE INTO json_dict ( name, dump ) VALUES ( ?, ? );', ( name, dump_buffer ) )
                
            except:
                
                HydrusData.DebugPrint( dump )
                HydrusData.ShowText( 'Had a problem saving a JSON object. The dump has been printed to the log.' )
                
                raise
                
            
        
    
    def SetYAMLDump( self, dump_type, dump_name, data ):
        
        if dump_type == YAML_DUMP_ID_LOCAL_BOORU:
            
            dump_name = dump_name.hex()
            
        
        self._Execute( 'DELETE FROM yaml_dumps WHERE dump_type = ? AND dump_name = ?;', ( dump_type, dump_name ) )
        
        try:
            
            self._Execute( 'INSERT INTO yaml_dumps ( dump_type, dump_name, dump ) VALUES ( ?, ?, ? );', ( dump_type, dump_name, data ) )
            
        except:
            
            HydrusData.Print( ( dump_type, dump_name, data ) )
            
            raise
            
        
        if dump_type == YAML_DUMP_ID_LOCAL_BOORU:
            
            service_id = self.modules_services.GetServiceId( CC.LOCAL_BOORU_SERVICE_KEY )
            
            self._Execute( 'DELETE FROM service_info WHERE service_id = ? AND info_type = ?;', ( service_id, HC.SERVICE_INFO_NUM_SHARES ) )
            
            HG.client_controller.pub( 'refresh_local_booru_shares' )
            
        
    
