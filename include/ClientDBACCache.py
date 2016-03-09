import ClientData
import ClientDefaults
import ClientFiles
import ClientImporting
import ClientMedia
import ClientRatings
import ClientThreading
import collections
import hashlib
import httplib
import itertools
import json
import HydrusConstants as HC
import HydrusDB
import ClientDownloading
import ClientImageHandling
import HydrusEncryption
import HydrusExceptions
import HydrusFileHandling
import HydrusImageHandling
import HydrusNATPunch
import HydrusPaths
import HydrusSerialisable
import HydrusServer
import HydrusTagArchive
import HydrusTags
import HydrusThreading
import ClientConstants as CC
import lz4
import os
import Queue
import random
import shutil
import sqlite3
import stat
import sys
import threading
import time
import traceback
import wx
import yaml
import HydrusData
import ClientSearch
import HydrusGlobals

class DB( HydrusDB.HydrusDB ):
    
    DB_NAME = 'ac_cache'
    READ_WRITE_ACTIONS = []
    WRITE_SPECIAL_ACTIONS = [ 'vacuum' ]
    UPDATE_WAIT = 0
    
    def _AddFiles( self, hash_ids ):
        
        self._c.executemany( 'INSERT OR IGNORE INTO current_files ( hash_id ) VALUES ( ? );', ( ( hash_id, ) for hash_id in hash_ids ) )
        
    
    def _AddMappings( self, namespace_id, tag_id, hash_ids ):
        
        hash_ids = self._FilterFiles( hash_ids )
        
        if len( hash_ids ) > 0:
            
            self._RescindPendingMappings( tag_id, namespace_id, hash_ids )
            
            self._c.executemany( 'INSERT OR IGNORE INTO current_mappings ( hash_id, namespace_id, tag_id ) VALUES ( ?, ?, ? );', ( ( hash_id, namespace_id, tag_id ) for hash_id in hash_ids ) )
            
            num_new = self._GetRowCount()
            
            if num_new > 0:
                
                self._c.execute( 'INSERT OR IGNORE INTO ac_cache ( namespace_id, tag_id, current_count, pending_count ) VALUES ( ?, ?, ?, ? );', ( namespace_id, tag_id, 0, 0 ) )
                
                self._c.execute( 'UPDATE ac_cache SET current_count = current_count + ? WHERE namespace_id = ? AND tag_id = ?;', ( num_new, namespace_id, tag_id ) )
                
            
        
    
    def _Analyze( self, stale_time_delta, stop_time ):
        
        all_names = [ name for ( name, ) in self._c.execute( 'SELECT name FROM sqlite_master;' ) ]
        
        existing_names_to_timestamps = dict( self._c.execute( 'SELECT name, timestamp FROM analyze_timestamps;' ).fetchall() )
        
        names_to_analyze = [ name for name in all_names if name not in existing_names_to_timestamps or HydrusData.TimeHasPassed( existing_names_to_timestamps[ name ] + stale_time_delta ) ]
        
        random.shuffle( names_to_analyze )
        
        while len( names_to_analyze ) > 0:
            
            name = names_to_analyze.pop()
            
            started = HydrusData.GetNowPrecise()
            
            self._c.execute( 'ANALYZE ' + name + ';' )
            
            self._c.execute( 'REPLACE INTO analyze_timestamps ( name, timestamp ) VALUES ( ?, ? );', ( name, HydrusData.GetNow() ) )
            
            time_took = HydrusData.GetNowPrecise() - started
            
            if HydrusData.TimeHasPassed( stop_time ) or not self._controller.CurrentlyIdle():
                
                break
                
            
        
        self._c.execute( 'ANALYZE sqlite_master;' ) # this reloads the current stats into the query planner
        
        still_more_to_do = len( names_to_analyze ) > 0
        
        return still_more_to_do
        
    
    def _CreateDB( self ):
        
        self._c.execute( 'PRAGMA auto_vacuum = 0;' ) # none
        
        if HC.PLATFORM_WINDOWS:
            
            self._c.execute( 'PRAGMA page_size = 4096;' )
            
        
        try: self._c.execute( 'BEGIN IMMEDIATE' )
        except Exception as e:
            
            raise HydrusExceptions.DBAccessException( HydrusData.ToUnicode( e ) )
            
        
        self._c.execute( 'CREATE TABLE current_files ( hash_id INTEGER PRIMARY KEY );' )
        
        self._c.execute( 'CREATE TABLE current_mappings ( hash_id INTEGER, namespace_id INTEGER, tag_id INTEGER, PRIMARY KEY( hash_id, namespace_id, tag_id ) );' )
        self._c.execute( 'CREATE TABLE pending_mappings ( hash_id INTEGER, namespace_id INTEGER, tag_id INTEGER, PRIMARY KEY( hash_id, namespace_id, tag_id ) );' )
        
        self._c.execute( 'CREATE TABLE ac_cache ( namespace_id INTEGER, tag_id INTEGER, current_count INTEGER, pending_count INTEGER, PRIMARY KEY( namespace_id, tag_id ) );' )
        
        self._c.execute( 'CREATE TABLE existing_tags ( namespace_id INTEGER, tag_id INTEGER, PRIMARY KEY( namespace_id, tag_id ) );' )
        self._c.execute( 'CREATE INDEX existing_tags_tag_id_index ON existing_tags ( tag_id );' )
        
        self._c.execute( 'CREATE TABLE analyze_timestamps ( name TEXT, timestamp INTEGER );' )
        self._c.execute( 'CREATE TABLE maintenance_timestamps ( name TEXT, timestamp INTEGER );' )
        self._c.execute( 'CREATE TABLE version ( version INTEGER );' )
        
        self._c.execute( 'INSERT INTO version ( version ) VALUES ( ? );', ( HC.SOFTWARE_VERSION, ) )
        
        self._c.execute( 'COMMIT' )
        
    
    def _DeleteFiles( self, hash_ids ):
        
        for hash_id in hash_ids:
            
            pending_mappings_ids = self._c.execute( 'SELECT namespace_id, tag_id FROM pending_mappings WHERE hash_id = ?;', ( hash_id, ) ).fetchall()
            
            for ( namespace_id, tag_id ) in pending_mappings_ids:
                
                self._RescindPendingMappings( namespace_id, tag_id, { hash_id } )
                
            
            current_mappings_ids = self._c.execute( 'SELECT namespace_id, tag_id FROM current_mappings WHERE hash_id = ?;', ( hash_id, ) ).fetchall()
            
            for ( namespace_id, tag_id ) in current_mappings_ids:
                
                self._DeleteMappings( namespace_id, tag_id, { hash_id } )
                
            
        
        self._c.execute( 'DELETE FROM current_files WHERE hash_id IN ' + HydrusData.SplayListForDB( hash_ids ) + ';' )
        
    
    def _DeleteMappings( self, namespace_id, tag_id, hash_ids ):
        
        hash_ids = self._FilterFiles( hash_ids )
        
        if len( hash_ids ) > 0:
            
            self._c.execute( 'DELETE FROM current_mappings WHERE hash_id IN ' + HydrusData.SplayListForDB( hash_ids ) + ' AND namespace_id = ? AND tag_id = ?;' )
            
            num_deleted = self._GetRowCount()
            
            if num_deleted > 0:
                
                self._c.execute( 'UPDATE ac_cache SET current_count = current_count - ? WHERE namespace_id = ? AND tag_id = ?;', ( num_deleted, namespace_id, tag_id ) )
                
                self._c.execute( 'DELETE FROM ac_cache WHERE namespace_id = ? AND tag_id = ? AND current_count = ? AND pending_count = ?;', ( namespace_id, tag_id, 0, 0 ) )
                
            
        
    
    def _GetAutocompleteCounts( self, mapping_ids ):
        
        results = []
        
        for ( namespace_id, tag_ids ) in HydrusData.BuildKeyToListDict( mapping_ids ).items():
            
            results.extend( ( ( namespace_id, tag_id, current_count, pending_count ) for ( tag_id, current_count, pending_count ) in self._c.execute( 'SELECT tag_id, current_count, pending_count FROM ac_cache WHERE namespace_id = ? AND tag_id IN ' + HydrusData.SplayListForDB( tag_ids ) + ';', ( namespace_id, ) ) ) )
            
        
        return results
        
    
    def _FilterFiles( self, hash_ids ):
        
        return [ hash_id for ( hash_id, ) in self._c.execute( 'SELECT hash_id FROM current_files WHERE hash_id IN ' + HydrusData.SplayListForDB( hash_ids ) + ';' ) ]
        
    
    def _HasFile( self, hash_id ):
        
        result = self._c.execute( 'SELECT 1 FROM current_files WHERE hash_id = ?;', ( hash_id, ) ).fetchone()
        
        if result is None:
            
            return False
            
        else:
            
            return True
            
        
    
    def _PendMappings( self, namespace_id, tag_id, hash_ids ):
        
        hash_ids = self._FilterFiles( hash_ids )
        
        if len( hash_ids ) > 0:
            
            self._c.executemany( 'INSERT OR IGNORE INTO pending_mappings ( hash_id, namespace_id, tag_id ) VALUES ( ?, ?, ? );', ( ( hash_id, namespace_id, tag_id ) for hash_id in hash_ids ) )
            
            num_new = self._GetRowCount()
            
            if num_new > 0:
                
                self._c.execute( 'INSERT OR IGNORE INTO ac_cache ( namespace_id, tag_id, current_count, pending_count ) VALUES ( ?, ?, ?, ? );', ( namespace_id, tag_id, 0, 0 ) )
                
                self._c.execute( 'UPDATE ac_cache SET pending_count = pending_count + ? WHERE namespace_id = ? AND tag_id = ?;', ( num_new, namespace_id, tag_id ) )
                
            
        
    
    def _RescindPendingMappings( self, namespace_id, tag_id, hash_ids ):
        
        hash_ids = self._FilterFiles( hash_ids )
        
        if len( hash_ids ) > 0:
            
            self._c.execute( 'DELETE FROM pending_mappings WHERE hash_id IN ' + HydrusData.SplayListForDB( hash_ids ) + ' AND namespace_id = ? AND tag_id = ?;' )
            
            num_deleted = self._GetRowCount()
            
            if num_deleted > 0:
                
                self._c.execute( 'UPDATE ac_cache SET pending_count = pending_count - ? WHERE namespace_id = ? AND tag_id = ?;', ( num_deleted, namespace_id, tag_id ) )
                
                self._c.execute( 'DELETE FROM ac_cache WHERE namespace_id = ? AND tag_id = ? AND current_count = ? AND pending_count = ?;', ( namespace_id, tag_id, 0, 0 ) )
                
            
        
    
    def _Read( self, action, *args, **kwargs ):
        
        if action == 'ac_counts': result = self._GetAutocompleteCounts( *args, **kwargs )
        else: raise Exception( 'db received an unknown read command: ' + action )
        
        return result
        
    
    def _UpdateDB( self, version ):
        
        self._c.execute( 'UPDATE version SET version = ?;', ( version + 1, ) )
        
    
    def _Vacuum( self ):
        
        if not self._fast_big_transaction_wal:
            
            self._c.execute( 'PRAGMA journal_mode = TRUNCATE;' )
            
        
        if HC.PLATFORM_WINDOWS:
            
            ideal_page_size = 4096
            
        else:
            
            ideal_page_size = 1024
            
        
        ( page_size, ) = self._c.execute( 'PRAGMA page_size;' ).fetchone()
        
        if page_size != ideal_page_size:
            
            self._c.execute( 'PRAGMA journal_mode = TRUNCATE;' )
            self._c.execute( 'PRAGMA page_size = ' + str( ideal_page_size ) + ';' )
            
        
        self._c.execute( 'VACUUM' )
        
        self._c.execute( 'REPLACE INTO maintenance_timestamps ( name, timestamp ) VALUES ( ?, ? );', ( 'vacuum', HydrusData.GetNow() ) )
        
        self._InitDBCursor()
        
    
    def _Write( self, action, *args, **kwargs ):
        
        if action == 'add_files': result = self._AddFiles( *args, **kwargs )
        elif action == 'add_mappings': result = self._AddMappings( *args, **kwargs )
        elif action == 'analyze': result = self._Analyze( *args, **kwargs )
        elif action == 'delete_files': result = self._DeleteFiles( *args, **kwargs )
        elif action == 'delete_mappings':result = self._DeleteMappings( *args, **kwargs )
        elif action == 'pend_mappings':result = self._PendMappings( *args, **kwargs )
        elif action == 'rescind_pending_mappings': result = self._RescindPendingMappings( *args, **kwargs )
        elif action == 'vacuum': result = self._Vacuum( *args, **kwargs )
        else: raise Exception( 'db received an unknown write command: ' + action )
        
        return result
        
    