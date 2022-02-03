import collections
import os
import sqlite3
import sys
import time
import traceback
import yaml

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusSerialisable

from hydrus.client import ClientConstants as CC
from hydrus.client import ClientThreading

def AddPaddingToDimensions( dimensions, padding ):
    
    ( x, y ) = dimensions
    
    return ( x + padding, y + padding )
    
def CatchExceptionClient( etype, value, tb ):
    
    try:
        
        trace_list = traceback.format_tb( tb )
        
        trace = ''.join( trace_list )
        
        pretty_value = str( value )
        
        if os.linesep in pretty_value:
            
            ( first_line, anything_else ) = pretty_value.split( os.linesep, 1 )
            
            trace = trace + os.linesep + anything_else
            
        else:
            
            first_line = pretty_value
            
        
        job_key = ClientThreading.JobKey()
        
        if etype == HydrusExceptions.ShutdownException:
            
            return
            
        else:
            
            try: job_key.SetStatusTitle( str( etype.__name__ ) )
            except: job_key.SetStatusTitle( str( etype ) )
            
            job_key.SetVariable( 'popup_text_1', first_line )
            job_key.SetTraceback( trace )
            
        
        text = job_key.ToString()
        
        HydrusData.Print( 'Uncaught exception:' )
        
        HydrusData.DebugPrint( text )
        
        HG.client_controller.pub( 'message', job_key )
        
    except:
        
        text = 'Encountered an error I could not parse:'
        
        text += os.linesep
        
        text += str( ( etype, value, tb ) )
        
        try: text += traceback.format_exc()
        except: pass
        
        HydrusData.ShowText( text )
        
    
    time.sleep( 1 )
    
def ConvertServiceKeysToContentUpdatesToPrettyString( service_keys_to_content_updates ):
    
    num_files = 0
    actions = set()
    locations = set()
    
    extra_words = ''
    
    for ( service_key, content_updates ) in list(service_keys_to_content_updates.items()):
        
        if len( content_updates ) > 0:
            
            name = HG.client_controller.services_manager.GetName( service_key )
            
            locations.add( name )
            
        
        for content_update in content_updates:
            
            ( data_type, action, row ) = content_update.ToTuple()
            
            if data_type == HC.CONTENT_TYPE_MAPPINGS:
                
                extra_words = ' tags for'
                
            
            actions.add( HC.content_update_string_lookup[ action ] )
            
            if action in ( HC.CONTENT_UPDATE_ARCHIVE, HC.CONTENT_UPDATE_INBOX ):
                
                locations = set()
                
            
            num_files += len( content_update.GetHashes() )
            
        
    
    s = ''
    
    if len( locations ) > 0:
        
        s += ', '.join( locations ) + '->'
        
    
    s += ', '.join( actions ) + extra_words + ' ' + HydrusData.ToHumanInt( num_files ) + ' files'
    
    return s
    
def ConvertServiceKeysToTagsToServiceKeysToContentUpdates( hashes, service_keys_to_tags ):
    
    service_keys_to_content_updates = {}
    
    for ( service_key, tags ) in service_keys_to_tags.items():
        
        if len( tags ) == 0:
            
            continue
            
        
        try:
            
            service = HG.client_controller.services_manager.GetService( service_key )
            
        except HydrusExceptions.DataMissing:
            
            continue
            
        
        if service.GetServiceType() == HC.LOCAL_TAG:
            
            action = HC.CONTENT_UPDATE_ADD
            
        else:
            
            action = HC.CONTENT_UPDATE_PEND
            
        
        content_updates = [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_MAPPINGS, action, ( tag, hashes ) ) for tag in tags ]
        
        service_keys_to_content_updates[ service_key ] = content_updates
        
    
    return service_keys_to_content_updates
    
def ConvertZoomToPercentage( zoom ):
    
    zoom_percent = zoom * 100
    
    pretty_zoom = '{:.2f}%'.format( zoom_percent )
    
    if pretty_zoom.endswith( '00%' ):
        
        pretty_zoom = '{:.0f}%'.format( zoom_percent )
        
    
    return pretty_zoom
    
def MergeCounts( min_a: int, max_a: int, min_b: int, max_b: int ):
    
    # this no longer takes 'None' maxes, and it is now comfortable with 0-5 ranges
    
    # 100-100 and 100-100 returns 100-200
    # 1-1 and 4-5 returns 4-6
    # 1-2, and 5-7 returns 5-9
    
    min_answer = max( min_a, min_b )
    max_answer = max_a + max_b
    
    return ( min_answer, max_answer )
    
def OrdIsSensibleASCII( o ):
    
    return 32 <= o and o <= 127
    
def OrdIsAlphaLower( o ):
    
    return 97 <= o and o <= 122
    
def OrdIsAlphaUpper( o ):
    
    return 65 <= o and o <= 90
    
def OrdIsAlpha( o ):
    
    return OrdIsAlphaLower( o ) or OrdIsAlphaUpper( o )
    
def OrdIsNumber( o ):
    
    return 48 <= o and o <= 57
    
def ShowExceptionClient( e, do_wait = True ):
    
    ( etype, value, tb ) = sys.exc_info()
    
    if etype is None:
        
        etype = type( e )
        value = str( e )
        
    
    ShowExceptionTupleClient( etype, value, tb, do_wait = do_wait )
    
def ShowExceptionTupleClient( etype, value, tb, do_wait = True ):
    
    if etype is None:
        
        etype = HydrusExceptions.UnknownException
        
    
    if value is None:
        
        value = 'Unknown error'
        
    
    if tb is None:
        
        trace = 'No error trace--here is the stack:' + os.linesep + ''.join( traceback.format_stack() )
        
    else:
        
        trace = ''.join( traceback.format_exception( etype, value, tb ) )
        
    
    pretty_value = str( value )
    
    if os.linesep in pretty_value:
        
        ( first_line, anything_else ) = pretty_value.split( os.linesep, 1 )
        
        trace = trace + os.linesep + anything_else
        
    else:
        
        first_line = pretty_value
        
    
    job_key = ClientThreading.JobKey()
    
    if etype == HydrusExceptions.ShutdownException:
        
        return
        
    else:
        
        title = str( getattr( etype, '__name__', etype ) )
        
        job_key.SetStatusTitle( title )
        
        job_key.SetVariable( 'popup_text_1', first_line )
        job_key.SetTraceback( trace )
        
    
    text = job_key.ToString()
    
    HydrusData.Print( 'Exception:' )
    
    HydrusData.DebugPrint( text )
    
    HG.client_controller.pub( 'message', job_key )
    
    if do_wait:
        
        time.sleep( 1 )
        
    
def ShowTextClient( text ):
    
    job_key = ClientThreading.JobKey()
    
    job_key.SetVariable( 'popup_text_1', str( text ) )
    
    text = job_key.ToString()
    
    HydrusData.Print( text )
    
    HG.client_controller.pub( 'message', job_key )
    
def TimestampToPrettyTimeDelta( timestamp, just_now_string = 'just now', just_now_threshold = 3, history_suffix = ' ago', show_seconds = True, no_prefix = False ):
    
    if HG.client_controller.new_options.GetBoolean( 'always_show_iso_time' ):
        
        return HydrusData.ConvertTimestampToPrettyTime( timestamp )
        
    else:
        
        return HydrusData.BaseTimestampToPrettyTimeDelta( timestamp, just_now_string = just_now_string, just_now_threshold = just_now_threshold, history_suffix = history_suffix, show_seconds = show_seconds, no_prefix = no_prefix )
        
    
HydrusData.TimestampToPrettyTimeDelta = TimestampToPrettyTimeDelta

def ToHumanBytes( size ):
    
    sig_figs = HG.client_controller.new_options.GetInteger( 'human_bytes_sig_figs' )
    
    return HydrusData.BaseToHumanBytes( size, sig_figs = sig_figs )
    
HydrusData.ToHumanBytes = ToHumanBytes

class Booru( HydrusData.HydrusYAMLBase ):
    
    yaml_tag = '!Booru'
    
    def __init__( self, name, search_url, search_separator, advance_by_page_num, thumb_classname, image_id, image_data, tag_classnames_to_namespaces ):
        
        self._name = name
        self._search_url = search_url
        self._search_separator = search_separator
        self._advance_by_page_num = advance_by_page_num
        self._thumb_classname = thumb_classname
        self._image_id = image_id
        self._image_data = image_data
        self._tag_classnames_to_namespaces = tag_classnames_to_namespaces
        
    
    def GetData( self ): return ( self._search_url, self._search_separator, self._advance_by_page_num, self._thumb_classname, self._image_id, self._image_data, self._tag_classnames_to_namespaces )
    
    def GetGalleryParsingInfo( self ): return ( self._search_url, self._advance_by_page_num, self._search_separator, self._thumb_classname )
    
    def GetName( self ): return self._name
    
    def GetNamespaces( self ): return list(self._tag_classnames_to_namespaces.values())
    
sqlite3.register_adapter( Booru, yaml.safe_dump )

class Credentials( HydrusData.HydrusYAMLBase ):
    
    yaml_tag = '!Credentials'
    
    def __init__( self, host, port, access_key = None ):
        
        HydrusData.HydrusYAMLBase.__init__( self )
        
        if host == 'localhost':
            
            host = '127.0.0.1'
            
        
        self._host = host
        self._port = port
        self._access_key = access_key
        
    
    def __eq__( self, other ):
        
        if isinstance( other, Credentials ):
            
            return self.__hash__() == other.__hash__()
            
        
        return NotImplemented
        
    
    def __hash__( self ): return ( self._host, self._port, self._access_key ).__hash__()
    
    def __ne__( self, other ): return self.__hash__() != other.__hash__()
    
    def __repr__( self ): return 'Credentials: ' + str( ( self._host, self._port, self._access_key.hex() ) )
    
    def GetAccessKey( self ): return self._access_key
    
    def GetAddress( self ): return ( self._host, self._port )
    
    def GetConnectionString( self ):
        
        connection_string = ''
        
        if self.HasAccessKey(): connection_string += self._access_key.hex() + '@'
        
        connection_string += self._host + ':' + str( self._port )
        
        return connection_string
        
    
    def GetPortedAddress( self ):
        
        if self._host.endswith( '/' ):
            
            host = self._host[:-1]
            
        else:
            
            host = self._host
            
        
        if '/' in host:
            
            ( actual_host, gubbins ) = self._host.split( '/', 1 )
            
            address = '{}:{}/{}'.format( actual_host, self._port, gubbins )
            
        else:
            
            address = '{}:{}'.format( self._host, self._port )
            
        
        return address
        
    
    def HasAccessKey( self ): return self._access_key is not None and self._access_key != ''
    
    def SetAccessKey( self, access_key ): self._access_key = access_key
    
class Imageboard( HydrusData.HydrusYAMLBase ):
    
    yaml_tag = '!Imageboard'
    
    def __init__( self, name, post_url, flood_time, form_fields, restrictions ):
        
        self._name = name
        self._post_url = post_url
        self._flood_time = flood_time
        self._form_fields = form_fields
        self._restrictions = restrictions
        
    
    def IsOKToPost( self, media_result ):
        
        # deleted old code due to deprecation
        
        return True
        
    
    def GetBoardInfo( self ): return ( self._post_url, self._flood_time, self._form_fields, self._restrictions )
    
    def GetName( self ): return self._name
    
sqlite3.register_adapter( Imageboard, yaml.safe_dump )
