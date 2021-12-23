import threading
import time

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusSerialisable

from hydrus.client import ClientConstants as CC
from hydrus.client import ClientDownloading
from hydrus.client.importing import ClientImportFileSeeds
from hydrus.client.importing import ClientImportGallerySeeds
from hydrus.client.importing import ClientImporting
from hydrus.client.importing.options import FileImportOptions
from hydrus.client.importing.options import PresentationImportOptions
from hydrus.client.importing.options import TagImportOptions
from hydrus.client.networking import ClientNetworkingJobs

class GalleryImport( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_GALLERY_IMPORT
    SERIALISABLE_NAME = 'Gallery Import'
    SERIALISABLE_VERSION = 2
    
    def __init__( self, query = None, source_name = None, initial_search_urls = None, start_file_queue_paused = False, start_gallery_queue_paused = False ):
        
        if query is None:
            
            query = 'samus_aran'
            
        
        if source_name is None:
            
            source_name = 'unknown'
            
        
        if initial_search_urls is None:
            
            initial_search_urls = []
            
        
        initial_search_urls = HydrusData.DedupeList( initial_search_urls )
        
        HydrusSerialisable.SerialisableBase.__init__( self )
        
        self._creation_time = HydrusData.GetNow()
        self._gallery_import_key = HydrusData.GenerateKey()
        
        self._query = query
        
        self._source_name = source_name
        
        self._page_key = 'initialising page key'
        self._publish_to_page = False
        
        self._current_page_index = 0
        self._num_new_urls_found = 0
        self._num_urls_found = 0
        
        self._file_limit = HC.options[ 'gallery_file_limit' ]
        
        self._files_paused = start_file_queue_paused
        self._gallery_paused = start_gallery_queue_paused
        
        self._file_import_options = HG.client_controller.new_options.GetDefaultFileImportOptions( 'loud' )
        
        self._tag_import_options = TagImportOptions.TagImportOptions( is_default = True )
        
        self._gallery_seed_log = ClientImportGallerySeeds.GallerySeedLog()
        
        gallery_seeds = [ ClientImportGallerySeeds.GallerySeed( url ) for url in initial_search_urls ]
        
        self._gallery_seed_log.AddGallerySeeds( gallery_seeds )
        
        self._file_seed_cache = ClientImportFileSeeds.FileSeedCache()
        
        self._no_work_until = 0
        self._no_work_until_reason = ''
        
        self._lock = threading.Lock()
        
        self._file_status = ''
        self._gallery_status = ''
        self._gallery_status_can_change_timestamp = 0
        
        self._all_work_finished = False
        
        self._have_started = False
        
        self._file_network_job = None
        self._gallery_network_job = None
        
        self._files_repeating_job = None
        self._gallery_repeating_job = None
        
        self._last_serialisable_change_timestamp = 0
        
        HG.client_controller.sub( self, 'NotifyFileSeedsUpdated', 'file_seed_cache_file_seeds_updated' )
        HG.client_controller.sub( self, 'NotifyGallerySeedsUpdated', 'gallery_seed_log_gallery_seeds_updated' )
        
    
    def _AmOverFileLimit( self ):
        
        if self._file_limit is not None and self._num_new_urls_found >= self._file_limit:
            
            return True
            
        
        return False
        
    
    def _DelayWork( self, time_delta, reason ):
        
        self._no_work_until = HydrusData.GetNow() + time_delta
        self._no_work_until_reason = reason
        
    
    def _GetSerialisableInfo( self ):
        
        serialisable_gallery_import_key = self._gallery_import_key.hex()
        
        serialisable_file_import_options = self._file_import_options.GetSerialisableTuple()
        serialisable_tag_import_options = self._tag_import_options.GetSerialisableTuple()
        
        serialisable_gallery_seed_log = self._gallery_seed_log.GetSerialisableTuple()
        serialisable_file_seed_cache = self._file_seed_cache.GetSerialisableTuple()
        
        return ( serialisable_gallery_import_key, self._creation_time, self._query, self._source_name, self._current_page_index, self._num_urls_found, self._num_new_urls_found, self._file_limit, self._gallery_paused, self._files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_seed_log, serialisable_file_seed_cache, self._no_work_until, self._no_work_until_reason )
        
    
    def _FileNetworkJobPresentationContextFactory( self, network_job ):
        
        def enter_call():
            
            with self._lock:
                
                self._file_network_job = network_job
                
            
        
        def exit_call():
            
            with self._lock:
                
                self._file_network_job = None
                
            
        
        return ClientImporting.NetworkJobPresentationContext( enter_call, exit_call )
        
    
    def _GalleryNetworkJobPresentationContextFactory( self, network_job ):
        
        def enter_call():
            
            with self._lock:
                
                self._gallery_network_job = network_job
                
            
        
        def exit_call():
            
            with self._lock:
                
                self._gallery_network_job = None
                
            
        
        return ClientImporting.NetworkJobPresentationContext( enter_call, exit_call )
        
    
    def _InitialiseFromSerialisableInfo( self, serialisable_info ):
        
        ( serialisable_gallery_import_key, self._creation_time, self._query, self._source_name, self._current_page_index, self._num_urls_found, self._num_new_urls_found, self._file_limit, self._gallery_paused, self._files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_seed_log, serialisable_file_seed_cache, self._no_work_until, self._no_work_until_reason ) = serialisable_info
        
        self._gallery_import_key = bytes.fromhex( serialisable_gallery_import_key )
        
        self._file_import_options = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_file_import_options )
        self._tag_import_options = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_tag_import_options )
        
        self._gallery_seed_log = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_gallery_seed_log )
        self._file_seed_cache = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_file_seed_cache )
        
    
    def _NetworkJobFactory( self, *args, **kwargs ):
        
        network_job = ClientNetworkingJobs.NetworkJobDownloader( self._gallery_import_key, *args, **kwargs )
        
        return network_job
        
    
    def _SerialisableChangeMade( self ):
        
        self._last_serialisable_change_timestamp = HydrusData.GetNow()
        
    
    def _UpdateSerialisableInfo( self, version, old_serialisable_info ):
        
        if version == 1:
            
            ( serialisable_gallery_import_key, self._creation_time, self._query, serialisable_gallery_identifier, self._current_page_index, self._num_urls_found, self._num_new_urls_found, self._file_limit, self._gallery_paused, self._files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_seed_log, serialisable_file_seed_cache, self._no_work_until, self._no_work_until_reason ) = old_serialisable_info
            
            gallery_identifier = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_gallery_identifier )
            
            source_name = ClientDownloading.ConvertGalleryIdentifierToGUGName( gallery_identifier )
            
            new_serialisable_info = ( serialisable_gallery_import_key, self._creation_time, self._query, source_name, self._current_page_index, self._num_urls_found, self._num_new_urls_found, self._file_limit, self._gallery_paused, self._files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_seed_log, serialisable_file_seed_cache, self._no_work_until, self._no_work_until_reason )
            
            return ( 2, new_serialisable_info )
            
        
    
    def _WorkOnFiles( self ):
        
        file_seed = self._file_seed_cache.GetNextFileSeed( CC.STATUS_UNKNOWN )
        
        if file_seed is None:
            
            return
            
        
        did_substantial_work = False
        
        try:
            
            def status_hook( text ):
                
                with self._lock:
                    
                    if len( text ) > 0:
                        
                        text = text.splitlines()[0]
                        
                    
                    self._file_status = text
                    
                
            
            did_substantial_work = file_seed.WorkOnURL( self._file_seed_cache, status_hook, self._NetworkJobFactory, self._FileNetworkJobPresentationContextFactory, self._file_import_options, self._tag_import_options )
            
            with self._lock:
                
                should_present = self._publish_to_page and file_seed.ShouldPresent( self._file_import_options.GetPresentationImportOptions() )
                
                page_key = self._page_key
                
            
            if should_present:
                
                file_seed.PresentToPage( page_key )
                
                did_substantial_work = True
                
            
        except HydrusExceptions.VetoException as e:
            
            status = CC.STATUS_VETOED
            
            note = str( e )
            
            file_seed.SetStatus( status, note = note )
            
            if isinstance( e, HydrusExceptions.CancelledException ):
                
                time.sleep( 2 )
                
            
        except HydrusExceptions.NotFoundException:
            
            status = CC.STATUS_VETOED
            note = '404'
            
            file_seed.SetStatus( status, note = note )
            
            time.sleep( 2 )
            
        except Exception as e:
            
            status = CC.STATUS_ERROR
            
            file_seed.SetStatus( status, exception = e )
            
            time.sleep( 3 )
            
        finally:
            
            self._file_seed_cache.NotifyFileSeedsUpdated( ( file_seed, ) )
            
            with self._lock:
                
                self._file_network_job = None
                
            
        
        if did_substantial_work:
            
            time.sleep( ClientImporting.DID_SUBSTANTIAL_FILE_WORK_MINIMUM_SLEEP_TIME )
            
        
    
    def _WorkOnGallery( self ):
        
        gallery_seed = self._gallery_seed_log.GetNextGallerySeed( CC.STATUS_UNKNOWN )
        
        if gallery_seed is None:
            
            return
            
        
        with self._lock:
            
            if self._AmOverFileLimit():
                
                self._gallery_paused = True
                
                return
                
            
            self._gallery_status = 'checking next page'
            
        
        def file_seeds_callable( file_seeds ):
            
            if self._file_limit is None:
                
                max_new_urls_allowed = None
                
            else:
                
                max_new_urls_allowed = self._file_limit - self._num_new_urls_found
                
            
            return ClientImporting.UpdateFileSeedCacheWithFileSeeds( self._file_seed_cache, file_seeds, max_new_urls_allowed = max_new_urls_allowed )
            
        
        def status_hook( text ):
            
            with self._lock:
                
                if len( text ) > 0:
                    
                    text = text.splitlines()[0]
                    
                
                self._gallery_status = text
                
            
        
        def title_hook( text ):
            
            return
            
        
        try:
            
            ( num_urls_added, num_urls_already_in_file_seed_cache, num_urls_total, result_404, added_new_gallery_pages, stop_reason ) = gallery_seed.WorkOnURL( 'download page', self._gallery_seed_log, file_seeds_callable, status_hook, title_hook, self._NetworkJobFactory, self._GalleryNetworkJobPresentationContextFactory, self._file_import_options )
            
            self._num_new_urls_found += num_urls_added
            self._num_urls_found += num_urls_total
            
            if num_urls_added > 0:
                
                ClientImporting.WakeRepeatingJob( self._files_repeating_job )
                
            
            self._current_page_index += 1
            
        except HydrusExceptions.NetworkException as e:
            
            with self._lock:
                
                delay = HG.client_controller.new_options.GetInteger( 'downloader_network_error_delay' )
                
                self._DelayWork( delay, str( e ) )
                
            
            return
            
        except Exception as e:
            
            gallery_seed_status = CC.STATUS_ERROR
            gallery_seed_note = str( e )
            
            gallery_seed.SetStatus( gallery_seed_status, note = gallery_seed_note )
            
            HydrusData.PrintException( e )
            
            with self._lock:
                
                self._gallery_paused = True
                
            
        finally:
            
            self._gallery_seed_log.NotifyGallerySeedsUpdated( ( gallery_seed, ) )
            
        
        return
        
    
    def CanRetryFailed( self ):
        
        with self._lock:
            
            return self._file_seed_cache.GetFileSeedCount( CC.STATUS_ERROR ) > 0
            
        
    
    def CanRetryIgnored( self ):
        
        with self._lock:
            
            return self._file_seed_cache.GetFileSeedCount( CC.STATUS_VETOED ) > 0
            
        
    
    def CurrentlyWorking( self ):
        
        with self._lock:
            
            finished = not self._file_seed_cache.WorkToDo()
            
            return not finished and not self._files_paused
            
        
    
    def FilesFinished( self ):
        
        with self._lock:
            
            return not self._file_seed_cache.WorkToDo()
            
        
    
    def FilesPaused( self ):
        
        with self._lock:
            
            return self._files_paused
            
        
    
    def GalleryFinished( self ):
        
        with self._lock:
            
            return not self._gallery_seed_log.WorkToDo()
            
        
    
    def GalleryPaused( self ):
        
        with self._lock:
            
            return self._gallery_paused
            
        
    
    def GetAPIInfoDict( self, simple ):
        
        with self._lock:
            
            d = {}
            
            d[ 'query_text' ] = self._query
            d[ 'source' ] = self._source_name
            d[ 'gallery_key' ] = self._gallery_import_key.hex()
            d[ 'files_paused' ] = self._files_paused
            d[ 'gallery_paused' ] = self._gallery_paused
            d[ 'imports' ] = self._file_seed_cache.GetAPIInfoDict( simple )
            d[ 'gallery_log' ] = self._gallery_seed_log.GetAPIInfoDict( simple )
            
            return d
            
        
    
    def GetCreationTime( self ):
        
        with self._lock:
            
            return self._creation_time
            
        
    
    def GetFileImportOptions( self ):
        
        with self._lock:
            
            return self._file_import_options
            
        
    
    def GetFileSeedCache( self ):
        
        with self._lock:
            
            return self._file_seed_cache
            
        
    
    def GetFileLimit( self ):
        
        with self._lock:
            
            return self._file_limit
            
        
    
    def GetGalleryImportKey( self ):
        
        with self._lock:
            
            return self._gallery_import_key
            
        
    
    def GetGallerySeedLog( self ):
        
        with self._lock:
            
            return self._gallery_seed_log
            
        
    
    def GetGalleryStatus( self ):
        
        with self._lock:
            
            if HydrusData.TimeHasPassed( self._no_work_until ):
                
                gallery_status = self._gallery_status
                
            else:
                
                no_work_text = '{}: {}'.format( HydrusData.ConvertTimestampToPrettyExpires( self._no_work_until ), self._no_work_until_reason )
                
                gallery_status = no_work_text
                
            
            return gallery_status
            
        
    
    def GetHashes( self ):
        
        with self._lock:
            
            fsc = self._file_seed_cache
            
        
        return fsc.GetHashes()
        
    
    def GetNetworkJobs( self ):
        
        with self._lock:
            
            return ( self._file_network_job, self._gallery_network_job )
            
        
    
    def GetNumSeeds( self ):
        
        with self._lock:
            
            return len( self._file_seed_cache ) + len( self._gallery_seed_log )
            
        
    
    def GetOptions( self ):
        
        with self._lock:
            
            return ( self._file_import_options, self._tag_import_options, self._file_limit )
            
        
    
    def GetPresentedHashes( self, presentation_import_options = None ):
        
        with self._lock:
            
            fsc = self._file_seed_cache
            
            if presentation_import_options is None:
                
                presentation_import_options = self._file_import_options.GetPresentationImportOptions()
                
            
        
        return fsc.GetPresentedHashes( presentation_import_options )
        
    
    def GetQueryText( self ):
        
        with self._lock:
            
            return self._query
            
        
    
    def GetSimpleStatus( self ):
        
        with self._lock:
            
            gallery_work_to_do = self._gallery_seed_log.WorkToDo()
            files_work_to_do = self._file_seed_cache.WorkToDo()
            
            gallery_go = gallery_work_to_do and not self._gallery_paused
            files_go = files_work_to_do and not self._files_paused
            
            if not ( gallery_work_to_do or files_work_to_do ):
                
                return ( ClientImporting.DOWNLOADER_SIMPLE_STATUS_DONE, 'DONE' )
                
            elif self._gallery_status != '' or self._file_status != '':
                
                return ( ClientImporting.DOWNLOADER_SIMPLE_STATUS_WORKING, 'working' )
                
            elif gallery_go or files_go:
                
                return ( ClientImporting.DOWNLOADER_SIMPLE_STATUS_PENDING, 'pending' )
                
            else:
                
                return ( ClientImporting.DOWNLOADER_SIMPLE_STATUS_PAUSED, '' )
                
            
        
    
    def GetSourceName( self ):
        
        with self._lock:
            
            return self._source_name
            
        
    
    def GetStatus( self ):
        
        with self._lock:
            
            if HydrusData.TimeHasPassed( self._no_work_until ):
                
                gallery_status = self._gallery_status
                file_status = self._file_status
                
            else:
                
                no_work_text = '{}: {}'.format( HydrusData.ConvertTimestampToPrettyExpires( self._no_work_until ), self._no_work_until_reason )
                
                gallery_status = no_work_text
                file_status = no_work_text
                
            
            return ( gallery_status, file_status, self._files_paused, self._gallery_paused )
            
        
    
    def GetTagImportOptions( self ):
        
        with self._lock:
            
            return self._tag_import_options
            
        
    
    def GetValueRange( self ):
        
        with self._lock:
            
            return self._file_seed_cache.GetValueRange()
            
        
    
    def HasSerialisableChangesSince( self, since_timestamp ):
        
        with self._lock:
            
            return self._last_serialisable_change_timestamp > since_timestamp
            
        
    
    def NotifyFileSeedsUpdated( self, file_seed_cache_key, file_seeds ):
        
        if file_seed_cache_key == self._file_seed_cache.GetFileSeedCacheKey():
            
            ClientImporting.WakeRepeatingJob( self._files_repeating_job )
            
            self._SerialisableChangeMade()
            
        
    
    def NotifyGallerySeedsUpdated( self, gallery_seed_log_key, gallery_seeds ):
        
        if gallery_seed_log_key == self._gallery_seed_log.GetGallerySeedLogKey():
            
            ClientImporting.WakeRepeatingJob( self._gallery_repeating_job )
            
            self._SerialisableChangeMade()
            
        
    
    def PausePlayFiles( self ):
        
        with self._lock:
            
            self._files_paused = not self._files_paused
            
            ClientImporting.WakeRepeatingJob( self._files_repeating_job )
            
            self._SerialisableChangeMade()
            
        
    
    def PausePlayGallery( self ):
        
        with self._lock:
            
            self._gallery_paused = not self._gallery_paused
            
            ClientImporting.WakeRepeatingJob( self._gallery_repeating_job )
            
            self._SerialisableChangeMade()
            
        
    
    def PublishToPage( self, publish_to_page ):
        
        with self._lock:
            
            if publish_to_page != self._publish_to_page:
                
                self._publish_to_page = publish_to_page
                
                self._SerialisableChangeMade()
                
            
        
    
    def Repage( self, page_key ):
        
        with self._lock:
            
            self._page_key = page_key
            
        
    
    def RetryFailed( self ):
        
        with self._lock:
            
            self._file_seed_cache.RetryFailed()
            
            self._SerialisableChangeMade()
            
        
    
    def RetryIgnored( self, ignored_regex = None ):
        
        with self._lock:
            
            self._file_seed_cache.RetryIgnored( ignored_regex = ignored_regex )
            
            self._SerialisableChangeMade()
            
        
    
    def SetFileLimit( self, file_limit ):
        
        with self._lock:
            
            if file_limit != self._file_limit:
                
                self._file_limit = file_limit
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetFileImportOptions( self, file_import_options: FileImportOptions.FileImportOptions ):
        
        with self._lock:
            
            if file_import_options.DumpToString() != self._file_import_options.DumpToString():
                
                self._file_import_options = file_import_options
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetFileSeedCache( self, file_seed_cache: ClientImportFileSeeds.FileSeedCache ):
        
        with self._lock:
            
            self._file_seed_cache = file_seed_cache
            
            self._SerialisableChangeMade()
            
        
    
    def SetGallerySeedLog( self, gallery_seed_log: ClientImportGallerySeeds.GallerySeedLog ):
        
        with self._lock:
            
            self._gallery_seed_log = gallery_seed_log
            
            self._SerialisableChangeMade()
            
        
    
    def SetTagImportOptions( self, tag_import_options: TagImportOptions.TagImportOptions ):
        
        with self._lock:
            
            if tag_import_options.DumpToString() != self._tag_import_options.DumpToString():
                
                self._tag_import_options = tag_import_options
                
            
        
    
    def Start( self, page_key, publish_to_page ):
        
        with self._lock:
            
            if self._have_started:
                
                return
                
            
            self._page_key = page_key
            self._publish_to_page = publish_to_page
            
            self._files_repeating_job = HG.client_controller.CallRepeating( ClientImporting.GetRepeatingJobInitialDelay(), ClientImporting.REPEATING_JOB_TYPICAL_PERIOD, self.REPEATINGWorkOnFiles )
            self._gallery_repeating_job = HG.client_controller.CallRepeating( ClientImporting.GetRepeatingJobInitialDelay(), ClientImporting.REPEATING_JOB_TYPICAL_PERIOD, self.REPEATINGWorkOnGallery )
            
            self._files_repeating_job.SetThreadSlotType( 'gallery_files' )
            self._gallery_repeating_job.SetThreadSlotType( 'gallery_search' )
            
            self._have_started = True
            
        
    
    def CanDoFileWork( self ):
        
        with self._lock:
            
            if ClientImporting.PageImporterShouldStopWorking( self._page_key ):
                
                self._files_repeating_job.Cancel()
                
                return False
                
            
            files_paused = self._files_paused or HG.client_controller.new_options.GetBoolean( 'pause_all_file_queues' )
            
            if files_paused:
                
                return False
                
            
            work_pending = self._file_seed_cache.WorkToDo()
            
            if not work_pending:
                
                return False
                
            
        
        return self.CanDoNetworkWork()
        
    
    def CanDoNetworkWork( self ):
        
        with self._lock:
            
            no_delays = HydrusData.TimeHasPassed( self._no_work_until )
            
            if not no_delays:
                
                return False
                
            
            page_shown = not HG.client_controller.PageClosedButNotDestroyed( self._page_key )
            
            if not page_shown:
                
                return False
                
            
            network_engine_good = not HG.client_controller.network_engine.IsBusy()
            
            if not network_engine_good:
                
                return False
                
            
        
        return True
        
    
    def REPEATINGWorkOnFiles( self ):
        
        try:
            
            while self.CanDoFileWork():
                
                try:
                    
                    self._WorkOnFiles()
                    
                    HG.client_controller.WaitUntilViewFree()
                    
                    self._SerialisableChangeMade()
                    
                except Exception as e:
                    
                    HydrusData.ShowException( e )
                    
                
            
        finally:
            
            with self._lock:
                
                self._file_status = ''
                
            
        
    
    def CanDoGalleryWork( self ):
        
        with self._lock:
            
            if ClientImporting.PageImporterShouldStopWorking( self._page_key ):
                
                self._gallery_repeating_job.Cancel()
                
                return False
                
            
            gallery_paused = self._gallery_paused or HG.client_controller.new_options.GetBoolean( 'pause_all_gallery_searches' )
            
            if gallery_paused:
                
                return False
                
            
            work_pending = self._gallery_seed_log.WorkToDo()
            
            if not work_pending:
                
                return False
                
            
        
        return self.CanDoNetworkWork()
        
    
    def REPEATINGWorkOnGallery( self ):
        
        try:
            
            while self.CanDoGalleryWork():
                
                try:
                    
                    self._WorkOnGallery()
                    
                    time.sleep( 1 )
                    
                    HG.client_controller.WaitUntilViewFree()
                    
                    self._SerialisableChangeMade()
                    
                except Exception as e:
                    
                    HydrusData.ShowException( e )
                    
                
            
        finally:
            
            with self._lock:
                
                self._gallery_status = ''
                
            
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_GALLERY_IMPORT ] = GalleryImport

class MultipleGalleryImport( HydrusSerialisable.SerialisableBase ):
    
    SERIALISABLE_TYPE = HydrusSerialisable.SERIALISABLE_TYPE_MULTIPLE_GALLERY_IMPORT
    SERIALISABLE_NAME = 'Multiple Gallery Import'
    SERIALISABLE_VERSION = 7
    
    def __init__( self, gug_key_and_name = None ):
        
        if gug_key_and_name is None:
            
            gug_key_and_name = ( HydrusData.GenerateKey(), 'unknown source' )
            
        
        HydrusSerialisable.SerialisableBase.__init__( self )
        
        self._lock = threading.Lock()
        
        self._page_key = 'initialising page key'
        
        self._gug_key_and_name = gug_key_and_name
        
        self._highlighted_gallery_import_key = None
        
        self._file_limit = HC.options[ 'gallery_file_limit' ]
        
        self._start_file_queues_paused = False
        self._start_gallery_queues_paused = False
        self._merge_simultaneous_pends_to_one_importer = False
        
        self._file_import_options = HG.client_controller.new_options.GetDefaultFileImportOptions( 'loud' )
        self._tag_import_options = TagImportOptions.TagImportOptions( is_default = True )
        
        self._gallery_imports = HydrusSerialisable.SerialisableList()
        
        self._gallery_import_keys_to_gallery_imports = {}
        
        self._status_dirty = True
        self._status_cache = ClientImportFileSeeds.FileSeedCacheStatus()
        
        self._last_time_imports_changed = HydrusData.GetNowPrecise()
        
        self._have_started = False
        
        self._last_serialisable_change_timestamp = 0
        
        self._last_pubbed_value_range = ( 0, 0 )
        self._next_pub_value_check_time = 0
        
        self._importers_repeating_job = None
        
    
    def _AddGalleryImport( self, gallery_import ):
        
        gallery_import.PublishToPage( False )
        gallery_import.Repage( self._page_key )
        
        self._gallery_imports.append( gallery_import )
        
        self._last_time_imports_changed = HydrusData.GetNowPrecise()
        
        gallery_import_key = gallery_import.GetGalleryImportKey()
        
        self._gallery_import_keys_to_gallery_imports[ gallery_import_key ] = gallery_import
        
        if len( self._gallery_imports ) == 1:
            
            self._highlighted_gallery_import_key = gallery_import_key
            
        
    
    def _GetSerialisableInfo( self ):
        
        ( gug_key, gug_name ) = self._gug_key_and_name
        
        serialisable_gug_key_and_name = ( gug_key.hex(), gug_name )
        
        if self._highlighted_gallery_import_key is None:
            
            serialisable_highlighted_gallery_import_key = self._highlighted_gallery_import_key
            
        else:
            
            serialisable_highlighted_gallery_import_key = self._highlighted_gallery_import_key.hex()
            
        
        pend_options = ( self._start_file_queues_paused, self._start_gallery_queues_paused, self._merge_simultaneous_pends_to_one_importer )
        
        serialisable_file_import_options = self._file_import_options.GetSerialisableTuple()
        serialisable_tag_import_options = self._tag_import_options.GetSerialisableTuple()
        
        serialisable_gallery_imports = self._gallery_imports.GetSerialisableTuple()
        
        return ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, self._file_limit, pend_options, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports )
        
    
    def _InitialiseFromSerialisableInfo( self, serialisable_info ):
        
        ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, self._file_limit, pend_options, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports ) = serialisable_info
        
        ( serialisable_gug_key, gug_name ) = serialisable_gug_key_and_name
        
        self._gug_key_and_name = ( bytes.fromhex( serialisable_gug_key ), gug_name )
        
        if serialisable_highlighted_gallery_import_key is None:
            
            self._highlighted_gallery_import_key = None
            
        else:
            
            self._highlighted_gallery_import_key = bytes.fromhex( serialisable_highlighted_gallery_import_key )
            
        
        ( self._start_file_queues_paused, self._start_gallery_queues_paused, self._merge_simultaneous_pends_to_one_importer ) = pend_options
        
        self._file_import_options = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_file_import_options )
        self._tag_import_options = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_tag_import_options )
        
        self._gallery_imports = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_gallery_imports )
        
        self._gallery_import_keys_to_gallery_imports = { gallery_import.GetGalleryImportKey() : gallery_import for gallery_import in self._gallery_imports }
        
    
    def _RegenerateStatus( self ):
        
        file_seed_caches = [ gallery_import.GetFileSeedCache() for gallery_import in self._gallery_imports ]
        
        self._status_cache = ClientImportFileSeeds.GenerateFileSeedCachesStatus( file_seed_caches )
        
        self._status_dirty = False
        
    
    def _RemoveGalleryImport( self, gallery_import_key ):
        
        if gallery_import_key not in self._gallery_import_keys_to_gallery_imports:
            
            return
            
        
        gallery_import = self._gallery_import_keys_to_gallery_imports[ gallery_import_key ]
        
        gallery_import.PublishToPage( False )
        gallery_import.Repage( 'dead page key' )
        
        self._gallery_imports.remove( gallery_import )
        
        self._last_time_imports_changed = HydrusData.GetNowPrecise()
        
        del self._gallery_import_keys_to_gallery_imports[ gallery_import_key ]
        
    
    def _SerialisableChangeMade( self ):
        
        self._last_serialisable_change_timestamp = HydrusData.GetNow()
        
    
    def _SetStatusDirty( self ):
        
        self._status_dirty = True
        
    
    def _UpdateSerialisableInfo( self, version, old_serialisable_info ):
        
        if version == 1:
            
            ( serialisable_gallery_identifier, serialisable_gallery_stream_identifiers, serialisable_current_query_stuff, pending_queries, get_tags_if_url_recognised_and_file_redundant, file_limit, gallery_paused, files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_file_seed_cache ) = old_serialisable_info
            
            new_serialisable_info = ( serialisable_gallery_identifier, serialisable_gallery_stream_identifiers, serialisable_current_query_stuff, pending_queries, file_limit, gallery_paused, files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_file_seed_cache )
            
            return ( 2, new_serialisable_info )
            
        
        if version == 2:
            
            ( serialisable_gallery_identifier, serialisable_gallery_stream_identifiers, serialisable_current_query_stuff, pending_queries, file_limit, gallery_paused, files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_file_seed_cache ) = old_serialisable_info
            
            gallery_seed_log = ClientImportGallerySeeds.GallerySeedLog()
            
            serialisable_gallery_seed_log = gallery_seed_log.GetSerialisableTuple()
            
            new_serialisable_info = ( serialisable_gallery_identifier, serialisable_gallery_stream_identifiers, serialisable_current_query_stuff, pending_queries, file_limit, gallery_paused, files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_seed_log, serialisable_file_seed_cache )
            
            return ( 3, new_serialisable_info )
            
        
        if version == 3:
            
            ( serialisable_gallery_identifier, serialisable_gallery_stream_identifiers, serialisable_current_query_stuff, pending_queries, file_limit, gallery_paused, files_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_seed_log, serialisable_file_seed_cache ) = old_serialisable_info
            
            ( current_query, current_query_num_new_urls, serialisable_current_gallery_stream_identifier, current_gallery_stream_identifier_page_index, serialisable_current_gallery_stream_identifier_found_urls, serialisable_pending_gallery_stream_identifiers ) = serialisable_current_query_stuff
            
            highlighted_gallery_import_key = None
            
            serialisable_highlighted_gallery_import_key = highlighted_gallery_import_key
            
            gallery_imports = HydrusSerialisable.SerialisableList()
            
            file_import_options = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_file_import_options )
            tag_import_options = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_tag_import_options )
            
            gallery_seed_log = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_gallery_seed_log )
            file_seed_cache = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_file_seed_cache )
            
            if len( file_seed_cache ) > 0:
                
                current_query = 'queue brought from old page'
                
                gallery_import = GalleryImport( query = current_query, source_name = 'updated from old system', initial_search_urls = [] )
                
                gallery_import.PausePlayGallery()
                gallery_import.PausePlayFiles()
                
                gallery_import.SetFileLimit( file_limit )
                
                gallery_import.SetFileImportOptions( file_import_options )
                gallery_import.SetTagImportOptions( tag_import_options )
                
                gallery_import.SetFileSeedCache( file_seed_cache )
                gallery_import.SetGallerySeedLog( gallery_seed_log )
                
                gallery_imports.append( gallery_import )
                
            
            serialisable_gallery_imports = gallery_imports.GetSerialisableTuple()
            
            new_serialisable_info = ( serialisable_gallery_identifier, serialisable_highlighted_gallery_import_key, file_limit, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports )
            
            return ( 4, new_serialisable_info )
            
        
        if version == 4:
            
            ( serialisable_gallery_identifier, serialisable_highlighted_gallery_import_key, file_limit, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports ) = old_serialisable_info
            
            gallery_identifier = HydrusSerialisable.CreateFromSerialisableTuple( serialisable_gallery_identifier )
            
            ( gug_key, gug_name ) = ClientDownloading.ConvertGalleryIdentifierToGUGKeyAndName( gallery_identifier )
            
            serialisable_gug_key_and_name = ( HydrusData.GenerateKey().hex(), gug_name )
            
            new_serialisable_info = ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, file_limit, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports )
            
            return ( 5, new_serialisable_info )
            
        
        if version == 5:
            
            ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, file_limit, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports ) = old_serialisable_info
            
            start_file_queues_paused = False
            start_gallery_queues_paused = False
            
            new_serialisable_info = ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, file_limit, start_file_queues_paused, start_gallery_queues_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports )
            
            return ( 6, new_serialisable_info )
            
        
        if version == 6:
            
            ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, file_limit, start_file_queues_paused, start_gallery_queues_paused, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports ) = old_serialisable_info
            
            merge_simultaneous_pends_to_one_importer = False
            
            pend_options = ( start_file_queues_paused, start_gallery_queues_paused, merge_simultaneous_pends_to_one_importer )
            
            new_serialisable_info = ( serialisable_gug_key_and_name, serialisable_highlighted_gallery_import_key, file_limit, pend_options, serialisable_file_import_options, serialisable_tag_import_options, serialisable_gallery_imports )
            
            return ( 7, new_serialisable_info )
            
        
    
    def ClearHighlightedGalleryImport( self ):
        
        with self._lock:
            
            if self._highlighted_gallery_import_key is not None:
                
                self._highlighted_gallery_import_key = None
                
                self._SerialisableChangeMade()
                
            
        
    
    def CurrentlyWorking( self ):
        
        with self._lock:
            
            return True in ( gallery_import.CurrentlyWorking() for gallery_import in self._gallery_imports )
            
        
    
    def GetAPIInfoDict( self, simple ):
        
        with self._lock:
            
            d = {}
            
            d[ 'gallery_imports' ] = [ gallery_import.GetAPIInfoDict( simple ) for gallery_import in self._gallery_imports ]
            
            if self._highlighted_gallery_import_key is None:
                
                d[ 'highlight' ] = None
                
            else:
                
                d[ 'highlight' ] = self._highlighted_gallery_import_key.hex()
                
            
            return d
            
        
    
    def GetFileLimit( self ):
        
        with self._lock:
            
            return self._file_limit
            
        
    
    def GetFileImportOptions( self ):
        
        with self._lock:
            
            return self._file_import_options
            
        
    
    def GetGalleryImports( self ):
        
        with self._lock:
            
            return list( self._gallery_imports )
            
        
    
    def GetGUGKeyAndName( self ):
        
        with self._lock:
            
            return self._gug_key_and_name
            
        
    
    def GetHighlightedGalleryImport( self ):
        
        with self._lock:
            
            if self._highlighted_gallery_import_key is not None:
                
                if self._highlighted_gallery_import_key in self._gallery_import_keys_to_gallery_imports:
                    
                    return self._gallery_import_keys_to_gallery_imports[ self._highlighted_gallery_import_key ]
                    
                
                self._highlighted_gallery_import_key = None
                
            
            return None
            
        
    
    def GetInitialSearchText( self ):
        
        return HG.client_controller.network_engine.domain_manager.GetInitialSearchText( self._gug_key_and_name )
        
    
    def GetLastTimeImportsChanged( self ):
        
        with self._lock:
            
            return self._last_time_imports_changed
            
        
    
    def GetNumGalleryImports( self ):
        
        with self._lock:
            
            return len( self._gallery_imports )
            
        
    
    def GetNumSeeds( self ):
        
        with self._lock:
            
            return sum( ( gallery_import.GetNumSeeds() for gallery_import in self._gallery_imports ) )
            
        
    
    def GetQueueStartSettings( self ):
        
        with self._lock:
            
            return ( self._start_file_queues_paused, self._start_gallery_queues_paused, self._merge_simultaneous_pends_to_one_importer )
            
        
    
    def GetTagImportOptions( self ):
        
        with self._lock:
            
            return self._tag_import_options
            
        
    
    def GetTotalStatus( self ) -> ClientImportFileSeeds.FileSeedCacheStatus:
        
        with self._lock:
            
            if self._status_dirty:
                
                self._RegenerateStatus()
                
            
            return self._status_cache
            
        
    
    def GetValueRange( self ):
        
        with self._lock:
            
            total_value = 0
            total_range = 0
            
            for gallery_import in self._gallery_imports:
                
                ( v, r ) = gallery_import.GetValueRange()
                
                if v != r:
                    
                    total_value += v
                    total_range += r
                    
                
            
            return ( total_value, total_range )
            
        
    
    def HasSerialisableChangesSince( self, since_timestamp ):
        
        with self._lock:
            
            if self._last_serialisable_change_timestamp > since_timestamp:
                
                return True
                
            
            for gallery_import in self._gallery_imports:
                
                if gallery_import.HasSerialisableChangesSince( since_timestamp ):
                    
                    return True
                    
                
            
            return False
            
        
    
    def PendSubscriptionGapDownloader( self, gug_key_and_name, query_text, file_import_options, tag_import_options, file_limit ):
        
        with self._lock:
            
            gug = HG.client_controller.network_engine.domain_manager.GetGUG( gug_key_and_name )
            
            if gug is None:
                
                HydrusData.ShowText( 'Could not find a Gallery URL Generator for "{}"!'.format( self._gug_key_and_name[1] ) )
                
                return
                
            
            initial_search_urls = gug.GenerateGalleryURLs( query_text )
            
            if len( initial_search_urls ) == 0:
                
                HydrusData.ShowText( 'The Gallery URL Generator "{}" did not produce any URLs!'.format( self._gug_key_and_name[1] ) )
                
                return
                
            
            gallery_import = GalleryImport( query = query_text, source_name = gug_key_and_name[1], initial_search_urls = initial_search_urls, start_file_queue_paused = self._start_file_queues_paused, start_gallery_queue_paused = self._start_gallery_queues_paused )
            
            gallery_import.SetFileLimit( file_limit )
            
            gallery_import.SetFileImportOptions( file_import_options )
            gallery_import.SetTagImportOptions( tag_import_options )
            
            publish_to_page = False
            
            if self._have_started:
                
                gallery_import.Start( self._page_key, publish_to_page )
                
            
            self._AddGalleryImport( gallery_import )
            
            ClientImporting.WakeRepeatingJob( self._importers_repeating_job )
            
            self._SetStatusDirty()
            
            self._SerialisableChangeMade()
            
        
    
    def PendQueries( self, query_texts ):
        
        created_importers = []
        
        with self._lock:
            
            gug = HG.client_controller.network_engine.domain_manager.GetGUG( self._gug_key_and_name )
            
            if gug is None:
                
                HydrusData.ShowText( 'Could not find a Gallery URL Generator for "{}"!'.format( self._gug_key_and_name[1] ) )
                
                return created_importers
                
            
            self._gug_key_and_name = gug.GetGUGKeyAndName() # just a refresher, to keep up with any changes
            
            groups_of_query_data = []
            
            for query_text in query_texts:
                
                initial_search_urls = gug.GenerateGalleryURLs( query_text )
                
                if len( initial_search_urls ) == 0:
                    
                    HydrusData.ShowText( 'The Gallery URL Generator "{}" did not produce any URLs!'.format( self._gug_key_and_name[1] ) )
                    
                    return created_importers
                    
                
                groups_of_query_data.append( ( query_text, initial_search_urls ) )
                
            
            if self._merge_simultaneous_pends_to_one_importer and len( groups_of_query_data ) > 1:
                
                # flatten these groups down to one
                
                all_search_urls_flat = []
                
                for ( query_text, initial_search_urls ) in groups_of_query_data:
                    
                    all_search_urls_flat.extend( initial_search_urls )
                    
                
                query_text = HydrusData.ToHumanInt( len( groups_of_query_data ) ) + ' queries'
                
                groups_of_query_data = [ ( query_text, all_search_urls_flat ) ]
                
            
            for ( query_text, initial_search_urls ) in groups_of_query_data:
                
                gallery_import = GalleryImport( query = query_text, source_name = self._gug_key_and_name[1], initial_search_urls = initial_search_urls, start_file_queue_paused = self._start_file_queues_paused, start_gallery_queue_paused = self._start_gallery_queues_paused )
                
                gallery_import.SetFileLimit( self._file_limit )
                
                gallery_import.SetFileImportOptions( self._file_import_options )
                gallery_import.SetTagImportOptions( self._tag_import_options )
                
                publish_to_page = False
                
                if self._have_started:
                    
                    gallery_import.Start( self._page_key, publish_to_page )
                    
                
                self._AddGalleryImport( gallery_import )
                
                created_importers.append( gallery_import )
                
            
            ClientImporting.WakeRepeatingJob( self._importers_repeating_job )
            
            self._SetStatusDirty()
            
            if len( created_importers ) > 0:
                
                self._SerialisableChangeMade()
                
            
        
        return created_importers
        
    
    def RemoveGalleryImport( self, gallery_import_key ):
        
        with self._lock:
            
            self._RemoveGalleryImport( gallery_import_key )
            
            self._SetStatusDirty()
            
            self._SerialisableChangeMade()
            
        
    
    def SetFileLimit( self, file_limit ):
        
        with self._lock:
            
            if file_limit != self._file_limit:
                
                self._file_limit = file_limit
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetFileImportOptions( self, file_import_options ):
        
        with self._lock:
            
            if self._file_import_options.DumpToString() != file_import_options.DumpToString():
                
                self._file_import_options = file_import_options
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetGUGKeyAndName( self, gug_key_and_name ):
        
        with self._lock:
            
            if gug_key_and_name != self._gug_key_and_name:
                
                self._gug_key_and_name = gug_key_and_name
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetHighlightedGalleryImport( self, highlighted_gallery_import: GalleryImport ):
        
        with self._lock:
            
            highlighted_gallery_import_key = highlighted_gallery_import.GetGalleryImportKey()
            
            if highlighted_gallery_import_key != self._highlighted_gallery_import_key:
                
                self._highlighted_gallery_import_key = highlighted_gallery_import.GetGalleryImportKey()
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetQueueStartSettings( self, start_file_queues_paused, start_gallery_queues_paused, merge_simultaneous_pends_to_one_importer ):
        
        with self._lock:
            
            if start_file_queues_paused != self._start_file_queues_paused:
                
                self._start_file_queues_paused = start_file_queues_paused
                
                self._SerialisableChangeMade()
                
            
            if start_gallery_queues_paused != self._start_gallery_queues_paused:
                
                self._start_gallery_queues_paused = start_gallery_queues_paused
                
                self._SerialisableChangeMade()
                
            
            if merge_simultaneous_pends_to_one_importer != self._merge_simultaneous_pends_to_one_importer:
                
                self._merge_simultaneous_pends_to_one_importer = merge_simultaneous_pends_to_one_importer
                
                self._SerialisableChangeMade()
                
            
        
    
    def SetTagImportOptions( self, tag_import_options ):
        
        with self._lock:
            
            if tag_import_options.DumpToString() != self._tag_import_options.DumpToString():
                
                self._tag_import_options = tag_import_options
                
                self._SerialisableChangeMade()
                
            
        
    
    def Start( self, page_key ):
        
        with self._lock:
            
            if self._have_started:
                
                return
                
            
            self._page_key = page_key
            
            # set a 2s period so the page value/range is breddy snappy
            self._importers_repeating_job = HG.client_controller.CallRepeating( ClientImporting.GetRepeatingJobInitialDelay(), 2.0, self.REPEATINGWorkOnImporters )
            
            for gallery_import in self._gallery_imports:
                
                publish_to_page = gallery_import.GetGalleryImportKey() == self._highlighted_gallery_import_key
                
                gallery_import.Start( page_key, publish_to_page )
                
            
            self._have_started = True
            
        
    
    def REPEATINGWorkOnImporters( self ):
        
        with self._lock:
            
            if ClientImporting.PageImporterShouldStopWorking( self._page_key ):
                
                self._importers_repeating_job.Cancel()
                
                return
                
            
            if not self._status_dirty: # if we think we are clean
                
                for gallery_import in self._gallery_imports:
                    
                    file_seed_cache = gallery_import.GetFileSeedCache()
                    
                    if file_seed_cache.GetStatus().GetGenerationTime() > self._status_cache.GetGenerationTime(): # has there has been an update?
                        
                        self._SetStatusDirty()
                        
                        break
                        
                    
                
            
        
        if HydrusData.TimeHasPassed( self._next_pub_value_check_time ):
            
            self._next_pub_value_check_time = HydrusData.GetNow() + 5
            
            current_value_range = self.GetValueRange()
            
            if current_value_range != self._last_pubbed_value_range:
                
                self._last_pubbed_value_range = current_value_range
                
                HG.client_controller.pub( 'refresh_page_name', self._page_key )
                
            
        
    
HydrusSerialisable.SERIALISABLE_TYPES_TO_OBJECT_TYPES[ HydrusSerialisable.SERIALISABLE_TYPE_MULTIPLE_GALLERY_IMPORT ] = MultipleGalleryImport
