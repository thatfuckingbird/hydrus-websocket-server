import collections
import os
import random
import unittest

from unittest.mock import patch

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusTags

from hydrus.client import ClientConstants as CC
from hydrus.client.importing import ClientImportFileSeeds
from hydrus.client.importing.options import ClientImportOptions
from hydrus.client.importing.options import FileImportOptions
from hydrus.client.importing.options import NoteImportOptions
from hydrus.client.importing.options import PresentationImportOptions
from hydrus.client.importing.options import TagImportOptions
from hydrus.client.media import ClientMedia
from hydrus.client.media import ClientMediaManagers
from hydrus.client.media import ClientMediaResult
from hydrus.client.metadata import ClientTags

class TestCheckerOptions( unittest.TestCase ):
    
    def test_checker_options( self ):
        
        regular_checker_options = ClientImportOptions.CheckerOptions( intended_files_per_check = 5, never_faster_than = 30, never_slower_than = 86400, death_file_velocity = ( 1, 86400 ) )
        fast_checker_options = ClientImportOptions.CheckerOptions( intended_files_per_check = 2, never_faster_than = 30, never_slower_than = 86400, death_file_velocity = ( 1, 86400 ) )
        slow_checker_options = ClientImportOptions.CheckerOptions( intended_files_per_check = 10, never_faster_than = 30, never_slower_than = 86400, death_file_velocity = ( 1, 86400 ) )
        callous_checker_options = ClientImportOptions.CheckerOptions( intended_files_per_check = 5, never_faster_than = 30, never_slower_than = 86400, death_file_velocity = ( 1, 60 ) )
        
        empty_file_seed_cache = ClientImportFileSeeds.FileSeedCache()
        
        file_seed_cache = ClientImportFileSeeds.FileSeedCache()
        
        last_check_time = 10000000
        
        one_day_before = last_check_time - 86400
        
        for i in range( 50 ):
            
            url = 'https://wew.lad/' + os.urandom( 16 ).hex()
            
            file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
            
            file_seed.source_time = one_day_before - 10
            
            file_seed_cache.AddFileSeeds( ( file_seed, ) )
            
        
        for i in range( 50 ):
            
            url = 'https://wew.lad/' + os.urandom( 16 ).hex()
            
            file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
            
            file_seed.source_time = last_check_time - 600
            
            file_seed_cache.AddFileSeeds( ( file_seed, ) )
            
        
        bare_file_seed_cache = ClientImportFileSeeds.FileSeedCache()
        
        url = 'https://wew.lad/' + 'early'
        
        file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
        
        file_seed.source_time = one_day_before - 10
        
        bare_file_seed_cache.AddFileSeeds( ( file_seed, ) )
        
        url = 'https://wew.lad/' + 'in_time_delta'
        
        file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
        
        file_seed.source_time = one_day_before + 10
        
        bare_file_seed_cache.AddFileSeeds( ( file_seed, ) )
        
        busy_file_seed_cache = ClientImportFileSeeds.FileSeedCache()
        
        url = 'https://wew.lad/' + 'early'
        
        file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
        
        file_seed.source_time = one_day_before - 10
        
        busy_file_seed_cache.AddFileSeeds( ( file_seed, ) )
        
        for i in range( 8640 ):
            
            url = 'https://wew.lad/' + os.urandom( 16 ).hex()
            
            file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
            
            file_seed.source_time = one_day_before + ( ( i + 1 ) * 10 ) - 1
            
            busy_file_seed_cache.AddFileSeeds( ( file_seed, ) )
            
        
        new_thread_file_seed_cache = ClientImportFileSeeds.FileSeedCache()
        
        for i in range( 10 ):
            
            url = 'https://wew.lad/' + os.urandom( 16 ).hex()
            
            file_seed = ClientImportFileSeeds.FileSeed( ClientImportFileSeeds.FILE_SEED_TYPE_URL, url )
            
            file_seed.source_time = last_check_time - 600
            
            new_thread_file_seed_cache.AddFileSeeds( ( file_seed, ) )
            
        
        # empty
        # should say ok if last_check_time is 0, so it can initialise
        # otherwise sperg out safely
        
        self.assertFalse( regular_checker_options.IsDead( empty_file_seed_cache, 0 ) )
        
        self.assertEqual( regular_checker_options.GetPrettyCurrentVelocity( empty_file_seed_cache, 0 ), 'no files yet' )
        
        self.assertEqual( regular_checker_options.GetNextCheckTime( empty_file_seed_cache, 0, 0 ), 0 )
        
        self.assertTrue( regular_checker_options.IsDead( empty_file_seed_cache, last_check_time ) )
        
        self.assertEqual( regular_checker_options.GetPrettyCurrentVelocity( empty_file_seed_cache, last_check_time ), 'no files, unable to determine velocity' )
        
        # regular
        # current velocity should be 50 files per day for the day ones and 0 files per min for the callous minute one
        
        self.assertFalse( regular_checker_options.IsDead( file_seed_cache, last_check_time ) )
        self.assertFalse( fast_checker_options.IsDead( file_seed_cache, last_check_time ) )
        self.assertFalse( slow_checker_options.IsDead( file_seed_cache, last_check_time ) )
        self.assertTrue( callous_checker_options.IsDead( file_seed_cache, last_check_time ) )
        
        self.assertEqual( regular_checker_options.GetPrettyCurrentVelocity( file_seed_cache, last_check_time ), 'at last check, found 50 files in previous 1 day' )
        self.assertEqual( fast_checker_options.GetPrettyCurrentVelocity( file_seed_cache, last_check_time ), 'at last check, found 50 files in previous 1 day' )
        self.assertEqual( slow_checker_options.GetPrettyCurrentVelocity( file_seed_cache, last_check_time ), 'at last check, found 50 files in previous 1 day' )
        self.assertEqual( callous_checker_options.GetPrettyCurrentVelocity( file_seed_cache, last_check_time ), 'at last check, found 0 files in previous 1 minute' )
        
        self.assertEqual( regular_checker_options.GetNextCheckTime( file_seed_cache, last_check_time, 0 ), last_check_time + 8640 )
        self.assertEqual( fast_checker_options.GetNextCheckTime( file_seed_cache, last_check_time, 0 ), last_check_time + 3456 )
        self.assertEqual( slow_checker_options.GetNextCheckTime( file_seed_cache, last_check_time, 0 ), last_check_time + 17280 )
        
        # bare
        # 1 files per day
        
        self.assertFalse( regular_checker_options.IsDead( bare_file_seed_cache, last_check_time ) )
        self.assertTrue( callous_checker_options.IsDead( bare_file_seed_cache, last_check_time ) )
        
        self.assertEqual( regular_checker_options.GetPrettyCurrentVelocity( bare_file_seed_cache, last_check_time ), 'at last check, found 1 files in previous 1 day' )
        
        self.assertEqual( regular_checker_options.GetNextCheckTime( bare_file_seed_cache, last_check_time, 0 ), last_check_time + 86400 )
        self.assertEqual( fast_checker_options.GetNextCheckTime( bare_file_seed_cache, last_check_time, 0 ), last_check_time + 86400 )
        self.assertEqual( slow_checker_options.GetNextCheckTime( bare_file_seed_cache, last_check_time, 0 ), last_check_time + 86400 )
        
        # busy
        # 8640 files per day, 6 files per minute
        
        self.assertFalse( regular_checker_options.IsDead( busy_file_seed_cache, last_check_time ) )
        self.assertFalse( fast_checker_options.IsDead( busy_file_seed_cache, last_check_time ) )
        self.assertFalse( slow_checker_options.IsDead( busy_file_seed_cache, last_check_time ) )
        self.assertFalse( callous_checker_options.IsDead( busy_file_seed_cache, last_check_time ) )
        
        self.assertEqual( regular_checker_options.GetPrettyCurrentVelocity( busy_file_seed_cache, last_check_time ), 'at last check, found 8,640 files in previous 1 day' )
        self.assertEqual( callous_checker_options.GetPrettyCurrentVelocity( busy_file_seed_cache, last_check_time ), 'at last check, found 6 files in previous 1 minute' )
        
        self.assertEqual( regular_checker_options.GetNextCheckTime( busy_file_seed_cache, last_check_time, 0 ), last_check_time + 50 )
        self.assertEqual( fast_checker_options.GetNextCheckTime( busy_file_seed_cache, last_check_time, 0 ), last_check_time + 30 )
        self.assertEqual( slow_checker_options.GetNextCheckTime( busy_file_seed_cache, last_check_time, 0 ), last_check_time + 100 )
        self.assertEqual( callous_checker_options.GetNextCheckTime( busy_file_seed_cache, last_check_time, 0 ), last_check_time + 50 )
        
        # new thread
        # only had files from ten mins ago, so timings are different
        
        self.assertFalse( regular_checker_options.IsDead( new_thread_file_seed_cache, last_check_time ) )
        self.assertFalse( fast_checker_options.IsDead( new_thread_file_seed_cache, last_check_time ) )
        self.assertFalse( slow_checker_options.IsDead( new_thread_file_seed_cache, last_check_time ) )
        self.assertTrue( callous_checker_options.IsDead( new_thread_file_seed_cache, last_check_time ) )
        
        self.assertEqual( regular_checker_options.GetPrettyCurrentVelocity( new_thread_file_seed_cache, last_check_time ), 'at last check, found 10 files in previous 10 minutes' )
        self.assertEqual( fast_checker_options.GetPrettyCurrentVelocity( new_thread_file_seed_cache, last_check_time ), 'at last check, found 10 files in previous 10 minutes' )
        self.assertEqual( slow_checker_options.GetPrettyCurrentVelocity( new_thread_file_seed_cache, last_check_time ), 'at last check, found 10 files in previous 10 minutes' )
        self.assertEqual( callous_checker_options.GetPrettyCurrentVelocity( new_thread_file_seed_cache, last_check_time ), 'at last check, found 0 files in previous 1 minute' )
        
        # these would be 360, 120, 600, but the 'don't check faster the time since last file post' bumps this up
        self.assertEqual( regular_checker_options.GetNextCheckTime( new_thread_file_seed_cache, last_check_time, 0 ), last_check_time + 600 )
        self.assertEqual( fast_checker_options.GetNextCheckTime( new_thread_file_seed_cache, last_check_time, 0 ), last_check_time + 600 )
        self.assertEqual( slow_checker_options.GetNextCheckTime( new_thread_file_seed_cache, last_check_time, 0 ), last_check_time + 600 )
        
        # Let's test these new static timings, where if faster_than == slower_than, we just add that period to the 'last_next_check_time' (e.g. checking every sunday night)
        
        static_checker_options = ClientImportOptions.CheckerOptions( intended_files_per_check = 5, never_faster_than = 3600, never_slower_than = 3600, death_file_velocity = ( 1, 3600 ) )
        
        self.assertTrue( static_checker_options.IsDead( bare_file_seed_cache, last_check_time ) )
        
        last_next_check_time = last_check_time - 200
        
        with patch.object( HydrusData, 'GetNow', return_value = last_check_time + 10 ):
            
            self.assertEqual( static_checker_options.GetNextCheckTime( new_thread_file_seed_cache, last_check_time, last_next_check_time ), last_next_check_time + 3600 )
            
        
    
class TestFileImportOptions( unittest.TestCase ):
    
    def test_file_import_options( self ):
        
        file_import_options = FileImportOptions.FileImportOptions()
        
        exclude_deleted = False
        do_not_check_known_urls_before_importing = False
        do_not_check_hashes_before_importing = False
        allow_decompression_bombs = False
        min_size = None
        max_size = None
        max_gif_size = None
        min_resolution = None
        max_resolution = None
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        automatic_archive = False
        associate_primary_urls = False
        associate_source_urls = False
        
        file_import_options.SetPostImportOptions( automatic_archive, associate_primary_urls, associate_source_urls )
        
        #
        
        self.assertFalse( file_import_options.ExcludesDeleted() )
        self.assertFalse( file_import_options.AllowsDecompressionBombs() )
        self.assertFalse( file_import_options.AutomaticallyArchives() )
        self.assertFalse( file_import_options.ShouldAssociatePrimaryURLs() )
        self.assertFalse( file_import_options.ShouldAssociateSourceURLs() )
        
        file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 480 )
        file_import_options.CheckFileIsValid( 65536, HC.APPLICATION_7Z, None, None )
        
        #
        
        exclude_deleted = True
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        self.assertTrue( file_import_options.ExcludesDeleted() )
        self.assertFalse( file_import_options.AllowsDecompressionBombs() )
        self.assertFalse( file_import_options.AutomaticallyArchives() )
        
        #
        
        allow_decompression_bombs = True
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        self.assertTrue( file_import_options.ExcludesDeleted() )
        self.assertTrue( file_import_options.AllowsDecompressionBombs() )
        self.assertFalse( file_import_options.AutomaticallyArchives() )
        
        #
        
        automatic_archive = True
        associate_primary_urls = True
        associate_source_urls  = True
        
        file_import_options.SetPostImportOptions( automatic_archive, associate_primary_urls, associate_source_urls )
        
        self.assertTrue( file_import_options.ExcludesDeleted() )
        self.assertTrue( file_import_options.AllowsDecompressionBombs() )
        self.assertTrue( file_import_options.AutomaticallyArchives() )
        self.assertTrue( file_import_options.ShouldAssociatePrimaryURLs() )
        self.assertTrue( file_import_options.ShouldAssociateSourceURLs() )
        
        #
        
        min_size = 4096
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 480 )
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 512, HC.IMAGE_JPEG, 640, 480 )
            
        
        #
        
        min_size = None
        max_size = 2000
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        file_import_options.CheckFileIsValid( 1800, HC.IMAGE_JPEG, 640, 480 )
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 2200, HC.IMAGE_JPEG, 640, 480 )
            
        
        #
        
        max_size = None
        max_gif_size = 2000
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        file_import_options.CheckFileIsValid( 1800, HC.IMAGE_JPEG, 640, 480 )
        file_import_options.CheckFileIsValid( 2200, HC.IMAGE_JPEG, 640, 480 )
        
        file_import_options.CheckFileIsValid( 1800, HC.IMAGE_GIF, 640, 480 )
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 2200, HC.IMAGE_GIF, 640, 480 )
            
        
        #
        
        max_gif_size = None
        min_resolution = ( 200, 100 )
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 480 )
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 180, 480 )
            
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 80 )
            
        
        file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 180 )
        
        #
        
        min_resolution = None
        max_resolution = ( 3000, 4000 )
        
        file_import_options.SetPreImportOptions( exclude_deleted, do_not_check_known_urls_before_importing, do_not_check_hashes_before_importing, allow_decompression_bombs, min_size, max_size, max_gif_size, min_resolution, max_resolution )
        
        file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 480 )
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 3200, 480 )
            
        
        with self.assertRaises( HydrusExceptions.FileSizeException ):
            
            file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 640, 4200 )
            
        
        file_import_options.CheckFileIsValid( 65536, HC.IMAGE_JPEG, 2800, 3800 )
        
    
def GetNotesMediaResult( hash, names_to_notes ):
    
    file_id = 123
    size = random.randint( 8192, 20 * 1048576 )
    mime = random.choice( [ HC.IMAGE_JPEG, HC.VIDEO_WEBM, HC.APPLICATION_PDF ] )
    width = random.randint( 200, 4096 )
    height = random.randint( 200, 4096 )
    duration = random.choice( [ 220, 16.66667, None ] )
    has_audio = random.choice( [ True, False ] )
    
    file_info_manager = ClientMediaManagers.FileInfoManager( file_id, hash, size = size, mime = mime, width = width, height = height, duration = duration, has_audio = has_audio )
    
    service_keys_to_statuses_to_tags = collections.defaultdict( HydrusData.default_dict_set )
    service_keys_to_statuses_to_display_tags = collections.defaultdict( HydrusData.default_dict_set )
    
    tags_manager = ClientMediaManagers.TagsManager( service_keys_to_statuses_to_tags, service_keys_to_statuses_to_display_tags )
    
    locations_manager = ClientMediaManagers.LocationsManager( dict(), dict(), set(), set(), inbox = True )
    ratings_manager = ClientMediaManagers.RatingsManager( {} )
    notes_manager = ClientMediaManagers.NotesManager( names_to_notes )
    file_viewing_stats_manager = ClientMediaManagers.FileViewingStatsManager( 0, 0, 0, 0 )
    
    media_result = ClientMediaResult.MediaResult( file_info_manager, tags_manager, locations_manager, ratings_manager, notes_manager, file_viewing_stats_manager )
    
    return media_result
    
class TestNoteImportOptions( unittest.TestCase ):
    
    def test_basics( self ):
        
        example_hash = HydrusData.GenerateKey()
        existing_names_to_notes = { 'notes' : 'here is a note' }
        
        media_result = GetNotesMediaResult( example_hash, existing_names_to_notes )
        
        #
        
        note_import_options = NoteImportOptions.NoteImportOptions()
        
        note_import_options.SetGetNotes( True )
        note_import_options.SetExtendExistingNoteIfPossible( True )
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_IGNORE )
        
        self.assertEqual( note_import_options.GetServiceKeysToContentUpdates( media_result, [] ), {} )
        
        #
        
        names_and_notes = [ ( 'test', 'yes' ) ]
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, names_and_notes )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'test', 'yes' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
        note_import_options.SetGetNotes( False )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, names_and_notes )
        
        self.assertEqual( result, {} )
        
        note_import_options.SetGetNotes( True )
        
        #
        
        extending_names_and_notes = [ ( 'notes', 'and here is a note that is more interesting' ) ]
        
        note_import_options.SetExtendExistingNoteIfPossible( True )
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_IGNORE )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, extending_names_and_notes )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'notes', 'and here is a note that is more interesting' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
        note_import_options.SetExtendExistingNoteIfPossible( False )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, extending_names_and_notes )
        
        self.assertEqual( result, {} )
        
        #
        
        conflict_names_and_notes = [ ( 'notes', 'other note' ) ]
        
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_IGNORE )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, conflict_names_and_notes )
        
        self.assertEqual( result, {} )
        
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_REPLACE )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, conflict_names_and_notes )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'notes', 'other note' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_RENAME )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, conflict_names_and_notes )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'notes (1)', 'other note' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_APPEND )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, conflict_names_and_notes )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'notes', 'here is a note' + os.linesep * 2 + 'other note' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
        #
        
        multinotes = [ ( 'notes', 'other note' ), ( 'b', 'bbb' ), ( 'c', 'ccc' ) ]
        
        note_import_options.SetConflictResolution( NoteImportOptions.NOTE_IMPORT_CONFLICT_IGNORE )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, multinotes )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'b', 'bbb' ) ), HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'c', 'ccc' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
        #
        
        renames = [ ( 'a', 'aaa' ), ( 'wew', 'wew note' ) ]
        
        note_import_options.SetNameOverrides( 'override', { 'wew' : 'lad' } )
        
        result = note_import_options.GetServiceKeysToContentUpdates( media_result, renames )
        expected_result = { CC.LOCAL_NOTES_SERVICE_KEY : [ HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'override', 'aaa' ) ), HydrusData.ContentUpdate( HC.CONTENT_TYPE_NOTES, HC.CONTENT_UPDATE_SET, ( example_hash, 'lad', 'wew note' ) ) ] }
        
        self.assertEqual( result, expected_result )
        
    
def GetTagsMediaResult( hash, in_inbox, service_key, deleted_tags ):
    
    file_id = 123
    size = random.randint( 8192, 20 * 1048576 )
    mime = random.choice( [ HC.IMAGE_JPEG, HC.VIDEO_WEBM, HC.APPLICATION_PDF ] )
    width = random.randint( 200, 4096 )
    height = random.randint( 200, 4096 )
    duration = random.choice( [ 220, 16.66667, None ] )
    has_audio = random.choice( [ True, False ] )
    
    file_info_manager = ClientMediaManagers.FileInfoManager( file_id, hash, size = size, mime = mime, width = width, height = height, duration = duration, has_audio = has_audio )
    
    service_keys_to_statuses_to_tags = collections.defaultdict( HydrusData.default_dict_set )
    
    service_keys_to_statuses_to_tags[ service_key ] = { HC.CONTENT_STATUS_DELETED : deleted_tags }
    
    service_keys_to_statuses_to_display_tags = collections.defaultdict( HydrusData.default_dict_set )
    
    tags_manager = ClientMediaManagers.TagsManager( service_keys_to_statuses_to_tags, service_keys_to_statuses_to_display_tags )
    
    locations_manager = ClientMediaManagers.LocationsManager( dict(), dict(), set(), set(), inbox = in_inbox )
    ratings_manager = ClientMediaManagers.RatingsManager( {} )
    notes_manager = ClientMediaManagers.NotesManager( {} )
    file_viewing_stats_manager = ClientMediaManagers.FileViewingStatsManager( 0, 0, 0, 0 )
    
    media_result = ClientMediaResult.MediaResult( file_info_manager, tags_manager, locations_manager, ratings_manager, notes_manager, file_viewing_stats_manager )
    
    return media_result
    
class TestPresentationImportOptions( unittest.TestCase ):
    
    def test_presentation_import_options( self ):
        
        new_and_inboxed_hash = HydrusData.GenerateKey()
        new_and_archived_hash = HydrusData.GenerateKey()
        already_in_and_inboxed_hash = HydrusData.GenerateKey()
        already_in_and_archived_hash = HydrusData.GenerateKey()
        new_and_inboxed_but_trashed_hash = HydrusData.GenerateKey()
        skipped_hash = HydrusData.GenerateKey()
        deleted_hash = HydrusData.GenerateKey()
        failed_hash = HydrusData.GenerateKey()
        
        hashes_and_statuses = [
            ( new_and_inboxed_hash, CC.STATUS_SUCCESSFUL_AND_NEW ),
            ( new_and_archived_hash, CC.STATUS_SUCCESSFUL_AND_NEW ),
            ( already_in_and_inboxed_hash, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT ),
            ( already_in_and_archived_hash, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT ),
            ( new_and_inboxed_but_trashed_hash, CC.STATUS_SUCCESSFUL_AND_NEW ),
            ( skipped_hash, CC.STATUS_SKIPPED ),
            ( deleted_hash, CC.STATUS_DELETED ),
            ( failed_hash, CC.STATUS_ERROR )
        ]
        
        # all good
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_ANY_GOOD )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_AGNOSTIC )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
            already_in_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
            already_in_and_archived_hash
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', 'not used' )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
        # all good and trash too
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_ANY_GOOD )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_AGNOSTIC )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_TRASH_TOO )
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
            already_in_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
            already_in_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', 'not used' )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.COMBINED_LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
        # silent
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_NONE )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_AGNOSTIC )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        expected_result = []
        
        HG.test_controller.SetRead( 'inbox_hashes', 'not used' )
        HG.test_controller.SetRead( 'filter_hashes', 'not used' )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        self.assertEqual( result, expected_result )
        
        # new files only
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_NEW_ONLY )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_AGNOSTIC )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', 'not used' )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
        # inbox only
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_ANY_GOOD )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_REQUIRE_INBOX )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        pre_inbox_filter_expected_result = {
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
            already_in_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        }
        
        inbox_filter_answer = {
            new_and_inboxed_hash,
            already_in_and_inboxed_hash,
            new_and_inboxed_but_trashed_hash
        }
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            already_in_and_inboxed_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash,
            already_in_and_inboxed_hash
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', inbox_filter_answer )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'inbox_hashes' )
        
        self.assertEqual( args, ( pre_inbox_filter_expected_result, ) )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
        # new only
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_NEW_ONLY )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_AGNOSTIC )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', 'not used' )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
        # new and inbox only
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_NEW_ONLY )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_REQUIRE_INBOX )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        pre_inbox_filter_expected_result = {
            new_and_inboxed_hash,
            new_and_archived_hash,
            new_and_inboxed_but_trashed_hash
        }
        
        inbox_filter_answer = {
            new_and_inboxed_hash,
            new_and_inboxed_but_trashed_hash
        }
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', inbox_filter_answer )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'inbox_hashes' )
        
        self.assertEqual( args, ( pre_inbox_filter_expected_result, ) )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
        # new or inbox only
        
        HG.test_controller.ClearReads( 'inbox_hashes' )
        HG.test_controller.ClearReads( 'file_query_ids' )
        
        presentation_import_options = PresentationImportOptions.PresentationImportOptions()
        
        presentation_import_options.SetPresentationStatus( PresentationImportOptions.PRESENTATION_STATUS_NEW_ONLY )
        presentation_import_options.SetPresentationInbox( PresentationImportOptions.PRESENTATION_INBOX_INCLUDE_INBOX )
        presentation_import_options.SetPresentationLocation( PresentationImportOptions.PRESENTATION_LOCATION_IN_LOCAL_FILES )
        
        pre_inbox_filter_expected_result = {
            already_in_and_inboxed_hash,
            already_in_and_archived_hash
        }
        
        inbox_filter_answer = {
            already_in_and_inboxed_hash
        }
        
        pre_filter_expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
            new_and_inboxed_but_trashed_hash
        ]
        
        expected_result = [
            new_and_inboxed_hash,
            new_and_archived_hash,
            already_in_and_inboxed_hash,
        ]
        
        HG.test_controller.SetRead( 'inbox_hashes', inbox_filter_answer )
        HG.test_controller.SetRead( 'filter_hashes', expected_result )
        
        result = presentation_import_options.GetPresentedHashes( hashes_and_statuses )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'inbox_hashes' )
        
        self.assertEqual( args, ( pre_inbox_filter_expected_result, ) )
        
        [ ( args, kwargs ) ] = HG.test_controller.GetRead( 'filter_hashes' )
        
        self.assertEqual( args, ( CC.LOCAL_FILE_SERVICE_KEY, pre_filter_expected_result ) )
        
        self.assertEqual( result, expected_result )
        
    
class TestTagImportOptions( unittest.TestCase ):
    
    def test_basics( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HG.test_controller.example_tag_repo_service_key
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        
        #
        
        default_tag_import_options = TagImportOptions.TagImportOptions()
        
        self.assertEqual( default_tag_import_options.ShouldFetchTagsEvenIfURLKnownAndFileAlreadyInDB(), False )
        self.assertEqual( default_tag_import_options.ShouldFetchTagsEvenIfHashKnownAndFileAlreadyInDB(), False )
        
        blacklist = default_tag_import_options.GetTagBlacklist()
        
        self.assertEqual( blacklist.Filter( some_tags ), some_tags )
        
        whitelist = default_tag_import_options.GetTagWhitelist()
        
        self.assertEqual( whitelist, [] )
        
        self.assertEqual( default_tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), {} )
        
        #
        
        tag_import_options = TagImportOptions.TagImportOptions( fetch_tags_even_if_url_recognised_and_file_already_in_db = True )
        
        self.assertEqual( tag_import_options.ShouldFetchTagsEvenIfURLKnownAndFileAlreadyInDB(), True )
        self.assertEqual( tag_import_options.ShouldFetchTagsEvenIfHashKnownAndFileAlreadyInDB(), False )
        
        #
        
        tag_import_options = TagImportOptions.TagImportOptions( fetch_tags_even_if_hash_recognised_and_file_already_in_db = True )
        
        self.assertEqual( tag_import_options.ShouldFetchTagsEvenIfURLKnownAndFileAlreadyInDB(), False )
        self.assertEqual( tag_import_options.ShouldFetchTagsEvenIfHashKnownAndFileAlreadyInDB(), True )
        
    
    def test_blacklist( self ):
        
        example_service_key = HG.test_controller.example_tag_repo_service_key
        
        tag_blacklist = HydrusTags.TagFilter()
        
        tag_blacklist.SetRule( 'series:', HC.FILTER_BLACKLIST )
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( tag_blacklist = tag_blacklist, service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        with self.assertRaises( HydrusExceptions.VetoException ):
            
            tag_import_options.CheckTagsVeto( { 'bodysuit', 'series:metroid' }, set() )
            
        
        with self.assertRaises( HydrusExceptions.VetoException ):
            
            tag_import_options.CheckTagsVeto( { 'bodysuit' }, { 'series:metroid' } )
            
        
        tag_import_options.CheckTagsVeto( { 'bodysuit' }, set() )
        
    
    def test_whitelist( self ):
        
        example_service_key = HG.test_controller.example_tag_repo_service_key
        
        tag_whitelist = [ 'bodysuit' ]
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( tag_whitelist = tag_whitelist, service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        with self.assertRaises( HydrusExceptions.VetoException ):
            
            tag_import_options.CheckTagsVeto( { 'series:metroid' }, set() )
            
        
        tag_import_options.CheckTagsVeto( { 'bodysuit', 'series:metroid' }, set() )
        tag_import_options.CheckTagsVeto( { 'series:metroid' }, { 'bodysuit' } )
        
    
    def test_external_tags( self ):
        
        some_tags = set()
        example_hash = HydrusData.GenerateKey()
        example_service_key = HG.test_controller.example_tag_repo_service_key
        
        external_filterable_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        external_additional_service_keys_to_tags = { example_service_key : { 'series:evangelion' } }
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        
        #
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags, external_filterable_tags = external_filterable_tags, external_additional_service_keys_to_tags = external_additional_service_keys_to_tags )
        
        self.assertIn( example_service_key, result )
        
        self.assertEqual( len( result ), 1 )
        
        content_updates = result[ example_service_key ]
        
        filtered_tags = { 'bodysuit', 'character:samus aran', 'series:metroid', 'series:evangelion' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates }
        
        self.assertEqual( result_tags, filtered_tags )
        
        #
        
        get_tags_filter = HydrusTags.TagFilter()
        
        get_tags_filter.SetRule( 'series:', HC.FILTER_BLACKLIST )
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_filter = get_tags_filter ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags, external_filterable_tags = external_filterable_tags, external_additional_service_keys_to_tags = external_additional_service_keys_to_tags )
        
        self.assertIn( example_service_key, result )
        
        self.assertEqual( len( result ), 1 )
        
        content_updates = result[ example_service_key ]
        
        filtered_tags = { 'bodysuit', 'character:samus aran', 'series:evangelion' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates }
        
        self.assertEqual( result_tags, filtered_tags )
        
    
    def test_services( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key_1 = CC.DEFAULT_LOCAL_TAG_SERVICE_KEY
        example_service_key_2 = HG.test_controller.example_tag_repo_service_key
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key_1, set() )
        
        #
        
        service_keys_to_service_tag_import_options = {}
        
        service_keys_to_service_tag_import_options[ example_service_key_1 ] = TagImportOptions.ServiceTagImportOptions( get_tags = True )
        service_keys_to_service_tag_import_options[ example_service_key_2 ] = TagImportOptions.ServiceTagImportOptions( get_tags = False )
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags )
        
        self.assertIn( example_service_key_1, result )
        self.assertNotIn( example_service_key_2, result )
        
        self.assertTrue( len( result ) == 1 )
        
        content_updates_1 = result[ example_service_key_1 ]
        
        filtered_tags = { 'bodysuit', 'character:samus aran', 'series:evangelion' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates_1 }
        
    
    def test_overwrite_deleted_filterable( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HG.test_controller.example_tag_repo_service_key
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, { 'bodysuit', 'series:metroid' } )
        
        #
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags )
        
        self.assertIn( example_service_key, result )
        
        self.assertEqual( len( result ), 1 )
        
        content_updates = result[ example_service_key ]
        
        filtered_tags = { 'character:samus aran' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates }
        
        self.assertEqual( result_tags, filtered_tags )
        
        #
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_overwrite_deleted = True ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags )
        
        self.assertIn( example_service_key, result )
        
        self.assertEqual( len( result ), 1 )
        
        content_updates = result[ example_service_key ]
        
        filtered_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates }
        
        self.assertEqual( result_tags, filtered_tags )
        
    
    def test_overwrite_deleted_additional( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HG.test_controller.example_tag_repo_service_key
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, { 'bodysuit', 'series:metroid' } )
        
        #
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True, additional_tags = some_tags ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags )
        
        self.assertIn( example_service_key, result )
        
        self.assertEqual( len( result ), 1 )
        
        content_updates = result[ example_service_key ]
        
        filtered_tags = { 'character:samus aran' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates }
        
        self.assertEqual( result_tags, filtered_tags )
        
        #
        
        service_keys_to_service_tag_import_options = { example_service_key : TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_overwrite_deleted = True, additional_tags = some_tags ) }
        
        tag_import_options = TagImportOptions.TagImportOptions( service_keys_to_service_tag_import_options = service_keys_to_service_tag_import_options )
        
        result = tag_import_options.GetServiceKeysToContentUpdates( CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags )
        
        self.assertIn( example_service_key, result )
        
        self.assertEqual( len( result ), 1 )
        
        content_updates = result[ example_service_key ]
        
        filtered_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        result_tags = { c_u.GetRow()[0] for c_u in content_updates }
        
        self.assertEqual( result_tags, filtered_tags )
        
    
class TestServiceTagImportOptions( unittest.TestCase ):
    
    def test_basics( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        
        #
        
        default_service_tag_import_options = TagImportOptions.ServiceTagImportOptions()
        
        self.assertEqual( default_service_tag_import_options._get_tags, False )
        self.assertEqual( default_service_tag_import_options._additional_tags, [] )
        self.assertEqual( default_service_tag_import_options._to_new_files, True )
        self.assertEqual( default_service_tag_import_options._to_already_in_inbox, True )
        self.assertEqual( default_service_tag_import_options._to_already_in_archive, True )
        self.assertEqual( default_service_tag_import_options._only_add_existing_tags, False )
        
        self.assertEqual( default_service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), set() )
        
    
    def test_get_tags_filtering( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), some_tags )
        
        #
        
        only_namespaced = HydrusTags.TagFilter()
        
        only_namespaced.SetRule( '', HC.FILTER_BLACKLIST )
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_filter = only_namespaced )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'character:samus aran', 'series:metroid' } )
        
        #
        
        only_samus = HydrusTags.TagFilter()
        
        only_samus.SetRule( '', HC.FILTER_BLACKLIST )
        only_samus.SetRule( ':', HC.FILTER_BLACKLIST )
        only_samus.SetRule( 'character:samus aran', HC.FILTER_WHITELIST )
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_filter = only_samus )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'character:samus aran' } )
        
    
    def test_additional( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, additional_tags = [ 'wew' ] )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), some_tags.union( [ 'wew' ] ) )
        
    
    def test_overwrite_deleted_get_tags_filtering( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, { 'bodysuit', 'series:metroid' } )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_overwrite_deleted = False )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'character:samus aran' } )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, get_tags_overwrite_deleted = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), some_tags )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, additional_tags_overwrite_deleted = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'character:samus aran' } )
        
    
    def test_overwrite_deleted_additional( self ):
        
        some_tags = set()
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, { 'bodysuit', 'series:metroid' } )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, additional_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }, additional_tags_overwrite_deleted = False )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'character:samus aran' } )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, additional_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }, additional_tags_overwrite_deleted = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'bodysuit', 'character:samus aran', 'series:metroid' } )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, additional_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }, get_tags_overwrite_deleted = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'character:samus aran' } )
        
    
    def test_application( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        inbox_media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        archive_media_result = GetTagsMediaResult( example_hash, False, example_service_key, set() )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, to_new_files = True, to_already_in_inbox = False, to_already_in_archive = False )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, inbox_media_result, some_tags ), some_tags )
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, inbox_media_result, some_tags ), set() )
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, archive_media_result, some_tags ), set() )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, to_new_files = False, to_already_in_inbox = True, to_already_in_archive = False )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, inbox_media_result, some_tags ), set() )
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, inbox_media_result, some_tags ), some_tags )
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, archive_media_result, some_tags ), set() )
        
        #
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, to_new_files = False, to_already_in_inbox = False, to_already_in_archive = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, inbox_media_result, some_tags ), set() )
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, inbox_media_result, some_tags ), set() )
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, archive_media_result, some_tags ), some_tags )
        
    
    def test_existing( self ):
        
        some_tags = { 'bodysuit', 'character:samus aran', 'series:metroid' }
        existing_tags = { 'character:samus aran', 'series:metroid' }
        example_hash = HydrusData.GenerateKey()
        example_service_key = HydrusData.GenerateKey()
        
        media_result = GetTagsMediaResult( example_hash, True, example_service_key, set() )
        
        #
        
        HG.test_controller.SetRead( 'filter_existing_tags', existing_tags )
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, only_add_existing_tags = True )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), existing_tags )
        
        #
        
        some_tags = { 'explicit', 'bodysuit', 'character:samus aran', 'series:metroid' }
        existing_tags = { 'bodysuit' }
        
        only_unnamespaced = HydrusTags.TagFilter()
        
        only_unnamespaced.SetRule( ':', HC.FILTER_BLACKLIST )
        
        HG.test_controller.SetRead( 'filter_existing_tags', existing_tags )
        
        service_tag_import_options = TagImportOptions.ServiceTagImportOptions( get_tags = True, only_add_existing_tags = True, only_add_existing_tags_filter = only_unnamespaced )
        
        self.assertEqual( service_tag_import_options.GetTags( example_service_key, CC.STATUS_SUCCESSFUL_AND_NEW, media_result, some_tags ), { 'bodysuit', 'character:samus aran', 'series:metroid' } )
        
    
