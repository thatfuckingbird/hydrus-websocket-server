import time
import unittest

from httmock import all_requests, urlmatch, HTTMock, response
from mock import patch

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core.networking import HydrusNetworking

from hydrus.client import ClientConstants as CC
from hydrus.client import ClientStrings
from hydrus.client import ClientServices
from hydrus.client.networking import ClientNetworking
from hydrus.client.networking import ClientNetworkingBandwidth
from hydrus.client.networking import ClientNetworkingContexts
from hydrus.client.networking import ClientNetworkingDomain
from hydrus.client.networking import ClientNetworkingJobs
from hydrus.client.networking import ClientNetworkingLogin
from hydrus.client.networking import ClientNetworkingSessions
from hydrus.client.networking import ClientNetworkingURLClass

from hydrus.test import TestController

# some gumpf
GOOD_RESPONSE = bytes( range( 256 ) )

# 256KB of gumpf
LONG_GOOD_RESPONSE = GOOD_RESPONSE * 4 * 256

BAD_RESPONSE = b'500, it done broke'

@all_requests
def catch_all( url, request ):
    
    raise Exception( 'An unexpected request for ' + url + ' came through in testing.' )
    

MOCK_DOMAIN = 'wew.lad'
MOCK_SUBDOMAIN = 'top.wew.lad'
MOCK_URL = 'https://wew.lad/folder/request&key1=value1&key2=value2'
MOCK_SUBURL = 'https://top.wew.lad/folder2/request&key1=value1&key2=value2'

MOCK_HYDRUS_SERVICE_KEY = HydrusData.GenerateKey()
MOCK_HYDRUS_ADDRESS = '123.45.67.89'
MOCK_HYDRUS_DOMAIN = '123.45.67.89:45871'
MOCK_HYDRUS_URL = 'https://123.45.67.89:45871/muh_hydrus_command'

@urlmatch( netloc = 'wew.lad' )
def catch_wew_error( url, request ):
    
    return { 'status_code' : 500, 'reason' : 'Internal Server Error', 'content' : BAD_RESPONSE }

@urlmatch( netloc = 'wew.lad' )
def catch_wew_ok( url, request ):
    
    return GOOD_RESPONSE
    
@urlmatch( netloc = MOCK_HYDRUS_ADDRESS )
def catch_hydrus_error( url, request ):
    
    return response( 500, BAD_RESPONSE, { 'Server' : HC.service_string_lookup[ HC.TAG_REPOSITORY ] + '/' + str( HC.NETWORK_VERSION ) }, 'Internal Server Error' )

@urlmatch( netloc = MOCK_HYDRUS_ADDRESS )
def catch_hydrus_ok( url, request ):
    
    return response( 200, GOOD_RESPONSE, { 'Server' : HC.service_string_lookup[ HC.TAG_REPOSITORY ] + '/' + str( HC.NETWORK_VERSION ) }, 'OK' )
    
class TestBandwidthManager( unittest.TestCase ):
    
    def test_can_start( self ):
        
        EMPTY_RULES = HydrusNetworking.BandwidthRules()
        
        PERMISSIVE_DATA_RULES = HydrusNetworking.BandwidthRules()
        
        PERMISSIVE_DATA_RULES.AddRule( HC.BANDWIDTH_TYPE_DATA, None, 1048576 )
        
        PERMISSIVE_REQUEST_RULES = HydrusNetworking.BandwidthRules()
        
        PERMISSIVE_REQUEST_RULES.AddRule( HC.BANDWIDTH_TYPE_REQUESTS, None, 10000 )
        
        RESTRICTIVE_DATA_RULES = HydrusNetworking.BandwidthRules()
        
        RESTRICTIVE_DATA_RULES.AddRule( HC.BANDWIDTH_TYPE_DATA, None, 10 )
        
        RESTRICTIVE_REQUEST_RULES = HydrusNetworking.BandwidthRules()
        
        RESTRICTIVE_REQUEST_RULES.AddRule( HC.BANDWIDTH_TYPE_REQUESTS, None, 1 )
        
        DOMAIN_NETWORK_CONTEXT = ClientNetworkingContexts.NetworkContext( CC.NETWORK_CONTEXT_DOMAIN, MOCK_DOMAIN )
        SUBDOMAIN_NETWORK_CONTEXT = ClientNetworkingContexts.NetworkContext( CC.NETWORK_CONTEXT_DOMAIN, MOCK_SUBDOMAIN )
        
        GLOBAL_NETWORK_CONTEXTS = [ ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT ]
        DOMAIN_NETWORK_CONTEXTS = [ ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, DOMAIN_NETWORK_CONTEXT ]
        SUBDOMAIN_NETWORK_CONTEXTS = [ ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, DOMAIN_NETWORK_CONTEXT, SUBDOMAIN_NETWORK_CONTEXT ]
        
        #
        
        fast_forward = HydrusData.GetNow() + 3600
        
        with patch.object( HydrusData, 'GetNow', return_value = fast_forward ):
            
            bm = ClientNetworkingBandwidth.NetworkBandwidthManager()
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            #
            
            bm.ReportRequestUsed( DOMAIN_NETWORK_CONTEXTS )
            bm.ReportDataUsed( DOMAIN_NETWORK_CONTEXTS, 50 )
            bm.ReportRequestUsed( SUBDOMAIN_NETWORK_CONTEXTS )
            bm.ReportDataUsed( SUBDOMAIN_NETWORK_CONTEXTS, 25 )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            #
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, EMPTY_RULES )
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, EMPTY_RULES )
            bm.SetRules( SUBDOMAIN_NETWORK_CONTEXT, EMPTY_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
            bm.SetRules( SUBDOMAIN_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, PERMISSIVE_REQUEST_RULES )
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, PERMISSIVE_REQUEST_RULES )
            bm.SetRules( SUBDOMAIN_NETWORK_CONTEXT, PERMISSIVE_REQUEST_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            #
            
            bm.SetRules( SUBDOMAIN_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( SUBDOMAIN_NETWORK_CONTEXT, RESTRICTIVE_REQUEST_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( SUBDOMAIN_NETWORK_CONTEXT, PERMISSIVE_REQUEST_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            #
            
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, RESTRICTIVE_REQUEST_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, PERMISSIVE_REQUEST_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            #
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
            
            self.assertFalse( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, RESTRICTIVE_REQUEST_RULES )
            
            self.assertFalse( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, PERMISSIVE_REQUEST_RULES )
            
            self.assertTrue( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertTrue( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
            #
            
            bm.SetRules( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, RESTRICTIVE_REQUEST_RULES )
            bm.SetRules( DOMAIN_NETWORK_CONTEXT, EMPTY_RULES )
            
            self.assertFalse( bm.CanStartRequest( GLOBAL_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( DOMAIN_NETWORK_CONTEXTS ) )
            self.assertFalse( bm.CanStartRequest( SUBDOMAIN_NETWORK_CONTEXTS ) )
            
        
    
    def test_can_continue( self ):
        
        pass
        
    
class TestNetworkingDomain( unittest.TestCase ):
    
    def test_url_classes( self ):
        
        name = 'test'
        url_type = HC.URL_TYPE_POST
        preferred_scheme = 'https'
        netloc = 'testbooru.cx'
        
        alphabetise_get_parameters = True
        match_subdomains = False
        keep_matched_subdomains = False
        can_produce_multiple_files = False
        should_be_associated_with_files = True
        keep_fragment = False
        
        path_components = []
        
        path_components.append( ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'post', example_string = 'post' ), None ) )
        path_components.append( ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'page.php', example_string = 'page.php' ), None ) )
        
        parameters = {}
        
        parameters[ 's' ] = ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'view', example_string = 'view' ), None )
        parameters[ 'id' ] = ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FLEXIBLE, match_value = ClientStrings.NUMERIC, example_string = '123456' ), None )
        
        send_referral_url = ClientNetworkingURLClass.SEND_REFERRAL_URL_ONLY_IF_PROVIDED
        referral_url_converter = None
        gallery_index_type = None
        gallery_index_identifier = None
        gallery_index_delta = 1
        example_url = 'https://testbooru.cx/post/page.php?id=123456&s=view'
        
        #
        
        referral_url = 'https://testbooru.cx/gallery/tags=samus_aran'
        good_url = 'https://testbooru.cx/post/page.php?id=123456&s=view'
        unnormalised_good_url_1 = 'https://testbooru.cx/post/page.php?id=123456&s=view&additional_gumpf=stuff'
        unnormalised_good_url_2 = 'https://testbooru.cx/post/page.php?s=view&id=123456'
        bad_url = 'https://wew.lad/123456'
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.Matches( example_url ), True )
        self.assertEqual( url_class.Matches( bad_url ), False )
        
        self.assertEqual( url_class.Normalise( unnormalised_good_url_1 ), good_url )
        self.assertEqual( url_class.Normalise( unnormalised_good_url_2 ), good_url )
        
        self.assertEqual( url_class.GetReferralURL( good_url, referral_url ), referral_url )
        self.assertEqual( url_class.GetReferralURL( good_url, None ), None )
        
        #
        
        alphabetise_get_parameters = False
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.Normalise( unnormalised_good_url_2 ), unnormalised_good_url_2 )
        
        alphabetise_get_parameters = True
        
        #
        
        send_referral_url = ClientNetworkingURLClass.SEND_REFERRAL_URL_NEVER
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.GetReferralURL( good_url, referral_url ), None )
        self.assertEqual( url_class.GetReferralURL( good_url, None ), None )
        
        #
        
        converted_referral_url = good_url.replace( 'testbooru.cx', 'replace.com' )
        
        conversions = []
        
        conversions.append( ( ClientStrings.STRING_CONVERSION_REGEX_SUB, ( 'testbooru.cx', 'replace.com' ) ) )
        
        referral_url_converter = ClientStrings.StringConverter( conversions, good_url )
        
        send_referral_url = ClientNetworkingURLClass.SEND_REFERRAL_URL_CONVERTER_IF_NONE_PROVIDED
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.GetReferralURL( good_url, referral_url ), referral_url )
        self.assertEqual( url_class.GetReferralURL( good_url, None ), converted_referral_url )
        
        #
        
        send_referral_url = ClientNetworkingURLClass.SEND_REFERRAL_URL_ONLY_CONVERTER
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.GetReferralURL( good_url, referral_url ), converted_referral_url )
        self.assertEqual( url_class.GetReferralURL( good_url, None ), converted_referral_url )
        
        # fragment test
        
        name = 'mega test'
        url_type = HC.URL_TYPE_POST
        preferred_scheme = 'https'
        netloc = 'mega.nz'
        
        alphabetise_get_parameters = True
        match_subdomains = False
        keep_matched_subdomains = False
        can_produce_multiple_files = True
        should_be_associated_with_files = True
        
        path_components = []
        
        path_components.append( ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'file', example_string = 'file' ), None ) )
        path_components.append( ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_ANY ), None ) )
        
        parameters = {}
        
        send_referral_url = ClientNetworkingURLClass.SEND_REFERRAL_URL_ONLY_IF_PROVIDED
        referral_url_converter = None
        gallery_index_type = None
        gallery_index_identifier = None
        gallery_index_delta = 1
        example_url = 'https://mega.nz/file/KxJHVKhT#0JPvygZDQcjBHrTWWECaDyNfXAFDyNZyE3Uonif5j-w'
        
        keep_fragment = False
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.Normalise( example_url ), 'https://mega.nz/file/KxJHVKhT' )
        
        keep_fragment = True
        
        url_class = ClientNetworkingURLClass.URLClass( name, url_type = url_type, preferred_scheme = preferred_scheme, netloc = netloc, path_components = path_components, parameters = parameters, send_referral_url = send_referral_url, referral_url_converter = referral_url_converter, gallery_index_type = gallery_index_type, gallery_index_identifier = gallery_index_identifier, gallery_index_delta = gallery_index_delta, example_url = example_url )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.Normalise( example_url ), example_url )
        
        # single-value params test
        
        single_value_good_url = 'https://testbooru.cx/post/page.php?id=123456&token&s=view'
        single_value_bad_url = 'https://testbooru.cx/post/page.php?id=123456&bad_token&s=view'
        single_value_missing_url = 'https://testbooru.cx/post/page.php?id=123456&s=view'
        single_value_good_url_multiple = 'https://testbooru.cx/post/page.php?id=123456&token1&token2&s=view&token0'
        
        single_value_good_url_alphabetical_normalised = 'https://testbooru.cx/post/page.php?id=123456&s=view&token'
        single_value_good_url_multiple_alphabetical_normalised = 'https://testbooru.cx/post/page.php?id=123456&s=view&token0&token1&token2'
        
        name = 'single value lad'
        url_type = HC.URL_TYPE_POST
        preferred_scheme = 'https'
        netloc = 'testbooru.cx'
        
        alphabetise_get_parameters = True
        match_subdomains = False
        keep_matched_subdomains = False
        can_produce_multiple_files = False
        should_be_associated_with_files = True
        keep_fragment = False
        
        path_components = []
        
        path_components.append( ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'post', example_string = 'post' ), None ) )
        path_components.append( ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'page.php', example_string = 'page.php' ), None ) )
        
        parameters = {}
        
        parameters[ 's' ] = ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FIXED, match_value = 'view', example_string = 'view' ), None )
        parameters[ 'id' ] = ( ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_FLEXIBLE, match_value = ClientStrings.NUMERIC, example_string = '123456' ), None )
        
        has_single_value_parameters = True
        single_value_parameters_string_match = ClientStrings.StringMatch( match_type = ClientStrings.STRING_MATCH_REGEX, match_value = '^token.*', example_string = 'token1' )
        
        example_url = single_value_good_url
        
        url_class = ClientNetworkingURLClass.URLClass(
            name,
            url_type = url_type,
            preferred_scheme = preferred_scheme,
            netloc = netloc,
            path_components = path_components,
            parameters = parameters,
            has_single_value_parameters = has_single_value_parameters,
            single_value_parameters_string_match = single_value_parameters_string_match,
            send_referral_url = send_referral_url,
            referral_url_converter = referral_url_converter,
            gallery_index_type = gallery_index_type,
            gallery_index_identifier = gallery_index_identifier,
            gallery_index_delta = gallery_index_delta,
            example_url = example_url
        )
        
        url_class.SetURLBooleans( match_subdomains, keep_matched_subdomains, alphabetise_get_parameters, can_produce_multiple_files, should_be_associated_with_files, keep_fragment )
        
        self.assertEqual( url_class.Normalise( single_value_good_url ), single_value_good_url_alphabetical_normalised )
        self.assertEqual( url_class.Normalise( single_value_good_url_multiple ), single_value_good_url_multiple_alphabetical_normalised )
        
        self.assertEqual( url_class.Matches( single_value_good_url ), True )
        self.assertEqual( url_class.Matches( single_value_good_url_alphabetical_normalised ), True )
        self.assertEqual( url_class.Matches( single_value_good_url_multiple ), True )
        self.assertEqual( url_class.Matches( single_value_good_url_multiple_alphabetical_normalised ), True )
        self.assertEqual( url_class.Matches( single_value_bad_url ), False )
        self.assertEqual( url_class.Matches( single_value_missing_url ), False )
        
        url_class.SetAlphabetiseGetParameters( False )
        
        self.assertEqual( url_class.Normalise( single_value_good_url ), single_value_good_url )
        self.assertEqual( url_class.Normalise( single_value_good_url_multiple ), single_value_good_url_multiple )
        
        self.assertEqual( url_class.Matches( single_value_good_url ), True )
        self.assertEqual( url_class.Matches( single_value_good_url_alphabetical_normalised ), True )
        self.assertEqual( url_class.Matches( single_value_good_url_multiple ), True )
        self.assertEqual( url_class.Matches( single_value_good_url_multiple_alphabetical_normalised ), True )
        self.assertEqual( url_class.Matches( single_value_bad_url ), False )
        self.assertEqual( url_class.Matches( single_value_missing_url ), False )
        
    
class TestNetworkingEngine( unittest.TestCase ):
    
    def test_engine_shutdown_app( self ):
        
        mock_controller = TestController.MockController()
        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
        session_manager = ClientNetworkingSessions.NetworkSessionManager()
        domain_manager = ClientNetworkingDomain.NetworkDomainManager()
        login_manager = ClientNetworkingLogin.NetworkLoginManager()
        
        engine = ClientNetworking.NetworkEngine( mock_controller, bandwidth_manager, session_manager, domain_manager, login_manager )
        
        self.assertFalse( engine.IsRunning() )
        self.assertFalse( engine.IsShutdown() )
        
        mock_controller.CallToThread( engine.MainLoop )
        
        time.sleep( 0.1 )
        
        self.assertTrue( engine.IsRunning() )
        self.assertFalse( engine.IsShutdown() )
        
        HG.model_shutdown = True
        
        try:
            
            engine._new_work_to_do.set()
            
            time.sleep( 0.1 )
            
            self.assertFalse( engine.IsRunning() )
            self.assertTrue( engine.IsShutdown() )
            
        finally:
            
            HG.model_shutdown = False
            
        
    
    def test_engine_shutdown_manual( self ):
        
        mock_controller = TestController.MockController()
        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
        session_manager = ClientNetworkingSessions.NetworkSessionManager()
        domain_manager = ClientNetworkingDomain.NetworkDomainManager()
        login_manager = ClientNetworkingLogin.NetworkLoginManager()
        
        engine = ClientNetworking.NetworkEngine( mock_controller, bandwidth_manager, session_manager, domain_manager, login_manager )
        
        self.assertFalse( engine.IsRunning() )
        self.assertFalse( engine.IsShutdown() )
        
        mock_controller.CallToThread( engine.MainLoop )
        
        time.sleep( 0.1 )
        
        self.assertTrue( engine.IsRunning() )
        self.assertFalse( engine.IsShutdown() )
        
        engine.Shutdown()
        
        time.sleep( 0.1 )
        
        self.assertFalse( engine.IsRunning() )
        self.assertTrue( engine.IsShutdown() )
        
    
    def test_engine_simple_job( self ):
        
        mock_controller = TestController.MockController()
        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
        session_manager = ClientNetworkingSessions.NetworkSessionManager()
        domain_manager = ClientNetworkingDomain.NetworkDomainManager()
        login_manager = ClientNetworkingLogin.NetworkLoginManager()
        
        engine = ClientNetworking.NetworkEngine( mock_controller, bandwidth_manager, session_manager, domain_manager, login_manager )
        
        self.assertFalse( engine.IsRunning() )
        self.assertFalse( engine.IsShutdown() )
        
        mock_controller.CallToThread( engine.MainLoop )
        
        #
        
        with HTTMock( catch_all ):
            
            with HTTMock( catch_wew_ok ):
                
                job = ClientNetworkingJobs.NetworkJob( 'GET', MOCK_URL )
                
                engine.AddJob( job )
                
                time.sleep( 0.25 )
                
                self.assertTrue( job.IsDone() )
                self.assertFalse( job.HasError() )
                
                engine._new_work_to_do.set()
                
                time.sleep( 0.25 )
                
                self.assertEqual( len( engine._jobs_awaiting_validity ), 0 )
                self.assertEqual( len( engine._jobs_awaiting_bandwidth ), 0 )
                self.assertEqual( len( engine._jobs_awaiting_login ), 0 )
                self.assertEqual( len( engine._jobs_awaiting_slot ), 0 )
                self.assertEqual( len( engine._jobs_running ), 0 )
                
            
        
        #
        
        engine.Shutdown()
        
    
class TestNetworkingJob( unittest.TestCase ):
    
    def _GetJob( self, for_login = False ):
        
        job = ClientNetworkingJobs.NetworkJob( 'GET', MOCK_URL )
        
        job.SetForLogin( for_login )
        
        mock_controller = TestController.MockController()
        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
        session_manager = ClientNetworkingSessions.NetworkSessionManager()
        domain_manager = ClientNetworkingDomain.NetworkDomainManager()
        login_manager = ClientNetworkingLogin.NetworkLoginManager()
        
        engine = ClientNetworking.NetworkEngine( mock_controller, bandwidth_manager, session_manager, domain_manager, login_manager )
        
        job.engine = engine
        
        return job
        
    
    def test_cancelled_manually( self ):
        
        job = self._GetJob()
        
        self.assertFalse( job.IsCancelled() )
        self.assertFalse( job.IsDone() )
        
        job.Cancel()
        
        self.assertTrue( job.IsCancelled() )
        self.assertTrue( job.IsDone() )
        
    
    def test_cancelled_app_shutdown( self ):
        
        job = self._GetJob()
        
        self.assertFalse( job.IsCancelled() )
        self.assertFalse( job.IsDone() )
        
        HG.started_shutdown = True
        
        try:
            
            self.assertTrue( job.IsCancelled() )
            self.assertTrue( job.IsDone() )
            
        finally:
            
            HG.started_shutdown = False
            
        
    
    def test_sleep( self ):
        
        job = self._GetJob()
        
        self.assertFalse( job.IsAsleep() )
        
        job.Sleep( 3 )
        
        self.assertTrue( job.IsAsleep() )
        
        five_secs_from_now = HydrusData.GetNowFloat() + 5
        
        with patch.object( HydrusData, 'GetNowFloat', return_value = five_secs_from_now ):
            
            self.assertFalse( job.IsAsleep() )
            
        
    
    def test_bandwidth_exceeded( self ):
        
        RESTRICTIVE_DATA_RULES = HydrusNetworking.BandwidthRules()
        
        RESTRICTIVE_DATA_RULES.AddRule( HC.BANDWIDTH_TYPE_DATA, None, 10 )
        
        DOMAIN_NETWORK_CONTEXT = ClientNetworkingContexts.NetworkContext( CC.NETWORK_CONTEXT_DOMAIN, MOCK_DOMAIN )
        
        #
        
        job = self._GetJob()
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.ReportDataUsed( [ DOMAIN_NETWORK_CONTEXT ], 50 )
        
        job.engine.bandwidth_manager.SetRules( DOMAIN_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), False )
        
        #
        
        job = self._GetJob( for_login = True )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.ReportDataUsed( [ DOMAIN_NETWORK_CONTEXT ], 50 )
        
        job.engine.bandwidth_manager.SetRules( DOMAIN_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
    
    def test_bandwidth_ok( self ):
        
        PERMISSIVE_DATA_RULES = HydrusNetworking.BandwidthRules()
        
        PERMISSIVE_DATA_RULES.AddRule( HC.BANDWIDTH_TYPE_DATA, None, 1048576 )
        
        DOMAIN_NETWORK_CONTEXT = ClientNetworkingContexts.NetworkContext( CC.NETWORK_CONTEXT_DOMAIN, MOCK_DOMAIN )
        
        #
        
        job = self._GetJob()
        
        job.engine.bandwidth_manager.ReportDataUsed( [ DOMAIN_NETWORK_CONTEXT ], 50 )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.SetRules( DOMAIN_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        #
        
        job = self._GetJob( for_login = True )
        
        job.engine.bandwidth_manager.ReportDataUsed( [ DOMAIN_NETWORK_CONTEXT ], 50 )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.SetRules( DOMAIN_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
    
    def test_bandwidth_reported( self ):
        
        with HTTMock( catch_all ):
            
            with HTTMock( catch_wew_ok ):
                
                job = self._GetJob()
                
                job.TryToStartBandwidth()
                
                job.Start()
                
                bm = job.engine.bandwidth_manager
                
                tracker = bm.GetTracker( ClientNetworkingContexts.GLOBAL_NETWORK_CONTEXT )
                
                self.assertEqual( tracker.GetUsage( HC.BANDWIDTH_TYPE_REQUESTS, None ), 1 )
                self.assertEqual( tracker.GetUsage( HC.BANDWIDTH_TYPE_DATA, None ), 256 )
                
            
        
    
    def test_done_ok( self ):
        
        with HTTMock( catch_all ):
            
            with HTTMock( catch_wew_ok ):
                
                job = self._GetJob()
                
                job.Start()
                
                self.assertFalse( job.HasError() )
                
                self.assertEqual( job.GetContentBytes(), GOOD_RESPONSE )
                
                self.assertEqual( job.GetStatus(), ( 'done!', 256, 256, None ) )
                
            
        
    
    def test_error( self ):
        
        with HTTMock( catch_all ):
            
            with HTTMock( catch_wew_error ):
                
                job = self._GetJob()
                
                job.Start()
                
                self.assertTrue( job.HasError() )
                
                self.assertEqual( job.GetContentBytes(), BAD_RESPONSE )
                
                self.assertEqual( type( job.GetErrorException() ), HydrusExceptions.ServerException )
                
                self.assertEqual( job.GetErrorText(), BAD_RESPONSE.decode( 'ascii' ) )
                
                self.assertEqual( job.GetStatus(), ( '500 - Internal Server Error', 18, 18, None ) )
                
            
        
    
    def test_generate_login_process( self ):
        
        # test the system works as expected
        
        pass
        
    
    def test_needs_login( self ):
        
        # test for both normal and login
        
        pass
        
    
class TestNetworkingJobHydrus( unittest.TestCase ):
    
    def _GetJob( self, for_login = False ):
        
        job = ClientNetworkingJobs.NetworkJobHydrus( MOCK_HYDRUS_SERVICE_KEY, 'GET', MOCK_HYDRUS_URL )
        
        job.SetForLogin( for_login )
        
        mock_controller = TestController.MockController()
        
        mock_service = ClientServices.GenerateService( MOCK_HYDRUS_SERVICE_KEY, HC.TAG_REPOSITORY, 'test tag repo' )
        
        mock_services_manager = TestController.MockServicesManager( ( mock_service, ) )
        
        mock_controller.services_manager = mock_services_manager
        
        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
        session_manager = ClientNetworkingSessions.NetworkSessionManager()
        domain_manager = ClientNetworkingDomain.NetworkDomainManager()
        login_manager = ClientNetworkingLogin.NetworkLoginManager()
        
        engine = ClientNetworking.NetworkEngine( mock_controller, bandwidth_manager, session_manager, domain_manager, login_manager )
        
        job.engine = engine
        
        return job
        
    
    def test_bandwidth_exceeded( self ):
        
        RESTRICTIVE_DATA_RULES = HydrusNetworking.BandwidthRules()
        
        RESTRICTIVE_DATA_RULES.AddRule( HC.BANDWIDTH_TYPE_DATA, None, 10 )
        
        HYDRUS_NETWORK_CONTEXT = ClientNetworkingContexts.NetworkContext( CC.NETWORK_CONTEXT_HYDRUS, MOCK_HYDRUS_SERVICE_KEY )
        
        #
        
        job = self._GetJob()
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.ReportDataUsed( [ HYDRUS_NETWORK_CONTEXT ], 50 )
        
        job.engine.bandwidth_manager.SetRules( HYDRUS_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), False )
        
        #
        
        job = self._GetJob( for_login = True )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.ReportDataUsed( [ HYDRUS_NETWORK_CONTEXT ], 50 )
        
        job.engine.bandwidth_manager.SetRules( HYDRUS_NETWORK_CONTEXT, RESTRICTIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
    
    def test_bandwidth_ok( self ):
        
        PERMISSIVE_DATA_RULES = HydrusNetworking.BandwidthRules()
        
        PERMISSIVE_DATA_RULES.AddRule( HC.BANDWIDTH_TYPE_DATA, None, 1048576 )
        
        HYDRUS_NETWORK_CONTEXT = ClientNetworkingContexts.NetworkContext( CC.NETWORK_CONTEXT_HYDRUS, MOCK_HYDRUS_SERVICE_KEY )
        
        #
        
        job = self._GetJob()
        
        job.engine.bandwidth_manager.ReportDataUsed( [ HYDRUS_NETWORK_CONTEXT ], 50 )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.SetRules( HYDRUS_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        #
        
        job = self._GetJob( for_login = True )
        
        job.engine.bandwidth_manager.ReportDataUsed( [ HYDRUS_NETWORK_CONTEXT ], 50 )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
        job.engine.bandwidth_manager.SetRules( HYDRUS_NETWORK_CONTEXT, PERMISSIVE_DATA_RULES )
        
        self.assertEqual( job.TryToStartBandwidth(), True )
        
    
    def test_bandwidth_reported( self ):
        
        pass
        
    
    def test_done_ok( self ):
        
        with HTTMock( catch_all ):
            
            with HTTMock( catch_hydrus_ok ):
                
                job = self._GetJob()
                
                job.Start()
                
                self.assertFalse( job.HasError() )
                
                self.assertEqual( job.GetContentBytes(), GOOD_RESPONSE )
                
                self.assertEqual( job.GetStatus(), ( 'done!', 256, 256, None ) )
                
            
        
    
    def test_error( self ):
        
        with HTTMock( catch_all ):
            
            with HTTMock( catch_hydrus_error ):
                
                job = self._GetJob()
                
                job.Start()
                
                self.assertTrue( job.HasError() )
                
                self.assertEqual( job.GetContentBytes(), BAD_RESPONSE )
                
                self.assertEqual( type( job.GetErrorException() ), HydrusExceptions.ServerException )
                
                self.assertEqual( job.GetErrorText(), BAD_RESPONSE.decode( 'ascii' ) )
                
                self.assertEqual( job.GetStatus(), ( '500 - Internal Server Error', 18, 18, None ) )
                
            
        
    
    def test_generate_login_process( self ):
        
        # test the system works as expected
        
        pass
        
    
    def test_needs_login( self ):
        
        # test for both normal and login
        
        pass
        
    
