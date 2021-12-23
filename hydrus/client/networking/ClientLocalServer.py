from twisted.web.resource import NoResource

from hydrus.core.networking import HydrusServer

from hydrus.client.networking import ClientLocalServerResources

class HydrusClientService( HydrusServer.HydrusService ):
    
    def __init__( self, service, allow_non_local_connections ):
        
        if allow_non_local_connections:
            
            self._client_requests_domain = HydrusServer.REMOTE_DOMAIN
            
        else:
            
            self._client_requests_domain = HydrusServer.LOCAL_DOMAIN
            
        
        HydrusServer.HydrusService.__init__( self, service )
        
    
class HydrusServiceBooru( HydrusClientService ):
    
    def _InitRoot( self ):
        
        root = HydrusClientService._InitRoot( self )
        
        root.putChild( b'gallery', ClientLocalServerResources.HydrusResourceBooruGallery( self._service, self._client_requests_domain ) )
        root.putChild( b'page', ClientLocalServerResources.HydrusResourceBooruPage( self._service, self._client_requests_domain ) )
        root.putChild( b'file', ClientLocalServerResources.HydrusResourceBooruFile( self._service, self._client_requests_domain ) )
        root.putChild( b'thumbnail', ClientLocalServerResources.HydrusResourceBooruThumbnail( self._service, self._client_requests_domain ) )
        root.putChild( b'style.css', ClientLocalServerResources.local_booru_css )
        
        return root
        
    
class HydrusServiceClientAPI( HydrusClientService ):
    
    def _InitRoot( self ):
        
        root = HydrusClientService._InitRoot( self )
        
        root.putChild( b'api_version', ClientLocalServerResources.HydrusResourceClientAPIVersion( self._service, self._client_requests_domain ) )
        root.putChild( b'request_new_permissions', ClientLocalServerResources.HydrusResourceClientAPIPermissionsRequest( self._service, self._client_requests_domain ) )
        root.putChild( b'session_key', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAccountSessionKey( self._service, self._client_requests_domain ) )
        root.putChild( b'verify_access_key', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAccountVerify( self._service, self._client_requests_domain ) )
        root.putChild( b'get_services', ClientLocalServerResources.HydrusResourceClientAPIRestrictedGetServices( self._service, self._client_requests_domain ) )
        
        add_files = NoResource()
        
        root.putChild( b'add_files', add_files )
        
        add_files.putChild( b'add_file', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddFilesAddFile( self._service, self._client_requests_domain ) )
        add_files.putChild( b'delete_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddFilesDeleteFiles( self._service, self._client_requests_domain ) )
        add_files.putChild( b'undelete_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddFilesUndeleteFiles( self._service, self._client_requests_domain ) )
        add_files.putChild( b'archive_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddFilesArchiveFiles( self._service, self._client_requests_domain ) )
        add_files.putChild( b'unarchive_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddFilesUnarchiveFiles( self._service, self._client_requests_domain ) )
        
        add_tags = NoResource()
        
        root.putChild( b'add_tags', add_tags )
        
        add_tags.putChild( b'add_tags', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddTagsAddTags( self._service, self._client_requests_domain ) )
        add_tags.putChild( b'clean_tags', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddTagsCleanTags( self._service, self._client_requests_domain ) )
        add_tags.putChild( b'get_tag_services', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddTagsGetTagServices( self._service, self._client_requests_domain ) )
        
        add_urls = NoResource()
        
        root.putChild( b'add_urls', add_urls )
        
        add_urls.putChild( b'get_url_info', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddURLsGetURLInfo( self._service, self._client_requests_domain ) )
        add_urls.putChild( b'get_url_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddURLsGetURLFiles( self._service, self._client_requests_domain ) )
        add_urls.putChild( b'add_url', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddURLsImportURL( self._service, self._client_requests_domain ) )
        add_urls.putChild( b'associate_url', ClientLocalServerResources.HydrusResourceClientAPIRestrictedAddURLsAssociateURL( self._service, self._client_requests_domain ) )
        
        get_files = NoResource()
        
        root.putChild( b'get_files', get_files )
        
        get_files.putChild( b'search_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedGetFilesSearchFiles( self._service, self._client_requests_domain ) )
        get_files.putChild( b'file_metadata', ClientLocalServerResources.HydrusResourceClientAPIRestrictedGetFilesFileMetadata( self._service, self._client_requests_domain ) )
        get_files.putChild( b'file', ClientLocalServerResources.HydrusResourceClientAPIRestrictedGetFilesGetFile( self._service, self._client_requests_domain ) )
        get_files.putChild( b'thumbnail', ClientLocalServerResources.HydrusResourceClientAPIRestrictedGetFilesGetThumbnail( self._service, self._client_requests_domain ) )
        
        manage_cookies = NoResource()
        
        root.putChild( b'manage_cookies', manage_cookies )
        
        manage_cookies.putChild( b'get_cookies', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManageCookiesGetCookies( self._service, self._client_requests_domain ) )
        manage_cookies.putChild( b'set_cookies', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManageCookiesSetCookies( self._service, self._client_requests_domain ) )
        
        manage_headers = NoResource()
        
        root.putChild( b'manage_headers', manage_headers )
        
        manage_headers.putChild( b'set_user_agent', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManageCookiesSetUserAgent( self._service, self._client_requests_domain ) )
        
        manage_pages = NoResource()
        
        root.putChild( b'manage_pages', manage_pages )
        
        manage_pages.putChild( b'add_files', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManagePagesAddFiles( self._service, self._client_requests_domain ) )
        manage_pages.putChild( b'focus_page', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManagePagesFocusPage( self._service, self._client_requests_domain ) )
        manage_pages.putChild( b'get_pages', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManagePagesGetPages( self._service, self._client_requests_domain ) )
        manage_pages.putChild( b'get_page_info', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManagePagesGetPageInfo( self._service, self._client_requests_domain ) )
        
        manage_database = NoResource()
        
        root.putChild( b'manage_database', manage_database )
        
        manage_database.putChild( b'mr_bones', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManageDatabaseMrBones( self._service, self._client_requests_domain ) )
        manage_database.putChild( b'lock_on', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManageDatabaseLockOn( self._service, self._client_requests_domain ) )
        manage_database.putChild( b'lock_off', ClientLocalServerResources.HydrusResourceClientAPIRestrictedManageDatabaseLockOff( self._service, self._client_requests_domain ) )
        
        return root
        
    
