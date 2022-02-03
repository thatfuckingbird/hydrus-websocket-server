import collections
import hashlib
import itertools    
import os
import random
import re
import sqlite3
import time
import traceback
import typing

from qtpy import QtCore as QC
from qtpy import QtWidgets as QW

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusDB
from hydrus.core import HydrusDBBase
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusPaths
from hydrus.core import HydrusSerialisable
from hydrus.core import HydrusTags
from hydrus.core.networking import HydrusNetwork

from hydrus.client import ClientAPI
from hydrus.client import ClientApplicationCommand as CAC
from hydrus.client import ClientConstants as CC
from hydrus.client import ClientData
from hydrus.client import ClientDefaults
from hydrus.client import ClientFiles
from hydrus.client import ClientLocation
from hydrus.client import ClientOptions
from hydrus.client import ClientSearch
from hydrus.client import ClientServices
from hydrus.client import ClientThreading
from hydrus.client.db import ClientDBDefinitionsCache
from hydrus.client.db import ClientDBFilesDuplicates
from hydrus.client.db import ClientDBFilesMaintenance
from hydrus.client.db import ClientDBFilesMaintenanceQueue
from hydrus.client.db import ClientDBFilesMetadataBasic
from hydrus.client.db import ClientDBFilesStorage
from hydrus.client.db import ClientDBMaintenance
from hydrus.client.db import ClientDBMappingsCacheSpecificDisplay
from hydrus.client.db import ClientDBMappingsCounts
from hydrus.client.db import ClientDBMappingsCountsUpdate
from hydrus.client.db import ClientDBMappingsStorage
from hydrus.client.db import ClientDBMaster
from hydrus.client.db import ClientDBRepositories
from hydrus.client.db import ClientDBSerialisable
from hydrus.client.db import ClientDBServices
from hydrus.client.db import ClientDBSimilarFiles
from hydrus.client.db import ClientDBTagDisplay
from hydrus.client.db import ClientDBTagParents
from hydrus.client.db import ClientDBTagSearch
from hydrus.client.db import ClientDBTagSiblings
from hydrus.client.importing import ClientImportFiles
from hydrus.client.media import ClientMedia
from hydrus.client.media import ClientMediaManagers
from hydrus.client.media import ClientMediaResult
from hydrus.client.media import ClientMediaResultCache
from hydrus.client.metadata import ClientTags
from hydrus.client.metadata import ClientTagsHandling
from hydrus.client.networking import ClientNetworkingBandwidth
from hydrus.client.networking import ClientNetworkingDomain
from hydrus.client.networking import ClientNetworkingFunctions
from hydrus.client.networking import ClientNetworkingLogin
from hydrus.client.networking import ClientNetworkingSessions

from hydrus.client.importing import ClientImportSubscriptionLegacy
from hydrus.client.networking import ClientNetworkingSessionsLegacy
from hydrus.client.networking import ClientNetworkingBandwidthLegacy

#
#                                𝓑𝓵𝓮𝓼𝓼𝓲𝓷𝓰𝓼 𝓸𝓯 𝓽𝓱𝓮 𝓢𝓱𝓻𝓲𝓷𝓮 𝓸𝓷 𝓽𝓱𝓲𝓼 𝓗𝓮𝓵𝓵 𝓒𝓸𝓭𝓮
#                                              ＲＥＳＯＬＶＥ ＩＮＣＩＤＥＮＴ
#
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██ █▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒░▒▓▓▓░  █▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▓▒  ░▓▓▓ ▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓████▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▓▒  ▓▓▓▓ ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▓▓▒▒▒▒▒▓  ▓▓▓▓  ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ ░▓░  ▓▓▓▓▒▒▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▓▓▓█▒ ▓▓▓█  ▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░ ▓░  ▓▒▒▒▒▒▒▒▒▒▒▒▓▓▓▓▒▓▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓  ▓▓▓░   ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  ▒▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒▒▒▓▓▓▓▒▒▒▒▒▒▒▒▒▒▓  ▓▓▓   ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█  ▒█▓░▒▓▒▒▒▒▓▓▓█▓████████████▓▓▓▓▓▒▒▒▓  ▒▓▓▓  ▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ ░█▓ ░▓▓█████████▓███▓█▓███████▓▓▓▓▓ ░▓▓█  █▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓█▓▓▓▓▓▓▓▓▓▓▓▓▓█▒▒█▓▓▓▓▓▓▓▓▓▓  ▓▓ ░██████▓███▓█████▓▓▓▓▓█████▓▓▓▒ ▓▓▓▒ ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒███▓▓▓▓▓▓▓▓▓▓▓████▓█▓▓▓▓▓▓▓▓▓▓█░▓▓███▓▓▓█▓█▓▓▓█▓█▓███▓▓▓▓▓▓██████▓ ▓▓▓   ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒▓▓▓█▒▓▓▒▓▓▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒██████▓▓▓▓▓████▓▓█▓▓██▓▓▓▓▓▓██▓███ ▓█   ██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓ ▒███▒█▒▓█▓▓███▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓█▓▓██▓▓▓▓▓▓▓▓██▓▓▓▓█▓░▒▒▒▓▓█████ ▒█  ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓░▓██▓▒█▓████▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓███▓▓▓█▓▓██▓▓▓▓▓▓▓▓▓█▓▓▓▓█░ ▓▓▓▓█████▓▓▓░   █▓▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒▓██▓▒█▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓▓▓▓▓▓▓▓██▓▓▓▓▓▒▒▒▓▒ ▒▓▓░▓▓▓▓▓█████▓▓▒  ▓▓▓▒▓▓  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓███▓███▓▓▓▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓█▓█▓▓█▓▓▓▓███▓▒▒▒▒░░▓▓▓▓█▓▓▓▓▓███████▓▓░██▓▓▓▓▒ ▒▓ ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒▓█▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▓▓▓█▓▓▓▓▒▒▓██▓▓▒▓▓▓▓████▓▓▓▓▓██▓▓███▒ ▒█▓▒░░ ▓▓ ▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒██▒▓▓█▓█▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓█▓█▓▒▓█▓▓▓▓▓▓▓▓██████▓▓███▓▓▓▓█████▓█▓  ▓  ░▒▓▓▒ ▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓▓█▓▓█▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓█▓▓█▓▓▓▓▓▓██▓██████████████▓▓▓███▓▓▓█░░█░▒▓▓░▒▒ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▒▓██▓█▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒██▓▓█▓▓▓██▓▓▓▓░▓█▓▒▓███████████▓▓▓███▓▓▓█▓▒▒▓▒   ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓█▒▓██▓▓█▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓███ ▓███░▒▒  ▓▓▒     ░░▒░░▓█▓▓██▓▓▓▓█▓▓▒  ▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓██▓▓███▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓██▓███   ███  ▒   ▒▒░░▓▓▒██   ██████▓▓▓█░▒▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓██▓█▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓█▒   ░██▓  ░▒▒▓█████▓    █▓█▓██▓▓▓█▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒██▓▓██▓▒█▒█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▒▓  ░   ▒▒   ▒ ░█▓▒      ▒ ░░▒█▓▓█████▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓███▓███▒█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓██▒  ▒▓▓▒                  ░▓▒▒██▓▓███▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓░▓▓█░▓█▒▓█▓███▓▓▒▓▓▓▓▓▓▓▒▓██▒▓████                  ▒▓░▒█▓▓█▓██▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓██▓░█▓█▓▒▒▒▓▓██▓▓▒▓▓▓▓▓▒▓██▒  ▓░                  ▒▓▒▓█▓███▓▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓▒▓▓█████▓▓▓██▒▓█▓█▓▓▓▓▒▒██▓▓▓▓▓▓▓▓▒▓█▓                      ▒▓▒▓█▓▓█▓█▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▒░▒▓▓███▓▓██▓▓▓▓█▓▓█▓██▓█▓▓▒▓█▓▓▓▓▓▓▓▓▓▓▓▓▒   ░                 ▓▓▒▓█▒██▓▓▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓▓█████▓▒▓▓▓█▓▓▓▓██▒█▓▓███▓▓▓▒██▓▓▓▓▓▓▓▓▓▓▓▓░                   ▓█▒░▒▒▓██▓█▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█████▓▓  ▓▓██▓▓▓██▒▓█▓█▓▒▓▓▓▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓░    ░░          ░▒█▒▒▒░▒▓█▓▓▓▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▓█▓▓▓   ▒██▓▓▓▓█▓▒██▓▓▒▓▓▓▓▒██▓▓▓▓▓▓▓▓▓▓▓▓█▓             ░▓░░ ░███▓██▓▓▓▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒██▓▓▓░▓██▓▓▓▓██░▓█▓▓▓▓▓▓▓▒▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒        ░▓▒  ░ ▓███▓██▓█▓▓▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓█▓█▓▒▓██▓▓▓██▓▒█▓▓▓▓▓▓▓▓▒██▓▒▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▓░   ▓█▓      █▓▓█▓█▓▓█▓▓▓██▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██   ░██▓▓▓▓█▓▒▓█▓▓▒▓▓▓▓▒▓█▓▓▓▓▓▓▓▓▓▒███▓▒▓▓▓▓███▓░       █▓▓█▓█▓▓█▓▓▓██▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓  █▓░  ░█▓▓▓▓██▓▓██▓▓▒▓▓▓▓▒██▓▓▓▓▓▓▓▓▒▓█▓▓▓▒▓▓▓▓▓░        ░█▒▓█▓█▓▓▓█▓▓▓▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█░ ░   ███  ██▓▓▓██▓▒██▓▓▒▓▓▓▓▒▓██▓▓▓▓▓▓▓▒▓█▓▓▓▒▒▓▓█▓          █▓██▒█▓▓▓▓█▓▓▓█▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒    ░  ███  ▓█▓▓▓▓██▒▓█▓▓▓▓▓▓▓▓▒██▓▒▓▓▓▓▓▓▓██▓█▒▓▓█▓░          █▓██▒▓██████▓▓▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█      ▓ ▓█   ░█▓▓▓▓██▓▓██▓▓▒▓▓▓▓▒▓█▓▓▒▓▓▓▓▓░▓███▓▓█░            █▓█▓▓▓▓▓█▓░███▓▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓█  ▓▒ ██▒    ▒████▓███▒▓█▓▓▓▒▓▓▓▒▓██▓▓▓▓▓▓▓▒▒███▓▓▒     ▒      ▓███▓▓▓▓▓ ░░▓▓██▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▓▓█▓██     ▓█▓▓▓▓▓██▓▓██▓▓▒▒▓▒▒▒▓██▓▒      ▓█▓██   ░        ▓▒▓██▓▓▓▒  ░    ██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓██▓█████▓      ▓██▓█████▓▓▓█▓▓▓▓▓▓▓▓█▒██     █░▒▓▓▓█           ▓▒▓██▓▒░  ▒▒      █▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓████▓         ▓█████████▓▓██▓▓▓▓▓█▓▓▓██▒   █▓  ▓▒▓▒          ▓▓▓█▓   ▒▓         ▒█▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒██▓▒▓░        ▒███▓█████▓▓███▓▓▓▓▓█████▓  ▓▓▓░ ▓▒▓▒        ▒▓▓▓▒  ▓▓▓█▒          ▓█▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▓▓▓▒        ███▓▓█████▓▓████▓▓▓███▓░   ▓▓▓█▓ ▓▓▓       ▓█▒░  ▒▒▓▓▓█            ██▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▓▒▓▒▓▓▓▓▓▓▓▓▓▓█▓       ▒███▓█████▓▓▓█▓▓▓███▓     ▓▓▓▓▓  ▒▓▓     ▓▓▒  ▒▓▒█▓▓▒▓▓            ▓█▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▓▒▒█▓▒▓▒▓▓▓▓▓▓▓▓█▒       ███▓▓█▒██▓▓█▓███▓▓▓░    ▓▓▒▓▒▓▓█  ▓▒ ░▓▓░   ▒█▓▓▓▒▒▓▓▓            ▒█▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▓▒▒▓▓▓▓▓▒▓▓▒▒▒▓▓▓▓▓       ▒██▓█▒▒▓██▒████▓▒▒▓    ▓▓▒▓▒▒▒▓▓▓ ▒▒ ▒▓░▓▒█▓▓▒▒▒▒▒▒▒▓▒             █▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▓▓▓▓▓▒▒▓█▓▓▓▓▓▓▓▓▓▓▒▓▒▒▓▒       ▓███▓▓▓██░▓▓██▓▒▓▒   ▓▓▒▒▒▒▒▒▓▓▓█▓░ ▒█▓▓▓▓▓▒▒▒▒▒▒▒▒▓▒  ░░         ▓▓▒▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▓▒▓▒▓▒▒▓█▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓░      ▒█▓▓█▓▒██░░ ▒██▒▓  ░▓▒▒▒▒▒▒▒▒▓▓▓▓▓█▓▓█▓▓▒▒▒▒▒▒▒▒▒▒▒▓░ ░▒▓▓         █▓▓▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓
#▒▓▓▓▒▒▓█▓▓▓█▓▓▓▓▓▓▒▓▓▓▓▓▓▓██████████▓██▒▓█▓▓  ██▓▓  ▓▒▒▒▒▒▒▒▒▒▒▓▓▓░▒▓▒▓▓▒▒▒▒▒▒▒▒▒▒▒▒▓░░▒▒░▒▓        ▒███▒▓▓▓▒▓▓▓▓▓▓▓▒▓▓▓
#▓▓▓▒▒▓█▓▓████▓▓▓▓▓▓▓▓▓▓▓▓█▓▓███████▓▒▓█▓▓██▒▒ ▓██  ▒▓▒▒▒▒▒▒▒▒▒▒▓▓░ ▒▒░▓▓▒▒▒▒▒▒▒▒▒▒▒▒▓▒ ░░░░█▓        ▓█▒▒▓▓▓▓▒▓▓▓▓▓▓▓▓▓▓
#▒▓▒▒▓███████████▓▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▓▓██▒▓█▓▒██▒▓░▒██  ▓▓▒▒▒▒▒▒▒▒▒▒▓▒  ▒▒░▓▓▒▒▒▒▒▒▒▒▒▒▒▒▓▓  ░░▒▓▓░    ▒░▒   ▒▓▒▓▒▓▒▓▒▓▒▓▒▓▒▓
#▓▒▓▒▓▓▓▓███████▓█▓██▓▓█▓▓▓▓▓▓▓▓▓█▓██▓▓██▒▓█▓▒▓▓██░ ▒▓▒▓▒▓▓▓▒▒▓▓▓ ░░▒░ ▒▓▒▒▒▒▒▒▒▒▒▒▒▒▓▓  ░ ▓▓▓▓  ▒ ▒     ▒▓▓▒▓▒▓▒▓▓▓▒▓▒▓▒
#▒▓▒▓▒▒▒▒▒▓▓██████████▓▓▓▓▓▓█▓█▓█▓███▓▓▓█▓▒██▒▓█▒██ ░█▓▓▓▓▓▓▓▓▓▓  ▒▒▒░ ▒▓▒▒▒▒▒▒▒▒▒▒▒▓▓▓ ░░▒▓▒▓█▒ ░       ██▒▓▒▓▒▓▒▓▒▓▒▓▒▓
#▓▒▓▒▓▒▓▒▒▒▒▒▒▒▓▓█████████▓▓██████████▓▓█▓▒▓██▓█▒▓█░ ▓▓▓▓▓▓▓▓▓█▒ ▒▒▓░▒ ░█▓▓▓▒▓▓▓▓▓▓▓▒▓▓▒ ▒▓▓▒▓▒░    ░▒█▒ ▓▒▓▓▓▒▓▒▓▒▓▒▓▒▓▒
#▒▓▒▓▒▓▒▓▒▓▒▒▒▒▒▒▒▒▒▓▓▓▓▓▓▓███████▓▓██▓▓▓█▒▒██▓▓▓▓█▓ ░█▓▓▓▓▓▓▓▓  ▒▒▒ ▒  █▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ ▒▓▒▓▒▓▓▓▓▓░▓█▓▒   ▒▓▒▓▒▓▒▓▒▓▒▓▒▓
#▓▒▓▒▓▒▓▒▓▒▒▒▓▒▓▒▒▒▒▒▒▒▒▒▒▒▒▒▓    ░▓▒██▓▓▓▓▒▓█▓▓█▓▓█ ░▓▓▓▓▓▓▓█░ ░▒▒▒ ▒  █▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▒▒▓▒▒▒▒▓▓▓▒░ ░      ▓▓▒▓▒▓▒▓▒▓▒▒▒
#▒▓▒▓▒▒▒▒▒▒▒▒▒▒▒▓▒▒▒▒▒▒▒▒▒▒▒▒▓▒   ▒░  ██▓██▒▓██▓█▓▒█▒░▓▓▓▓▓▓▓▓  ░▓▓▒ ▒  ▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▓▒          ▓▓▒▒▒▓▒▒▒▓▒▒
#▓▒▓▒▓▒▓▒▒▒▒▒▒▒▓▒▒▒▒▒▓▒▒▒▓▒▒▒▓▓░░░    ▓██▓█▓▓██▓▓█▒█▓▒▓▓▓▓▓▓▓░  ░▓▒░ ▒  ▒▒▒█▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒▒▒▒▒▒▓▓▒         ░▓▓▒▒▒▒▒▒▒▓▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▒ ░     ██▓▓█▒▓█▓▒█▓▓█▓▓▓▓▓▓▓▓░  ░▓▓  ▒░ ▒▒ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▓▒▓▒▓▓▒     ░░░ ░▓▓▒▒▒▓▒▒▒▒
#▓▒▒▒▒▒▒▒▒▒▒▒▒▒▓▒▒▒▓▒▒▒▒▒▒▒▒▒▒▒▓ ░░    ▓██▓█▒▓██▓▓▓▓█▓▓▓▓▓▓▓▓██▒░▒▒  ▓▒ ░▓ ░█▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▓▒▒▒▓▒▓▓     ░░░░ ▒▓▓▒▒▒▒▒▓▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓ ░░   ░██▓▓▓▒██▓▓█▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▒  ▒▒  ▓░ ▓▓▓▓▓▓▓▓▓▓▓█▒▒▒▒▒▒▒▒▒▒▒▒▓▓▒       ░ ▒▓▓▒▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▒▒▒▒▓▓ ░░   ██▓▓█▒▓██▓██▓▓▓▓▓▓▓▓▓▓▓▓▓█▒  ▒▓  ░▒  ▓▓▓▓▓▓▓▓▓█▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓ ░ ░░░     ▒▓▒▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓░    ▓██▓▓▓▒██▓▓▓▓▓▓▓▓▒▒▒▓▓▓▓▓▓░  ░▓▒▓▓▓░▒▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▓▒     ░░ ░░  ▒▓▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▒  ▓█▓█▓▓█▒███▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▓▓▓▒▒▓█▓█▓▓█▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓      ░░░ ░░  ▓▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▓▓▓▒▓█▓█▓▓██▓▒▓▓▓▓▓▓▓▓▓▓▓▒▒▒▓▓▓▓▓▓▒▒▒▒▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▓▓     ░  ░░░░░  ▓▓▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓██▒▒▓▓█▓▓▒▓██▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▓▒  ▒░   ░ ░░░░░  ▓▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒██▓▒▓▓▓▒▓▓▓█▓▒▒▓▓▓▒▓▒▒▒▓▒▒▒▒▓▓▒▒▒▓▓▓▓▓▓▓▒▒▒▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▓░  ██▓   ░  ░░░░ ▒▓▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░▓██▒▓▓▓█▓▓██▓▒▓▓▓▓▓▒▒▒▒▒▒▒▒▒▓▒▓▓▓▓▓▒▓▒▓▓▒▓▓▓▓▓▒▓▓▒▒▒▒▒▒▒▒▒▓░▓█▒▒░▒    ░ ░░░░ ▒▓▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒██▓▓▓▓▓███▓▒▓▒▓▒▓▒▓▒▒▒▒▒▒▒▒▓▓▒▓▒▓▓▓▓▓▒▒▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▓▓██▒   ▒░      ░░░ ▓▒▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒███▓▒▓██▓█▓▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▒▓▓▓▓▓▓▓▓▒▒▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒▒█░    ▒       ░░ ░▓▒▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓██▓▓▓▒▒▓▓▓▓▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒▒▓▓        ▒░▓░  ░░ ▒▓▒▒▒▒▒▒
#▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░░░▒▒░░▓▓▒▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓▓▓▓▓  ░▒▒▒▒       ▓████▒     ▒▒▒▒▒▒▒▒

def BlockingSafeShowMessage( message ):
    
    HydrusData.DebugPrint( message )
    
    HG.client_controller.CallBlockingToQt( HG.client_controller.app, QW.QMessageBox.warning, None, 'Warning', message )
    
def report_content_speed_to_job_key( job_key, rows_done, total_rows, precise_timestamp, num_rows, row_name ):
    
    it_took = HydrusData.GetNowPrecise() - precise_timestamp
    
    rows_s = HydrusData.ToHumanInt( int( num_rows / it_took ) )
    
    popup_message = 'content row ' + HydrusData.ConvertValueRangeToPrettyString( rows_done, total_rows ) + ': processing ' + row_name + ' at ' + rows_s + ' rows/s'
    
    HG.client_controller.frame_splash_status.SetText( popup_message, print_to_log = False )
    job_key.SetVariable( 'popup_text_2', popup_message )
    
def report_speed_to_job_key( job_key, precise_timestamp, num_rows, row_name ):
    
    it_took = HydrusData.GetNowPrecise() - precise_timestamp
    
    rows_s = HydrusData.ToHumanInt( int( num_rows / it_took ) )
    
    popup_message = 'processing ' + row_name + ' at ' + rows_s + ' rows/s'
    
    HG.client_controller.frame_splash_status.SetText( popup_message, print_to_log = False )
    job_key.SetVariable( 'popup_text_2', popup_message )
    
def report_speed_to_log( precise_timestamp, num_rows, row_name ):
    
    if num_rows == 0:
        
        return
        
    
    it_took = HydrusData.GetNowPrecise() - precise_timestamp
    
    rows_s = HydrusData.ToHumanInt( int( num_rows / it_took ) )
    
    summary = 'processed ' + HydrusData.ToHumanInt( num_rows ) + ' ' + row_name + ' at ' + rows_s + ' rows/s'
    
    HydrusData.Print( summary )
    
class FilteredHashesGenerator( object ):
    
    def __init__( self, file_service_ids_to_valid_hash_ids ):
        
        self._file_service_ids_to_valid_hash_ids = file_service_ids_to_valid_hash_ids
        
    
    def GetHashes( self, file_service_id, hash_ids ):
        
        return self._file_service_ids_to_valid_hash_ids[ file_service_id ].intersection( hash_ids )
        
    
    def IterateHashes( self, hash_ids ):
        
        for ( file_service_id, valid_hash_ids ) in self._file_service_ids_to_valid_hash_ids.items():
            
            if len( valid_hash_ids ) == 0:
                
                continue
                
            
            filtered_hash_ids = valid_hash_ids.intersection( hash_ids )
            
            if len( filtered_hash_ids ) == 0:
                
                continue
                
            
            yield ( file_service_id, filtered_hash_ids )
            
        
    
class FilteredMappingsGenerator( object ):
    
    def __init__( self, file_service_ids_to_valid_hash_ids, mappings_ids ):
        
        self._file_service_ids_to_valid_hash_ids = file_service_ids_to_valid_hash_ids
        self._mappings_ids = mappings_ids
        
    
    def IterateMappings( self, file_service_id ):
        
        valid_hash_ids = self._file_service_ids_to_valid_hash_ids[ file_service_id ]
        
        if len( valid_hash_ids ) > 0:
            
            for ( tag_id, hash_ids ) in self._mappings_ids:
                
                hash_ids = valid_hash_ids.intersection( hash_ids )
                
                if len( hash_ids ) == 0:
                    
                    continue
                    
                
                yield ( tag_id, hash_ids )
                
            
        
    
class JobDatabaseClient( HydrusData.JobDatabase ):
    
    def _DoDelayedResultRelief( self ):
        
        if HG.db_ui_hang_relief_mode:
            
            if QC.QThread.currentThread() == HG.client_controller.main_qt_thread:
                
                HydrusData.Print( 'ui-hang event processing: begin' )
                QW.QApplication.instance().processEvents()
                HydrusData.Print( 'ui-hang event processing: end' )
                
            
        
    
class DB( HydrusDB.HydrusDB ):
    
    READ_WRITE_ACTIONS = [ 'service_info', 'system_predicates', 'missing_thumbnail_hashes' ]
    
    def __init__( self, controller, db_dir, db_name ):
        
        self._initial_messages = []
        
        self._have_printed_a_cannot_vacuum_message = False
        
        self._weakref_media_result_cache = ClientMediaResultCache.MediaResultCache()
        
        self._after_job_content_update_jobs = []
        self._regen_tags_managers_hash_ids = set()
        self._regen_tags_managers_tag_ids = set()
        
        HydrusDB.HydrusDB.__init__( self, controller, db_dir, db_name )
        
    
    def _AddFiles( self, service_id, rows ):
        
        hash_ids = { row[0] for row in rows }
        
        existing_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( service_id, hash_ids, HC.CONTENT_STATUS_CURRENT )
        
        new_hash_ids = hash_ids.difference( existing_hash_ids )
        
        if len( new_hash_ids ) > 0:
            
            service = self.modules_services.GetService( service_id )
            
            service_type = service.GetServiceType()
            
            valid_rows = [ ( hash_id, timestamp ) for ( hash_id, timestamp ) in rows if hash_id in new_hash_ids ]
            
            # if we are adding to a local file domain, either an import or an undelete, remove any from the trash and add to combined local file service if needed
            
            if service_type == HC.LOCAL_FILE_DOMAIN:
                
                self._DeleteFiles( self.modules_services.trash_service_id, new_hash_ids )
                
                self._AddFiles( self.modules_services.combined_local_file_service_id, valid_rows )
                
            
            # insert the files
            
            pending_changed = self.modules_files_storage.AddFiles( service_id, valid_rows )
            
            if pending_changed:
                
                self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
                
            
            delta_size = self.modules_files_metadata_basic.GetTotalSize( new_hash_ids )
            num_viewable_files = self.modules_files_metadata_basic.GetNumViewable( new_hash_ids )
            num_files = len( new_hash_ids )
            num_inbox = len( new_hash_ids.intersection( self.modules_files_metadata_basic.inbox_hash_ids ) )
            
            service_info_updates = []
            
            service_info_updates.append( ( delta_size, service_id, HC.SERVICE_INFO_TOTAL_SIZE ) )
            service_info_updates.append( ( num_viewable_files, service_id, HC.SERVICE_INFO_NUM_VIEWABLE_FILES ) )
            service_info_updates.append( ( num_files, service_id, HC.SERVICE_INFO_NUM_FILES ) )
            service_info_updates.append( ( num_inbox, service_id, HC.SERVICE_INFO_NUM_INBOX ) )
            
            # remove any records of previous deletion
            
            if service_id != self.modules_services.trash_service_id:
                
                num_deleted = self.modules_files_storage.ClearDeleteRecord( service_id, new_hash_ids )
                
                service_info_updates.append( ( -num_deleted, service_id, HC.SERVICE_INFO_NUM_DELETED_FILES ) )
                
            
            # if entering the combined local domain, update the hash cache
            
            if service_id == self.modules_services.combined_local_file_service_id:
                
                self.modules_hashes_local_cache.AddHashIdsToCache( new_hash_ids )
                
            
            # if adding an update file, repo manager wants to know
            
            if service_id == self.modules_services.local_update_service_id:
                
                self.modules_repositories.NotifyUpdatesImported( new_hash_ids )
                
            
            # if we track tags for this service, update the a/c cache
            
            if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
                with self._MakeTemporaryIntegerTable( new_hash_ids, 'hash_id' ) as temp_hash_id_table_name:
                    
                    for tag_service_id in tag_service_ids:
                        
                        self._CacheSpecificMappingsAddFiles( service_id, tag_service_id, new_hash_ids, temp_hash_id_table_name )
                        self.modules_mappings_cache_specific_display.AddFiles( service_id, tag_service_id, new_hash_ids, temp_hash_id_table_name )
                        
                    
                
            
            # now update the combined deleted files service
            
            if service_type in HC.FILE_SERVICES_COVERED_BY_COMBINED_DELETED_FILE:
                
                location_context = self.modules_files_storage.GetLocationContextForAllServicesDeletedFiles()
                
                still_deleted_hash_ids = self.modules_files_storage.FilterHashIds( location_context, new_hash_ids )
                
                no_longer_deleted_hash_ids = new_hash_ids.difference( still_deleted_hash_ids )
                
                self._DeleteFiles( self.modules_services.combined_deleted_file_service_id, no_longer_deleted_hash_ids )
                
            
            # push the service updates, done
            
            self._ExecuteMany( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', service_info_updates )
            
        
    
    def _AddService( self, service_key, service_type, name, dictionary ):
        
        name = self.modules_services.GetNonDupeName( name )
        
        service_id = self.modules_services.AddService( service_key, service_type, name, dictionary )
        
        self._AddServiceCreateFiles( service_id, service_type )
        
        if service_type in HC.REPOSITORIES:
            
            self.modules_repositories.GenerateRepositoryTables( service_id )
            
        
        if service_type in HC.REAL_TAG_SERVICES:
            
            self.modules_tag_search.Generate( self.modules_services.combined_file_service_id, service_id )
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            for file_service_id in file_service_ids:
                
                self.modules_tag_search.Generate( file_service_id, service_id )
                
            
            self.modules_tag_parents.Generate( service_id )
            self.modules_tag_siblings.Generate( service_id )
            
        
        self._AddServiceCreateMappings( service_id, service_type )
        
        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
            for tag_service_id in tag_service_ids:
                
                self.modules_tag_search.Generate( service_id, tag_service_id )
                
            
        
    
    def _AddServiceCreateFiles( self, service_id, service_type ):
        
        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
            
            self.modules_files_storage.GenerateFilesTables( service_id )
            
        
        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
            for tag_service_id in tag_service_ids:
                
                self._CacheSpecificMappingsGenerate( service_id, tag_service_id )
                
            
        
    
    def _AddServiceCreateMappings( self, service_id, service_type ):
        
        if service_type in HC.REAL_TAG_SERVICES:
            
            self.modules_mappings_storage.GenerateMappingsTables( service_id )
            
            self._CacheCombinedFilesMappingsGenerate( service_id )
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for file_service_id in file_service_ids:
                
                self._CacheSpecificMappingsGenerate( file_service_id, service_id )
                
            
        
    
    def _ArchiveFiles( self, hash_ids ):
        
        hash_ids_archived = self.modules_files_metadata_basic.ArchiveFiles( hash_ids )
        
        if len( hash_ids_archived ) > 0:
            
            service_ids_to_counts = self.modules_files_storage.GetServiceIdCounts( hash_ids_archived )
            
            update_rows = list( service_ids_to_counts.items() )
            
            self._ExecuteMany( 'UPDATE service_info SET info = info - ? WHERE service_id = ? AND info_type = ?;', [ ( count, service_id, HC.SERVICE_INFO_NUM_INBOX ) for ( service_id, count ) in update_rows ] )
            
        
    
    def _Backup( self, path ):
        
        self._CloseDBConnection()
        
        try:
            
            job_key = ClientThreading.JobKey( cancellable = True )
            
            job_key.SetStatusTitle( 'backing up db' )
            
            self._controller.pub( 'modal_message', job_key )
            
            job_key.SetVariable( 'popup_text_1', 'closing db' )
            
            HydrusPaths.MakeSureDirectoryExists( path )
            
            for filename in self._db_filenames.values():
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                job_key.SetVariable( 'popup_text_1', 'copying ' + filename )
                
                source = os.path.join( self._db_dir, filename )
                dest = os.path.join( path, filename )
                
                HydrusPaths.MirrorFile( source, dest )
                
            
            additional_filenames = self._GetPossibleAdditionalDBFilenames()
            
            for additional_filename in additional_filenames:
                
                source = os.path.join( self._db_dir, additional_filename )
                dest = os.path.join( path, additional_filename )
                
                if os.path.exists( source ):
                    
                    HydrusPaths.MirrorFile( source, dest )
                    
                
            
            def is_cancelled_hook():
                
                return job_key.IsCancelled()
                
            
            def text_update_hook( text ):
                
                job_key.SetVariable( 'popup_text_1', text )
                
            
            client_files_default = os.path.join( self._db_dir, 'client_files' )
            
            if os.path.exists( client_files_default ):
                
                HydrusPaths.MirrorTree( client_files_default, os.path.join( path, 'client_files' ), text_update_hook = text_update_hook, is_cancelled_hook = is_cancelled_hook )
                
            
        finally:
            
            self._InitDBConnection()
            
            job_key.SetVariable( 'popup_text_1', 'backup complete!' )
            
            job_key.Finish()
            
        
    
    def _CacheCombinedFilesDisplayMappingsAddImplications( self, tag_service_id, implication_tag_ids, tag_id, status_hook = None ):
        
        if len( implication_tag_ids ) == 0:
            
            return
            
        
        remaining_implication_tag_ids = set( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, tag_id ) ).difference( implication_tag_ids )
        
        ( current_delta, pending_delta ) = self._GetWithAndWithoutTagsFileCountCombined( tag_service_id, implication_tag_ids, remaining_implication_tag_ids )
        
        if current_delta > 0 or pending_delta > 0:
            
            counts_cache_changes = ( ( tag_id, current_delta, pending_delta ), )
            
            self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheCombinedFilesDisplayMappingsAddMappingsForChained( self, tag_service_id, storage_tag_id, hash_ids ):
        
        ac_current_counts = collections.Counter()
        ac_pending_counts = collections.Counter()
        
        with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
            
            display_tag_ids = self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, storage_tag_id )
            
            display_tag_ids_to_implied_by_tag_ids = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, display_tag_ids, tags_are_ideal = True )
            
            file_service_ids_to_hash_ids = self._GroupHashIdsByTagCachedFileServiceId( hash_ids, temp_hash_ids_table_name )
            
            for ( display_tag_id, implied_by_tag_ids ) in display_tag_ids_to_implied_by_tag_ids.items():
                
                other_implied_by_tag_ids = set( implied_by_tag_ids )
                other_implied_by_tag_ids.discard( storage_tag_id )
                
                # get the count of pending that are tagged by storage_tag_id but not tagged by any of the other implied_by
                
                num_pending_to_be_rescinded = self._GetWithAndWithoutTagsForFilesFileCount( HC.CONTENT_STATUS_PENDING, tag_service_id, ( storage_tag_id, ), other_implied_by_tag_ids, hash_ids, temp_hash_ids_table_name, file_service_ids_to_hash_ids )
                
                # get the count of current that already have any implication
                
                num_non_addable = self._GetWithAndWithoutTagsForFilesFileCount( HC.CONTENT_STATUS_CURRENT, tag_service_id, implied_by_tag_ids, set(), hash_ids, temp_hash_ids_table_name, file_service_ids_to_hash_ids )
                
                num_addable = len( hash_ids ) - num_non_addable
                
                if num_addable > 0:
                    
                    ac_current_counts[ display_tag_id ] += num_addable
                    
                
                if num_pending_to_be_rescinded > 0:
                    
                    ac_pending_counts[ display_tag_id ] += num_pending_to_be_rescinded
                    
                
            
        
        if len( ac_current_counts ) > 0:
            
            counts_cache_changes = [ ( tag_id, current_delta, 0 ) for ( tag_id, current_delta ) in ac_current_counts.items() ]
            
            self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
        if len( ac_pending_counts ) > 0:
            
            counts_cache_changes = [ ( tag_id, 0, pending_delta ) for ( tag_id, pending_delta ) in ac_pending_counts.items() ]
            
            self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheCombinedFilesDisplayMappingsDeleteImplications( self, tag_service_id, implication_tag_ids, tag_id, status_hook = None ):
        
        if len( implication_tag_ids ) == 0:
            
            return
            
        
        remaining_implication_tag_ids = set( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, tag_id ) ).difference( implication_tag_ids )
        
        ( current_delta, pending_delta ) = self._GetWithAndWithoutTagsFileCountCombined( tag_service_id, implication_tag_ids, remaining_implication_tag_ids )
        
        if current_delta > 0 or pending_delta > 0:
            
            counts_cache_changes = ( ( tag_id, current_delta, pending_delta ), )
            
            self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheCombinedFilesDisplayMappingsDeleteMappingsForChained( self, tag_service_id, storage_tag_id, hash_ids ):
        
        ac_counts = collections.Counter()
        
        with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
            
            display_tag_ids = self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, storage_tag_id )
            
            display_tag_ids_to_implied_by_tag_ids = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, display_tag_ids, tags_are_ideal = True )
            
            file_service_ids_to_hash_ids = self._GroupHashIdsByTagCachedFileServiceId( hash_ids, temp_hash_ids_table_name )
            
            for ( display_tag_id, implied_by_tag_ids ) in display_tag_ids_to_implied_by_tag_ids.items():
                
                other_implied_by_tag_ids = set( implied_by_tag_ids )
                other_implied_by_tag_ids.discard( storage_tag_id )
                
                # get the count of current that are tagged by storage_tag_id but not tagged by any of the other implied_by
                
                num_deletable = self._GetWithAndWithoutTagsForFilesFileCount( HC.CONTENT_STATUS_CURRENT, tag_service_id, ( storage_tag_id, ), other_implied_by_tag_ids, hash_ids, temp_hash_ids_table_name, file_service_ids_to_hash_ids )
                
                if num_deletable > 0:
                    
                    ac_counts[ display_tag_id ] += num_deletable
                    
                
            
        
        if len( ac_counts ) > 0:
            
            counts_cache_changes = [ ( tag_id, current_delta, 0 ) for ( tag_id, current_delta ) in ac_counts.items() ]
            
            self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheCombinedFilesDisplayMappingsClear( self, tag_service_id, keep_pending = False ):
        
        self.modules_mappings_counts.ClearCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, keep_pending = keep_pending )
        
    
    def _CacheCombinedFilesDisplayMappingsDrop( self, tag_service_id ):
        
        self.modules_mappings_counts.DropTables( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id )
        
    
    def _CacheCombinedFilesDisplayMappingsGenerate( self, tag_service_id, status_hook = None ):
        
        if status_hook is not None:
            
            status_hook( 'copying storage counts' )
            
        
        self.modules_mappings_counts.CreateTables( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, populate_from_storage = True )
        
    
    def _CacheCombinedFilesDisplayMappingsPendMappingsForChained( self, tag_service_id, storage_tag_id, hash_ids ):
        
        ac_counts = collections.Counter()
        
        with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
            
            display_tag_ids = self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, storage_tag_id )
            
            display_tag_ids_to_implied_by_tag_ids = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, display_tag_ids, tags_are_ideal = True )
            
            file_service_ids_to_hash_ids = self._GroupHashIdsByTagCachedFileServiceId( hash_ids, temp_hash_ids_table_name )
            
            for ( display_tag_id, implied_by_tag_ids ) in display_tag_ids_to_implied_by_tag_ids.items():
                
                # get the count of current that are tagged by any of the implications
                
                num_non_pendable = self._GetWithAndWithoutTagsForFilesFileCount( HC.CONTENT_STATUS_PENDING, tag_service_id, implied_by_tag_ids, set(), hash_ids, temp_hash_ids_table_name, file_service_ids_to_hash_ids )
                
                num_pendable = len( hash_ids ) - num_non_pendable
                
                if num_pendable > 0:
                    
                    ac_counts[ display_tag_id ] += num_pendable
                    
                
            
        
        if len( ac_counts ) > 0:
            
            counts_cache_changes = [ ( tag_id, 0, pending_delta ) for ( tag_id, pending_delta ) in ac_counts.items() ]
            
            self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheCombinedFilesDisplayMappingsRegeneratePending( self, tag_service_id, status_hook = None ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        if status_hook is not None:
            
            message = 'clearing old combined display data'
            
            status_hook( message )
            
        
        all_pending_storage_tag_ids = self._STS( self._Execute( 'SELECT DISTINCT tag_id FROM {};'.format( pending_mappings_table_name ) ) )
        
        storage_tag_ids_to_display_tag_ids = self.modules_tag_display.GetTagsToImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, all_pending_storage_tag_ids )
        
        all_pending_display_tag_ids = set( itertools.chain.from_iterable( storage_tag_ids_to_display_tag_ids.values() ) )
        
        del all_pending_storage_tag_ids
        del storage_tag_ids_to_display_tag_ids
        
        self.modules_mappings_counts.ClearCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, keep_current = True )
        
        all_pending_display_tag_ids_to_implied_by_storage_tag_ids = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, all_pending_display_tag_ids, tags_are_ideal = True )
        
        counts_cache_changes = []
        
        num_to_do = len( all_pending_display_tag_ids_to_implied_by_storage_tag_ids )
        
        for ( i, ( display_tag_id, storage_tag_ids ) ) in enumerate( all_pending_display_tag_ids_to_implied_by_storage_tag_ids.items() ):
            
            if i % 100 == 0 and status_hook is not None:
                
                message = 'regenerating pending tags {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, num_to_do ) )
                
                status_hook( message )
                
            
            # we'll do these counts from raw tables, not 'get withandwithout count' cleverness, since this is a recovery function and other caches may be dodgy atm
            
            if len( storage_tag_ids ) == 1:
                
                ( storage_tag_id, ) = storage_tag_ids
                
                ( pending_delta, ) = self._Execute( 'SELECT COUNT( DISTINCT hash_id ) FROM {} WHERE tag_id = ?;'.format( pending_mappings_table_name ), ( storage_tag_id, ) ).fetchone()
                
            else:
                
                with self._MakeTemporaryIntegerTable( storage_tag_ids, 'tag_id' ) as temp_tag_ids_table_name:
                    
                    # temp tags to mappings merged
                    ( pending_delta, ) = self._Execute( 'SELECT COUNT( DISTINCT hash_id ) FROM {} CROSS JOIN {} USING ( tag_id );'.format( temp_tag_ids_table_name, pending_mappings_table_name ) ).fetchone()
                    
                
            
            counts_cache_changes.append( ( display_tag_id, 0, pending_delta ) )
            
        
        self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
        
    
    def _CacheCombinedFilesDisplayMappingsRescindPendingMappingsForChained( self, tag_service_id, storage_tag_id, hash_ids ):
        
        ac_counts = collections.Counter()
        
        with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
            
            display_tag_ids = self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, storage_tag_id )
            
            display_tag_ids_to_implied_by_tag_ids = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, display_tag_ids, tags_are_ideal = True )
            
            file_service_ids_to_hash_ids = self._GroupHashIdsByTagCachedFileServiceId( hash_ids, temp_hash_ids_table_name )
            
            for ( display_tag_id, implied_by_tag_ids ) in display_tag_ids_to_implied_by_tag_ids.items():
                
                other_implied_by_tag_ids = set( implied_by_tag_ids )
                other_implied_by_tag_ids.discard( storage_tag_id )
                
                # get the count of current that are tagged by storage_tag_id but not tagged by any of the other implications
                
                num_rescindable = self._GetWithAndWithoutTagsForFilesFileCount( HC.CONTENT_STATUS_PENDING, tag_service_id, ( storage_tag_id, ), other_implied_by_tag_ids, hash_ids, temp_hash_ids_table_name, file_service_ids_to_hash_ids )
                
                if num_rescindable > 0:
                    
                    ac_counts[ display_tag_id ] += num_rescindable
                    
                
            
        
        if len( ac_counts ) > 0:
            
            counts_cache_changes = [ ( tag_id, 0, pending_delta ) for ( tag_id, pending_delta ) in ac_counts.items() ]
            
            self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheCombinedFilesMappingsClear( self, tag_service_id, keep_pending = False ):
        
        self.modules_mappings_counts.ClearCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, keep_pending = keep_pending )
        
        self._CacheCombinedFilesDisplayMappingsClear( tag_service_id, keep_pending = keep_pending )
        
    
    def _CacheCombinedFilesMappingsDrop( self, tag_service_id ):
        
        self.modules_mappings_counts.DropTables( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id )
        
        self._CacheCombinedFilesDisplayMappingsDrop( tag_service_id )
        
    
    def _CacheCombinedFilesMappingsGenerate( self, tag_service_id ):
        
        self.modules_mappings_counts.CreateTables( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id )
        
        #
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        current_mappings_exist = self._Execute( 'SELECT 1 FROM ' + current_mappings_table_name + ' LIMIT 1;' ).fetchone() is not None
        pending_mappings_exist = self._Execute( 'SELECT 1 FROM ' + pending_mappings_table_name + ' LIMIT 1;' ).fetchone() is not None
        
        if current_mappings_exist or pending_mappings_exist: # not worth iterating through all known tags for an empty service
            
            for ( group_of_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, 'SELECT tag_id FROM tags;', 10000 ): # must be a cleverer way of doing this
                
                with self._MakeTemporaryIntegerTable( group_of_ids, 'tag_id' ) as temp_table_name:
                    
                    current_counter = collections.Counter()
                    
                    # temp tags to mappings
                    for ( tag_id, count ) in self._Execute( 'SELECT tag_id, COUNT( * ) FROM {} CROSS JOIN {} USING ( tag_id ) GROUP BY ( tag_id );'.format( temp_table_name, current_mappings_table_name ) ):
                        
                        current_counter[ tag_id ] = count
                        
                    
                    pending_counter = collections.Counter()
                    
                    # temp tags to mappings
                    for ( tag_id, count ) in self._Execute( 'SELECT tag_id, COUNT( * ) FROM {} CROSS JOIN {} USING ( tag_id ) GROUP BY ( tag_id );'.format( temp_table_name, pending_mappings_table_name ) ):
                        
                        pending_counter[ tag_id ] = count
                        
                    
                
                all_ids_seen = set( current_counter.keys() )
                all_ids_seen.update( pending_counter.keys() )
                
                counts_cache_changes = [ ( tag_id, current_counter[ tag_id ], pending_counter[ tag_id ] ) for tag_id in all_ids_seen ]
                
                if len( counts_cache_changes ) > 0:
                    
                    self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
                    
                
            
        
        self._CacheCombinedFilesDisplayMappingsGenerate( tag_service_id )
        
    
    def _CacheCombinedFilesMappingsRegeneratePending( self, tag_service_id, status_hook = None ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        if status_hook is not None:
            
            message = 'clearing old combined display data'
            
            status_hook( message )
            
        
        all_pending_storage_tag_ids = self._STS( self._Execute( 'SELECT DISTINCT tag_id FROM {};'.format( pending_mappings_table_name ) ) )
        
        self.modules_mappings_counts.ClearCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, keep_current = True )
        
        counts_cache_changes = []
        
        num_to_do = len( all_pending_storage_tag_ids )
        
        for ( i, storage_tag_id ) in enumerate( all_pending_storage_tag_ids ):
            
            if i % 100 == 0 and status_hook is not None:
                
                message = 'regenerating pending tags {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, num_to_do ) )
                
                status_hook( message )
                
            
            ( pending_delta, ) = self._Execute( 'SELECT COUNT( DISTINCT hash_id ) FROM {} WHERE tag_id = ?;'.format( pending_mappings_table_name ), ( storage_tag_id, ) ).fetchone()
            
            counts_cache_changes.append( ( storage_tag_id, 0, pending_delta ) )
            
        
        self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, counts_cache_changes )
        
        self._CacheCombinedFilesDisplayMappingsRegeneratePending( tag_service_id, status_hook = status_hook )
        
    
    def _CacheSpecificMappingsAddFiles( self, file_service_id, tag_service_id, hash_ids, hash_ids_table_name ):
        
        ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        # deleted don't have a/c counts to update, so we can do it all in one go here
        self._Execute( 'INSERT OR IGNORE INTO {} ( hash_id, tag_id ) SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( cache_deleted_mappings_table_name, hash_ids_table_name, deleted_mappings_table_name ) )
        
        # temp hashes to mappings
        current_mapping_ids_raw = self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_ids_table_name, current_mappings_table_name ) ).fetchall()
        
        current_mapping_ids_dict = HydrusData.BuildKeyToSetDict( current_mapping_ids_raw )
        
        # temp hashes to mappings
        pending_mapping_ids_raw = self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_ids_table_name, pending_mappings_table_name ) ).fetchall()
        
        pending_mapping_ids_dict = HydrusData.BuildKeyToSetDict( pending_mapping_ids_raw )
        
        all_ids_seen = set( current_mapping_ids_dict.keys() )
        all_ids_seen.update( pending_mapping_ids_dict.keys() )
        
        counts_cache_changes = []
        
        for tag_id in all_ids_seen:
            
            current_hash_ids = current_mapping_ids_dict[ tag_id ]
            
            current_delta = len( current_hash_ids )
            
            if current_delta > 0:
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO ' + cache_current_mappings_table_name + ' ( hash_id, tag_id ) VALUES ( ?, ? );', ( ( hash_id, tag_id ) for hash_id in current_hash_ids ) )
                
                current_delta = self._GetRowCount()
                
            
            #
            
            pending_hash_ids = pending_mapping_ids_dict[ tag_id ]
            
            pending_delta = len( pending_hash_ids )
            
            if pending_delta > 0:
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO ' + cache_pending_mappings_table_name + ' ( hash_id, tag_id ) VALUES ( ?, ? );', ( ( hash_id, tag_id ) for hash_id in pending_hash_ids ) )
                
                pending_delta = self._GetRowCount()
                
            
            #
            
            if current_delta > 0 or pending_delta > 0:
                
                counts_cache_changes.append( ( tag_id, current_delta, pending_delta ) )
                
            
        
        if len( counts_cache_changes ) > 0:
            
            self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheSpecificMappingsAddMappings( self, tag_service_id, tag_id, hash_ids, filtered_hashes_generator: FilteredHashesGenerator ):
        
        for ( file_service_id, filtered_hash_ids ) in filtered_hashes_generator.IterateHashes( hash_ids ):
            
            ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
            
            # we have to interleave this into the iterator so that if two siblings with the same ideal are pend->currented at once, we remain logic consistent for soletag lookups!
            self.modules_mappings_cache_specific_display.RescindPendingMappings( file_service_id, tag_service_id, tag_id, filtered_hash_ids )
            
            self._ExecuteMany( 'DELETE FROM ' + cache_pending_mappings_table_name + ' WHERE hash_id = ? AND tag_id = ?;', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            num_pending_rescinded = self._GetRowCount()
            
            #
            
            self._ExecuteMany( 'INSERT OR IGNORE INTO ' + cache_current_mappings_table_name + ' ( hash_id, tag_id ) VALUES ( ?, ? );', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            num_current_inserted = self._GetRowCount()
            
            #
            
            self._ExecuteMany( 'DELETE FROM ' + cache_deleted_mappings_table_name + ' WHERE hash_id = ? AND tag_id = ?;', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            if num_current_inserted > 0:
                
                counts_cache_changes = [ ( tag_id, num_current_inserted, 0 ) ]
                
                self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
                
            
            if num_pending_rescinded > 0:
                
                counts_cache_changes = [ ( tag_id, 0, num_pending_rescinded ) ]
                
                self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
                
            
            self.modules_mappings_cache_specific_display.AddMappings( file_service_id, tag_service_id, tag_id, filtered_hash_ids )
            
        
    
    def _CacheSpecificMappingsClear( self, file_service_id, tag_service_id, keep_pending = False ):
        
        ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
        
        self._Execute( 'DELETE FROM {};'.format( cache_current_mappings_table_name ) )
        self._Execute( 'DELETE FROM {};'.format( cache_deleted_mappings_table_name ) )
        
        if not keep_pending:
            
            self._Execute( 'DELETE FROM {};'.format( cache_pending_mappings_table_name ) )
            
        
        self.modules_mappings_counts.ClearCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, keep_pending = keep_pending )
        
        self.modules_mappings_cache_specific_display.Clear( file_service_id, tag_service_id, keep_pending = keep_pending )
        
    
    def _CacheSpecificMappingsCreateTables( self, file_service_id, tag_service_id ):
        
        ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS ' + cache_current_mappings_table_name + ' ( hash_id INTEGER, tag_id INTEGER, PRIMARY KEY ( hash_id, tag_id ) ) WITHOUT ROWID;' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS ' + cache_deleted_mappings_table_name + ' ( hash_id INTEGER, tag_id INTEGER, PRIMARY KEY ( hash_id, tag_id ) ) WITHOUT ROWID;' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS ' + cache_pending_mappings_table_name + ' ( hash_id INTEGER, tag_id INTEGER, PRIMARY KEY ( hash_id, tag_id ) ) WITHOUT ROWID;' )
        
        self._CreateIndex( cache_current_mappings_table_name, [ 'tag_id', 'hash_id' ], unique = True )
        self._CreateIndex( cache_deleted_mappings_table_name, [ 'tag_id', 'hash_id' ], unique = True )
        self._CreateIndex( cache_pending_mappings_table_name, [ 'tag_id', 'hash_id' ], unique = True )
        
        self.modules_mappings_counts.CreateTables( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id )
        
    
    def _CacheSpecificMappingsDrop( self, file_service_id, tag_service_id ):
        
        ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
        
        self._Execute( 'DROP TABLE IF EXISTS {};'.format( cache_current_mappings_table_name ) )
        self._Execute( 'DROP TABLE IF EXISTS {};'.format( cache_deleted_mappings_table_name ) )
        self._Execute( 'DROP TABLE IF EXISTS {};'.format( cache_pending_mappings_table_name ) )
        
        self.modules_mappings_counts.DropTables( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id )
        
        self.modules_mappings_cache_specific_display.Drop( file_service_id, tag_service_id )
        
    
    def _CacheSpecificMappingsDeleteFiles( self, file_service_id, tag_service_id, hash_ids, hash_id_table_name ):
        
        self.modules_mappings_cache_specific_display.DeleteFiles( file_service_id, tag_service_id, hash_ids, hash_id_table_name )
        
        ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
        
        # temp hashes to mappings
        deleted_mapping_ids_raw = self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_id_table_name, cache_deleted_mappings_table_name ) ).fetchall()
        
        if len( deleted_mapping_ids_raw ) > 0:
            
            self._ExecuteMany( 'DELETE FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( cache_deleted_mappings_table_name ), deleted_mapping_ids_raw )
            
        
        # temp hashes to mappings
        current_mapping_ids_raw = self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_id_table_name, cache_current_mappings_table_name ) ).fetchall()
        
        current_mapping_ids_dict = HydrusData.BuildKeyToSetDict( current_mapping_ids_raw )
        
        # temp hashes to mappings
        pending_mapping_ids_raw = self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_id_table_name, cache_pending_mappings_table_name ) ).fetchall()
        
        pending_mapping_ids_dict = HydrusData.BuildKeyToSetDict( pending_mapping_ids_raw )
        
        all_ids_seen = set( current_mapping_ids_dict.keys() )
        all_ids_seen.update( pending_mapping_ids_dict.keys() )
        
        counts_cache_changes = []
        
        for tag_id in all_ids_seen:
            
            current_hash_ids = current_mapping_ids_dict[ tag_id ]
            
            num_current = len( current_hash_ids )
            
            #
            
            pending_hash_ids = pending_mapping_ids_dict[ tag_id ]
            
            num_pending = len( pending_hash_ids )
            
            counts_cache_changes.append( ( tag_id, num_current, num_pending ) )
            
        
        self._ExecuteMany( 'DELETE FROM ' + cache_current_mappings_table_name + ' WHERE hash_id = ?;', ( ( hash_id, ) for hash_id in hash_ids ) )
        self._ExecuteMany( 'DELETE FROM ' + cache_pending_mappings_table_name + ' WHERE hash_id = ?;', ( ( hash_id, ) for hash_id in hash_ids ) )
        
        if len( counts_cache_changes ) > 0:
            
            self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
            
        
    
    def _CacheSpecificMappingsDeleteMappings( self, tag_service_id, tag_id, hash_ids, filtered_hashes_generator: FilteredHashesGenerator ):
        
        for ( file_service_id, filtered_hash_ids ) in filtered_hashes_generator.IterateHashes( hash_ids ):
            
            ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
            
            self.modules_mappings_cache_specific_display.DeleteMappings( file_service_id, tag_service_id, tag_id, filtered_hash_ids )
            
            self._ExecuteMany( 'DELETE FROM ' + cache_current_mappings_table_name + ' WHERE hash_id = ? AND tag_id = ?;', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            num_deleted = self._GetRowCount()
            
            #
            
            self._ExecuteMany( 'INSERT OR IGNORE INTO ' + cache_deleted_mappings_table_name + ' ( hash_id, tag_id ) VALUES ( ?, ? );', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            if num_deleted > 0:
                
                counts_cache_changes = [ ( tag_id, num_deleted, 0 ) ]
                
                self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
                
            
        
    
    def _CacheSpecificMappingsGenerate( self, file_service_id, tag_service_id ):
        
        self._CacheSpecificMappingsCreateTables( file_service_id, tag_service_id )
        
        #
        
        hash_ids = self.modules_files_storage.GetCurrentHashIdsList( file_service_id )
        
        BLOCK_SIZE = 10000
        
        for ( i, block_of_hash_ids ) in enumerate( HydrusData.SplitListIntoChunks( hash_ids, BLOCK_SIZE ) ):
            
            with self._MakeTemporaryIntegerTable( block_of_hash_ids, 'hash_id' ) as temp_hash_id_table_name:
                
                self._CacheSpecificMappingsAddFiles( file_service_id, tag_service_id, block_of_hash_ids, temp_hash_id_table_name )
                
            
        
        self.modules_db_maintenance.TouchAnalyzeNewTables()
        
        self.modules_mappings_cache_specific_display.Generate( file_service_id, tag_service_id, populate_from_storage = True )
        
    
    def _CacheSpecificMappingsGetFilteredHashesGenerator( self, file_service_ids, tag_service_id, hash_ids ):
        
        file_service_ids_to_valid_hash_ids = collections.defaultdict( set )
        
        with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_table_name:
            
            for file_service_id in file_service_ids:
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( file_service_id, temp_table_name, HC.CONTENT_STATUS_CURRENT )
                
                valid_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {};'.format( table_join ) ) )
                
                file_service_ids_to_valid_hash_ids[ file_service_id ] = valid_hash_ids
                
            
        
        return FilteredHashesGenerator( file_service_ids_to_valid_hash_ids )
        
    
    def _CacheSpecificMappingsGetFilteredMappingsGenerator( self, file_service_ids, tag_service_id, mappings_ids ):
        
        all_hash_ids = set( itertools.chain.from_iterable( ( hash_ids for ( tag_id, hash_ids ) in mappings_ids ) ) )
        
        file_service_ids_to_valid_hash_ids = collections.defaultdict( set )
        
        with self._MakeTemporaryIntegerTable( all_hash_ids, 'hash_id' ) as temp_table_name:
            
            for file_service_id in file_service_ids:
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( file_service_id, temp_table_name, HC.CONTENT_STATUS_CURRENT )
                
                valid_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {};'.format( table_join ) ) )
                
                file_service_ids_to_valid_hash_ids[ file_service_id ] = valid_hash_ids
                
            
        
        return FilteredMappingsGenerator( file_service_ids_to_valid_hash_ids, mappings_ids )
        
    
    def _CacheSpecificMappingsPendMappings( self, tag_service_id, tag_id, hash_ids, filtered_hashes_generator: FilteredHashesGenerator ):
        
        for ( file_service_id, filtered_hash_ids ) in filtered_hashes_generator.IterateHashes( hash_ids ):
            
            ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
            
            self._ExecuteMany( 'INSERT OR IGNORE INTO ' + cache_pending_mappings_table_name + ' ( hash_id, tag_id ) VALUES ( ?, ? );', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            num_added = self._GetRowCount()
            
            if num_added > 0:
                
                counts_cache_changes = [ ( tag_id, 0, num_added ) ]
                
                self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
                
            
            self.modules_mappings_cache_specific_display.PendMappings( file_service_id, tag_service_id, tag_id, filtered_hash_ids )
            
        
    
    def _CacheSpecificMappingsRegeneratePending( self, file_service_id, tag_service_id, status_hook = None ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
        
        if status_hook is not None:
            
            message = 'clearing old specific data'
            
            status_hook( message )
            
        
        all_pending_storage_tag_ids = self._STS( self._Execute( 'SELECT DISTINCT tag_id FROM {};'.format( pending_mappings_table_name ) ) )
        
        self.modules_mappings_counts.ClearCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, keep_current = True )
        
        self._Execute( 'DELETE FROM {};'.format( cache_pending_mappings_table_name ) )
        
        counts_cache_changes = []
        
        num_to_do = len( all_pending_storage_tag_ids )
        
        select_table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( file_service_id, pending_mappings_table_name, HC.CONTENT_STATUS_CURRENT )
        
        for ( i, storage_tag_id ) in enumerate( all_pending_storage_tag_ids ):
            
            if i % 100 == 0 and status_hook is not None:
                
                message = 'regenerating pending tags {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, num_to_do ) )
                
                status_hook( message )
                
            
            self._Execute( 'INSERT OR IGNORE INTO {} ( tag_id, hash_id ) SELECT tag_id, hash_id FROM {} WHERE tag_id = ?;'.format( cache_pending_mappings_table_name, select_table_join ), ( storage_tag_id, ) )
            
            pending_delta = self._GetRowCount()
            
            counts_cache_changes.append( ( storage_tag_id, 0, pending_delta ) )
            
        
        self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
        
        self.modules_mappings_cache_specific_display.RegeneratePending( file_service_id, tag_service_id, status_hook = status_hook )
        
    
    def _CacheSpecificMappingsRescindPendingMappings( self, tag_service_id, tag_id, hash_ids, filtered_hashes_generator: FilteredHashesGenerator ):
        
        for ( file_service_id, filtered_hash_ids ) in filtered_hashes_generator.IterateHashes( hash_ids ):
            
            ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
            
            ac_counts = collections.Counter()
            
            self.modules_mappings_cache_specific_display.RescindPendingMappings( file_service_id, tag_service_id, tag_id, filtered_hash_ids )
            
            self._ExecuteMany( 'DELETE FROM ' + cache_pending_mappings_table_name + ' WHERE hash_id = ? AND tag_id = ?;', ( ( hash_id, tag_id ) for hash_id in filtered_hash_ids ) )
            
            num_deleted = self._GetRowCount()
            
            if num_deleted > 0:
                
                counts_cache_changes = [ ( tag_id, 0, num_deleted ) ]
                
                self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, counts_cache_changes )
                
            
        
    
    def _CacheTagDisplayForceFullSyncTagsOnSpecifics( self, tag_service_id, file_service_ids ):
        
        # this assumes the caches are empty. it is a 'quick' force repopulation for emergency fill-in maintenance
        
        tag_ids_in_dispute = set()
        
        tag_ids_in_dispute.update( self.modules_tag_siblings.GetAllTagIds( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id ) )
        tag_ids_in_dispute.update( self.modules_tag_parents.GetAllTagIds( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id ) )
        
        for tag_id in tag_ids_in_dispute:
            
            storage_implication_tag_ids = { tag_id }
            
            actual_implication_tag_ids = self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, tag_id )
            
            add_implication_tag_ids = actual_implication_tag_ids.difference( storage_implication_tag_ids )
            
            if len( add_implication_tag_ids ) > 0:
                
                for file_service_id in file_service_ids:
                    
                    self.modules_mappings_cache_specific_display.AddImplications( file_service_id, tag_service_id, add_implication_tag_ids, tag_id )
                    
                
            
            delete_implication_tag_ids = storage_implication_tag_ids.difference( actual_implication_tag_ids )
            
            if len( delete_implication_tag_ids ) > 0:
                
                for file_service_id in file_service_ids:
                    
                    self.modules_mappings_cache_specific_display.DeleteImplications( file_service_id, tag_service_id, delete_implication_tag_ids, tag_id )
                    
                
            
        
        for block_of_tag_ids in HydrusData.SplitIteratorIntoChunks( tag_ids_in_dispute, 1024 ):
            
            self._CacheTagsSyncTags( tag_service_id, block_of_tag_ids, just_these_file_service_ids = file_service_ids )
            
        
    
    def _CacheTagDisplayGetApplicationStatusNumbers( self, service_key ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        ( sibling_rows_to_add, sibling_rows_to_remove, parent_rows_to_add, parent_rows_to_remove, num_actual_rows, num_ideal_rows ) = self.modules_tag_display.GetApplicationStatus( service_id )
        
        status = {}
        
        status[ 'num_siblings_to_sync' ] = len( sibling_rows_to_add ) + len( sibling_rows_to_remove )
        status[ 'num_parents_to_sync' ] = len( parent_rows_to_add ) + len( parent_rows_to_remove )
        status[ 'num_actual_rows' ] = num_actual_rows
        status[ 'num_ideal_rows' ] = num_ideal_rows
        
        status[ 'waiting_on_tag_repos' ] = []
        
        for ( applicable_service_ids, content_type ) in [
            ( self.modules_tag_parents.GetApplicableServiceIds( service_id ), HC.CONTENT_TYPE_TAG_PARENTS ),
            ( self.modules_tag_siblings.GetApplicableServiceIds( service_id ), HC.CONTENT_TYPE_TAG_SIBLINGS )
        ]:
            
            for applicable_service_id in applicable_service_ids:
                
                service = self.modules_services.GetService( applicable_service_id )
                
                if service.GetServiceType() == HC.TAG_REPOSITORY:
                    
                    if self.modules_repositories.HasLotsOfOutstandingLocalProcessing( applicable_service_id, ( content_type, ) ):
                        
                        status[ 'waiting_on_tag_repos' ].append( 'waiting on {} for {} processing'.format( service.GetName(), HC.content_type_string_lookup[ content_type ] ) )
                        
                    
                
            
        
        return status
        
    
    def _CacheTagDisplaySync( self, service_key: bytes, work_time = 0.5 ):
        
        # ok, this is the big maintenance lad
        # basically, we fetch what is in actual, what should be in ideal, and migrate
        # the important change here as compared to the old system is that if you have a bunch of parents like 'character name' -> 'female', which might be a 10k-to-1 relationship, adding a new link to the chain does need much work
        # we compare the current structure, the ideal structure, and just make the needed changes
        
        time_started = HydrusData.GetNowFloat()
        
        tag_service_id = self.modules_services.GetServiceId( service_key )
        
        all_tag_ids_altered = set()
        
        ( sibling_rows_to_add, sibling_rows_to_remove, parent_rows_to_add, parent_rows_to_remove, num_actual_rows, num_ideal_rows ) = self.modules_tag_display.GetApplicationStatus( tag_service_id )
        
        while len( sibling_rows_to_add ) + len( sibling_rows_to_remove ) + len( parent_rows_to_add ) + len( parent_rows_to_remove ) > 0 and not HydrusData.TimeHasPassedFloat( time_started + work_time ):
            
            # ok, so it turns out that migrating entire chains at once was sometimes laggy for certain large parent chains like 'azur lane'
            # imagine the instance where we simply want to parent a hundred As to a single B--we obviously don't have to do all that in one go
            # therefore, we are now going to break the migration into smaller pieces
            
            # I spent a large amount of time trying to figure out a way to _completely_ sync subsets of a chain's tags. this was a gigantic logical pain and complete sync couldn't get neat subsets in certain situations
            
            #█▓█▓███▓█▓███████████████████████████████▓▓▓███▓████████████████
            #█▓▓█▓▓▓▓▓███████████████████▓▓▓▓▓▓▓▓▓██████▓▓███▓███████████████
            #█▓███▓████████████████▓▒░              ░▒▓██████████████████████
            #█▓▓▓▓██████████████▒      ░░░░░░░░░░░░     ▒▓███████████████████
            #█▓█▓████████████▓░    ░░░░░░░░░░░░░░░░░ ░░░  ░▓█████████████████
            #██████████████▓    ░░▒▒▒▒▒░░ ░░░    ░░ ░ ░░░░  ░████████████████
            #█████████████▒  ░░░▒▒▒▒░░░░░░░░       ░   ░░░░   ████▓▓█████████
            #▓▓██████████▒ ░░░░▒▓▒░▒▒░░   ░░░       ░ ░ ░░░░░  ███▓▓▓████████
            #███▓███████▒ ▒▒▒░░▒▒▒▒░░░      ░          ░░░ ░░░  ███▓▓▓███████
            #██████████▓ ▒▒░▒░▒░▒▒▒▒▒░▒░ ░░             ░░░░░ ░  ██▓▓▓███████
            #█████▓▓▓█▒ ▒▒░▒░░░░▒▒░░░░░▒░                ░ ░ ▒▒▒  ██▓▓███████
            #▓▓▓▓▓▓▓█░ ▒▓░░▒░░▒▒▒▒▓░░░░░▒░░             ░ ░░▒▒▒▒░ ▒██▓█▓▓▓▓▓▓
            #▓▓▓▓███▓ ▒▒▒░░░▒▒░░▒░▒▒░░   ░░░░░           ░░░▒░ ▒░▒ ███▓▓▓▓▓▓▓
            #███████▓░▒▒▒▒▒▒░░░▒▒▒░░░░      ░           ░░░ ░░░▒▒░ ░██▓████▓▓
            #▓▓█▓███▒▒▒▓▒▒▓░░▒░▒▒▒▒░░░░░ ░         ░   ░ ░░░░░░▒░░░ ██▓█████▓
            #▒▒▓▓▓▓▓▓▒▓▓░░▓▒ ▒▒░▒▒▒▒▒░░                     ░░ ░░░▒░▒▓▓██████
            #▒▒▓▓▓▓▓▓▒▒▒░▒▒▓░░░▒▒▒▒▒▒░                       ░░░░▒▒░▒▓▓▓▓▓▓▓▓
            #▓▒▓▓▓▓▓▓▒▓░ ▒▒▒▓▒▒░░▒▒▒▒▒▒░▒▒▒▒▒▒▒▒▒▒▒░░░░░▒░▒░░░▒░▒▒▒░▓█▓▓▓▓▓▓▓
            #▓▒▒▓▓▓▓▓▓▓▓░ ▒▒▒▓▒▓▒▒▒▒▒▒▒▒▒▓▓▓▓▓▓▓▓▓▒▒▓▓▓▓▓▓▓▓▒▒▒▒▒▒▒▒▓▓▓▓▓▓▓▓▓
            #▓▓▓▓▓▓▓▓▓▓▓▓▒░▒▒▒░▒▒▓▒▒▒░░▒▓▓▓██▓▓▓░░░░░▒▒▒▓▓▒ ░▒▒▒▒▒▒▓▓▓▓▒▒▒▓▓▓
            #█▓█▓▓▓▓▓▓▓▓▓▓▓▒▒▒▒▒▒▓▓▓▒▒▒▓▓▓▓▒▒▒▓█▓   ░▓▓▒▒▓█▓▒░▒▒▒▒▓█▓█▓▓▓▓▓▓▓
            #█████▓▒▓▓▓▓▓▒▓▓▒▒▒▒▒▒▒▒▒▒▓▒░▒▓▒░░ ░▒▒  ░░░  ▓█▓▓▓▒▒▒▒█▓▒▒▒▓▓▓▓▓▒
            #█████▓▓▓█▓▓▓▓▒▓▓▓▒▒▒▒▒▒░▒▒░░░░   ░░░▒░  ▒ ░  ░ ░▒░░▒▓▓▓▒▒▒▒▒▒▒▒░
            #████▓▓▓███▓▓▓▓▓▓▓▒▒▒▒░░  ▒▒░   ░░░░▒▒   ░▒░▒░  ░░ ░▓█▓▓▒▒▒▒░░▒▒▒
            #███▓▓▓█████▓▓▓▒▒▓▒▒▒▒▒░░  ░ ░░▒░ ░▒▒▒    ▒░░▒░░   ▒▓▒▒▒░▒▒▒▒▓▓▓▒
            #████▓███████▓▒▒▒░▒▒▓▓▓▒▒░░   ░   ▒▒▓██▒▒▓▓░  ░░░░▒▒░▒▒▒▒▒▓▒▓▒▓▒▒
            #████████████▒░▒██░▒▓▓▓▓▓▒▒▒░░░░  ▒▓▒▓▓▓▒░▒▒░  ▒▒▒▓▒▒▒▒▓▒▒▓▓▓▒▒▒▒
            #████▓▓▓▓▓▓▒▓▒  ▓▓  ▒▓▓▓▓▓▓▒▒▒░░░░░    ░ ░░░▒░░▒▒▒▒▒▒ ▒▓▒▒▒▒▒▒▒▒▒
            #▓░░░░░░░▒▒▓▓▓  ▒█▒  ▒▒▓▒▒▒▒▒▒░░░░ ░░░   ░ ░ ▒░▒▒▒▒▒░░▒▓▒▒▒▒▒▒▒▓▒
            #▒▒░░░▒▒▒▒▓▒▒▓▒░ ░▒▒▒▒▓▒▒▒▒▒▒▒▒▒▒▒▓▓▓▓▒▒▓▓▓▓▒░▒▒▒▒▒░░▒▓▒▒▒▒▒▒▒▓▒▒
            #▓▒▒▒▓▓▓▓▓▒▒▒▒▒▓▓▒▓██▓▓▓▒▒▒▒▒░░▒▒▒▒░░░▒▒░░▒▒▓▒░░▒▓▓▓▒▓▓▒▒▒▒▒▒▒▒▒▒
            #▓▒▓▓▓▓▒▒▒▒▒▒▒▒▒▒▓▓▒▓▓▓▓▓▒▒▒▒░░░░░░▒▒▒▒▒▒░░ ░▒░░▒▒▒▒▒▒▒▒▒▒▓▒▓▓▓▓▒
            #▓▒▒▒▒▒▓▓▓▒▓▓▓▓▓▓▓▒▒▒▓▓▓▓▓▒▒▒░░░░░░░     ░░░░░▒▒▓▒▒▒▒▒▒▒▒▓▓▓▓▓▓▓▓
            #▓▓▓▓▓▓▓▓▒▒▒▒▒▓▓▓▒▓▒▒▓▓▓▓▓▓▓▒▒▒░░░░░░     ░░▒▒▒▒▓▒▒▒▒▒▒▒▓▒▒▓▓▓▓▓▓
            #▓▓▓▓▓▓▓▒▒▒▒▓▓▓▓▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▒▒░░▒▒░░░▒▒▓▓▓▒▒█▓▒▓▒▒▒▓▓▒▒▓▓▓▓▓▓
            #█▓▓▓▓▒▒▓▓▓▓▓▓▓▓▓▒▓▓▓▓▓▓██▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▓▓▓▓▒▒░█▓▓▓▓▓▒▒▒▒▒▒▓▓▓▓▓
            #▓▓▓▒▒▒▒▒▓▓▓▓▓▒▓▓▓▒▒▒▒▒ ░▓▓▓▓▓▓▓▓▓██▓█▓▓▓▒▓▒░░░ ▓▓▒▓▒▒▒▒▒▒▒▒▒▓▓▓▒
            #
            #                         IN MEMORIAM
            #     tag_ids_to_trunkward_additional_implication_work_weight
            #
            
            # I am now moving to table row addition/subtraction. we'll try to move one row at a time and do the smallest amount of work
            
            # There are potential multi-row optimisations here to reduce total work amount. Stuff like reordering existing chains, reassigning siblings.
            # e.g. if sibling A->B moves to A->C, we now go:
            # rescind A->B sibling: remove A->B, add A->A implications
            # add A->C sibling: remove A->A, add A->C implications
            # However, multi-row tech requires mixing removes and adds, which means we again stray into Hell Logic Zone 3000. We'll put the thought off.
            
            # I can always remove a sibling row from actual and stay valid. this does not invalidate ideals in parents table
            # I can always remove a parent row from actual and stay valid
            
            # I know I can copy a parent to actual if the tags aren't in any pending removes
            # I know I can copy a sibling to actual if the tags aren't in any pending removes (I would if there were pending removes indicating merges or something, but there won't be!)
            
            # we will remove surplus rows from actual and then add needed rows
            
            # There may be multi-row optimisations here to reduce total work amount, I am not sure. Probably for stuff like reordering existing chains. It probably requires mixing removes and adds, which means we stray into hell logic mode, so we'll put the thought off.
            
            # If we need to remove 1,000 mappings and then add 500 to be correct, we'll be doing 1,500 total no matter the order we do them in. This 1,000/500 is not the sum of all the current rows' individual current estimated work.
                # When removing, the sum overestimates, when adding, the sum underestimates. The number of sibling/parent rows to change is obviously also the same.
            
            # When you remove a row, the other row estimates may stay as weighty, or they may get less. (e.g. removing sibling A->B makes the parent B->C easier to remove later)
            # When you add a row, the other row estimates may stay as weighty, or they may get more. (e.g. adding parent A->B makes adding the sibling b->B more difficult later on)
            
            # The main priority of this function is to reduce each piece of work time.
            # When removing, we can break down the large jobs by doing small jobs. So, by doing small jobs first, we reduce max job time.
            # However, if we try that strategy when adding, we actually increase max job time, as those delayed big jobs only have the option of staying the same or getting bigger! We get zoom speed and then clunk mode.
            # Therefore, when adding, to limit max work time for the whole migration, we want to actually choose the largest jobs first! That work has to be done, and it doesn't get easier!
            
            ( cache_ideal_tag_siblings_lookup_table_name, cache_actual_tag_siblings_lookup_table_name ) = ClientDBTagSiblings.GenerateTagSiblingsLookupCacheTableNames( tag_service_id )
            ( cache_ideal_tag_parents_lookup_table_name, cache_actual_tag_parents_lookup_table_name ) = ClientDBTagParents.GenerateTagParentsLookupCacheTableNames( tag_service_id )
            
            def GetWeightedSiblingRow( sibling_rows, index ):
                
                # when you change the sibling A->B in the _lookup table_:
                # you need to add/remove about A number of mappings for B and all it implies. the weight is: A * count( all the B->X implications )
                
                ideal_tag_ids = { i for ( b, i ) in sibling_rows }
                
                ideal_tag_ids_to_implies = self.modules_tag_display.GetTagsToImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, ideal_tag_ids )
                
                bad_tag_ids = { b for ( b, i ) in sibling_rows }
                
                bad_tag_ids_to_count = self.modules_mappings_counts.GetCountsEstimate( ClientTags.TAG_DISPLAY_STORAGE, tag_service_id, self.modules_services.combined_file_service_id, bad_tag_ids, True, True )
                
                weight_and_rows = [ ( bad_tag_ids_to_count[ b ] * len( ideal_tag_ids_to_implies[ i ] ) + 1, ( b, i ) ) for ( b, i ) in sibling_rows ]
                
                weight_and_rows.sort()
                
                return weight_and_rows[ index ]
                
            
            def GetWeightedParentRow( parent_rows, index ):
                
                # when you change the parent A->B in the _lookup table_:
                # you need to add/remove mappings (of B) for all instances of A and all that implies it. the weight is: sum( all the X->A implications )
                
                child_tag_ids = { c for ( c, a ) in parent_rows }
                
                child_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, child_tag_ids )
                
                all_child_tags = set( child_tag_ids )
                all_child_tags.update( itertools.chain.from_iterable( child_tag_ids_to_implied_by.values() ) )
                
                child_tag_ids_to_count = self.modules_mappings_counts.GetCountsEstimate( ClientTags.TAG_DISPLAY_STORAGE, tag_service_id, self.modules_services.combined_file_service_id, all_child_tags, True, True )
                
                weight_and_rows = [ ( sum( ( child_tag_ids_to_count[ implied_by ] for implied_by in child_tag_ids_to_implied_by[ c ] ) ), ( c, p ) ) for ( c, p ) in parent_rows ]
                
                weight_and_rows.sort()
                
                return weight_and_rows[ index ]
                
            
            # first up, the removees. what is in actual but not ideal
            
            some_removee_sibling_rows = HydrusData.SampleSetByGettingFirst( sibling_rows_to_remove, 20 )
            some_removee_parent_rows = HydrusData.SampleSetByGettingFirst( parent_rows_to_remove, 20 )
            
            if len( some_removee_sibling_rows ) + len( some_removee_parent_rows ) > 0:
                
                smallest_sibling_weight = None
                smallest_sibling_row = None
                smallest_parent_weight = None
                smallest_parent_row = None
                
                if len( some_removee_sibling_rows ) > 0:
                    
                    ( smallest_sibling_weight, smallest_sibling_row ) = GetWeightedSiblingRow( some_removee_sibling_rows, 0 )
                    
                
                if len( some_removee_parent_rows ) > 0:
                    
                    ( smallest_parent_weight, smallest_parent_row ) = GetWeightedParentRow( some_removee_parent_rows, 0 )
                    
                
                if smallest_sibling_weight is not None and smallest_parent_weight is not None:
                    
                    if smallest_sibling_weight < smallest_parent_weight:
                        
                        smallest_parent_weight = None
                        smallest_parent_row = None
                        
                    else:
                        
                        smallest_sibling_weight = None
                        smallest_sibling_row = None
                        
                    
                
                if smallest_sibling_row is not None:
                    
                    # the only things changed here are those implied by or that imply one of these values
                    
                    ( a, b ) = smallest_sibling_row
                    
                    possibly_affected_tag_ids = { a, b }
                    
                    # when you delete a sibling, impliesA and impliedbyA should be subsets of impliesB and impliedbyB
                    # but let's do everything anyway, just in case of invalid cache or something
                    
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                    
                    previous_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                    
                    self._Execute( 'DELETE FROM {} WHERE bad_tag_id = ? AND ideal_tag_id = ?;'.format( cache_actual_tag_siblings_lookup_table_name ), smallest_sibling_row )
                    
                    after_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                    
                    self.modules_tag_siblings.NotifySiblingDeleteRowSynced( tag_service_id, smallest_sibling_row )
                    
                
                if smallest_parent_row is not None:
                    
                    # the only things changed here are those implied by or that imply one of these values
                    
                    ( a, b ) = smallest_parent_row
                    
                    possibly_affected_tag_ids = { a, b }
                    
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                    possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                    
                    previous_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                    
                    self._Execute( 'DELETE FROM {} WHERE child_tag_id = ? AND ancestor_tag_id = ?;'.format( cache_actual_tag_parents_lookup_table_name ), smallest_parent_row )
                    
                    after_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                    
                    self.modules_tag_parents.NotifyParentDeleteRowSynced( tag_service_id, smallest_parent_row )
                    
                
            else:
                
                # there is nothing to remove, so we'll now go for what is in ideal but not actual
                
                some_addee_sibling_rows = HydrusData.SampleSetByGettingFirst( sibling_rows_to_add, 20 )
                some_addee_parent_rows = HydrusData.SampleSetByGettingFirst( parent_rows_to_add, 20 )
                
                if len( some_addee_sibling_rows ) + len( some_addee_parent_rows ) > 0:
                    
                    largest_sibling_weight = None
                    largest_sibling_row = None
                    largest_parent_weight = None
                    largest_parent_row = None
                    
                    if len( some_addee_sibling_rows ) > 0:
                        
                        ( largest_sibling_weight, largest_sibling_row ) = GetWeightedSiblingRow( some_addee_sibling_rows, -1 )
                        
                    
                    if len( some_addee_parent_rows ) > 0:
                        
                        ( largest_parent_weight, largest_parent_row ) = GetWeightedParentRow( some_addee_parent_rows, -1 )
                        
                    
                    if largest_sibling_weight is not None and largest_parent_weight is not None:
                        
                        if largest_sibling_weight > largest_parent_weight:
                            
                            largest_parent_weight = None
                            largest_parent_row = None
                            
                        else:
                            
                            largest_sibling_weight = None
                            largest_sibling_row = None
                            
                        
                    
                    if largest_sibling_row is not None:
                        
                        # the only things changed here are those implied by or that imply one of these values
                        
                        ( a, b ) = largest_sibling_row
                        
                        possibly_affected_tag_ids = { a, b }
                        
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                        
                        previous_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                        
                        self._Execute( 'INSERT OR IGNORE INTO {} ( bad_tag_id, ideal_tag_id ) VALUES ( ?, ? );'.format( cache_actual_tag_siblings_lookup_table_name ), largest_sibling_row )
                        
                        after_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                        
                        self.modules_tag_siblings.NotifySiblingAddRowSynced( tag_service_id, largest_sibling_row )
                        
                    
                    if largest_parent_row is not None:
                        
                        # the only things changed here are those implied by or that imply one of these values
                        
                        ( a, b ) = largest_parent_row
                        
                        possibly_affected_tag_ids = { a, b }
                        
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, a ) )
                        possibly_affected_tag_ids.update( self.modules_tag_display.GetImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, b ) )
                        
                        previous_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                        
                        self._Execute( 'INSERT OR IGNORE INTO {} ( child_tag_id, ancestor_tag_id ) VALUES ( ?, ? );'.format( cache_actual_tag_parents_lookup_table_name ), largest_parent_row )
                        
                        after_chain_tag_ids_to_implied_by = self.modules_tag_display.GetTagsToImpliedBy( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, possibly_affected_tag_ids )
                        
                        self.modules_tag_parents.NotifyParentAddRowSynced( tag_service_id, largest_parent_row )
                        
                    
                else:
                    
                    break
                    
                
            
            #
            
            tag_ids_to_delete_implied_by = collections.defaultdict( set )
            tag_ids_to_add_implied_by = collections.defaultdict( set )
            
            for tag_id in possibly_affected_tag_ids:
                
                previous_implied_by = previous_chain_tag_ids_to_implied_by[ tag_id ]
                after_implied_by = after_chain_tag_ids_to_implied_by[ tag_id ]
                
                to_delete = previous_implied_by.difference( after_implied_by )
                to_add = after_implied_by.difference( previous_implied_by )
                
                if len( to_delete ) > 0:
                    
                    tag_ids_to_delete_implied_by[ tag_id ] = to_delete
                    
                    all_tag_ids_altered.add( tag_id )
                    all_tag_ids_altered.update( to_delete )
                    
                
                if len( to_add ) > 0:
                    
                    tag_ids_to_add_implied_by[ tag_id ] = to_add
                    
                    all_tag_ids_altered.add( tag_id )
                    all_tag_ids_altered.update( to_add )
                    
                
            
            # now do the implications
            
            # if I am feeling very clever, I could potentially add tag_ids_to_migrate_implied_by, which would be an UPDATE
            # this would only work for tag_ids that have the same current implied by in actual and ideal (e.g. moving a tag sibling from A->B to B->A)
            # may be better to do this in a merged add/deleteimplication function that would be able to well detect this with 'same current implied' of count > 0 for that domain
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for file_service_id in file_service_ids:
                
                for ( tag_id, implication_tag_ids ) in tag_ids_to_delete_implied_by.items():
                    
                    self.modules_mappings_cache_specific_display.DeleteImplications( file_service_id, tag_service_id, implication_tag_ids, tag_id )
                    
                
                for ( tag_id, implication_tag_ids ) in tag_ids_to_add_implied_by.items():
                    
                    self.modules_mappings_cache_specific_display.AddImplications( file_service_id, tag_service_id, implication_tag_ids, tag_id )
                    
                
            
            for ( tag_id, implication_tag_ids ) in tag_ids_to_delete_implied_by.items():
                
                self._CacheCombinedFilesDisplayMappingsDeleteImplications( tag_service_id, implication_tag_ids, tag_id )
                
            
            for ( tag_id, implication_tag_ids ) in tag_ids_to_add_implied_by.items():
                
                self._CacheCombinedFilesDisplayMappingsAddImplications( tag_service_id, implication_tag_ids, tag_id )
                
            
            ( sibling_rows_to_add, sibling_rows_to_remove, parent_rows_to_add, parent_rows_to_remove, num_actual_rows, num_ideal_rows ) = self.modules_tag_display.GetApplicationStatus( tag_service_id )
            
        
        if len( all_tag_ids_altered ) > 0:
            
            self._regen_tags_managers_tag_ids.update( all_tag_ids_altered )
            
            self._CacheTagsSyncTags( tag_service_id, all_tag_ids_altered )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_sync_status', service_key )
            
        
        still_needs_work = len( sibling_rows_to_add ) + len( sibling_rows_to_remove ) + len( parent_rows_to_add ) + len( parent_rows_to_remove ) > 0
        
        return still_needs_work
        
    
    def _CacheTagsPopulate( self, file_service_id, tag_service_id, status_hook = None ):
        
        siblings_table_name = ClientDBTagSiblings.GenerateTagSiblingsLookupCacheTableName( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id )
        parents_table_name = ClientDBTagParents.GenerateTagParentsLookupCacheTableName( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id )
        
        queries = [
            self.modules_mappings_counts.GetQueryPhraseForCurrentTagIds( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id ),
            'SELECT DISTINCT bad_tag_id FROM {}'.format( siblings_table_name ),
            'SELECT ideal_tag_id FROM {}'.format( siblings_table_name ),
            'SELECT DISTINCT child_tag_id FROM {}'.format( parents_table_name ),
            'SELECT DISTINCT ancestor_tag_id FROM {}'.format( parents_table_name )
        ]
        
        full_query = '{};'.format( ' UNION '.join( queries ) )
        
        BLOCK_SIZE = 10000
        
        for ( group_of_tag_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, full_query, BLOCK_SIZE ):
            
            self.modules_tag_search.AddTags( file_service_id, tag_service_id, group_of_tag_ids )
            
            message = HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do )
            
            self._controller.frame_splash_status.SetSubtext( message )
            
            if status_hook is not None:
                
                status_hook( message )
                
            
        
        self.modules_db_maintenance.TouchAnalyzeNewTables()
        
    
    def _CacheTagsSyncTags( self, tag_service_id, tag_ids, just_these_file_service_ids = None ):
        
        if len( tag_ids ) == 0:
            
            return
            
        
        if just_these_file_service_ids is None:
            
            file_service_ids = list( self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES ) )
            
            file_service_ids.append( self.modules_services.combined_file_service_id )
            
        else:
            
            file_service_ids = just_these_file_service_ids
            
        
        chained_tag_ids = self.modules_tag_display.FilterChained( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, tag_ids )
        unchained_tag_ids = { tag_id for tag_id in tag_ids if tag_id not in chained_tag_ids }
        
        with self._MakeTemporaryIntegerTable( tag_ids, 'tag_id' ) as temp_tag_ids_table_name:
            
            with self._MakeTemporaryIntegerTable( unchained_tag_ids, 'tag_id' ) as temp_unchained_tag_ids_table_name:
                
                for file_service_id in file_service_ids:
                    
                    exist_in_tag_search_tag_ids = self.modules_tag_search.FilterExistingTagIds( file_service_id, tag_service_id, temp_tag_ids_table_name )
                    
                    exist_in_counts_cache_tag_ids = self.modules_mappings_counts.FilterExistingTagIds( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, temp_unchained_tag_ids_table_name  )
                    
                    should_have = chained_tag_ids.union( exist_in_counts_cache_tag_ids )
                    
                    should_not_have = unchained_tag_ids.difference( exist_in_counts_cache_tag_ids )
                    
                    should_add = should_have.difference( exist_in_tag_search_tag_ids )
                    should_delete = exist_in_tag_search_tag_ids.intersection( should_not_have )
                    
                    self.modules_tag_search.AddTags( file_service_id, tag_service_id, should_add )
                    self.modules_tag_search.DeleteTags( file_service_id, tag_service_id, should_delete )
                    
                
            
        
    
    def _CheckDBIntegrity( self ):
        
        prefix_string = 'checking db integrity: '
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( prefix_string + 'preparing' )
            
            self._controller.pub( 'modal_message', job_key )
            
            num_errors = 0
            
            job_key.SetStatusTitle( prefix_string + 'running' )
            job_key.SetVariable( 'popup_text_1', 'errors found so far: ' + HydrusData.ToHumanInt( num_errors ) )
            
            db_names = [ name for ( index, name, path ) in self._Execute( 'PRAGMA database_list;' ) if name not in ( 'mem', 'temp', 'durable_temp' ) ]
            
            for db_name in db_names:
                
                for ( text, ) in self._Execute( 'PRAGMA ' + db_name + '.integrity_check;' ):
                    
                    ( i_paused, should_quit ) = job_key.WaitIfNeeded()
                    
                    if should_quit:
                        
                        job_key.SetStatusTitle( prefix_string + 'cancelled' )
                        job_key.SetVariable( 'popup_text_1', 'errors found: ' + HydrusData.ToHumanInt( num_errors ) )
                        
                        return
                        
                    
                    if text != 'ok':
                        
                        if num_errors == 0:
                            
                            HydrusData.Print( 'During a db integrity check, these errors were discovered:' )
                            
                        
                        HydrusData.Print( text )
                        
                        num_errors += 1
                        
                    
                    job_key.SetVariable( 'popup_text_1', 'errors found so far: ' + HydrusData.ToHumanInt( num_errors ) )
                    
                
            
        finally:
            
            job_key.SetStatusTitle( prefix_string + 'completed' )
            job_key.SetVariable( 'popup_text_1', 'errors found: ' + HydrusData.ToHumanInt( num_errors ) )
            
            HydrusData.Print( job_key.ToString() )
            
            job_key.Finish()
            
        
    
    def _CleanAfterJobWork( self ):
        
        self._after_job_content_update_jobs = []
        self._regen_tags_managers_hash_ids = set()
        self._regen_tags_managers_tag_ids = set()
        
        HydrusDB.HydrusDB._CleanAfterJobWork( self )
        
    
    def _ClearOrphanFileRecords( self ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        job_key.SetStatusTitle( 'clear orphan file records' )
        
        self._controller.pub( 'modal_message', job_key )
        
        try:
            
            job_key.SetVariable( 'popup_text_1', 'looking for orphans' )
            
            local_file_service_ids = self.modules_services.GetServiceIds( ( HC.LOCAL_FILE_DOMAIN, HC.LOCAL_FILE_TRASH_DOMAIN ) )
            
            local_hash_ids = set()
            
            for local_file_service_id in local_file_service_ids:
                
                some_hash_ids = self.modules_files_storage.GetCurrentHashIdsList( local_file_service_id )
                
                local_hash_ids.update( some_hash_ids )
                
            
            combined_local_hash_ids = set( self.modules_files_storage.GetCurrentHashIdsList( self.modules_services.combined_local_file_service_id ) )
            
            in_local_not_in_combined = local_hash_ids.difference( combined_local_hash_ids )
            in_combined_not_in_local = combined_local_hash_ids.difference( local_hash_ids )
            
            if job_key.IsCancelled():
                
                return
                
            
            job_key.SetVariable( 'popup_text_1', 'deleting orphans' )
            
            if len( in_local_not_in_combined ) > 0:
                
                # these files were deleted from the umbrella service without being cleared from a specific file domain
                # they are most likely deleted from disk
                # pushing the 'delete combined' call will flush from the local services as well
                
                self._DeleteFiles( self.modules_services.combined_local_file_service_id, in_local_not_in_combined )
                
                for hash_id in in_local_not_in_combined:
                    
                    self.modules_similar_files.StopSearchingFile( hash_id )
                    
                
                HydrusData.ShowText( 'Found and deleted ' + HydrusData.ToHumanInt( len( in_local_not_in_combined ) ) + ' local domain orphan file records.' )
                
            
            if job_key.IsCancelled():
                
                return
                
            
            if len( in_combined_not_in_local ) > 0:
                
                # these files were deleted from all specific services but not from the combined service
                # I have only ever seen one example of this and am not sure how it happened
                # in any case, the same 'delete combined' call will do the job
                
                self._DeleteFiles( self.modules_services.combined_local_file_service_id, in_combined_not_in_local )
                
                for hash_id in in_combined_not_in_local:
                    
                    self.modules_similar_files.StopSearchingFile( hash_id )
                    
                
                HydrusData.ShowText( 'Found and deleted ' + HydrusData.ToHumanInt( len( in_combined_not_in_local ) ) + ' combined domain orphan file records.' )
                
            
            if len( in_local_not_in_combined ) == 0 and len( in_combined_not_in_local ) == 0:
                
                HydrusData.ShowText( 'No orphan file records found!' )
                
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
        
    
    def _ClearOrphanTables( self ):
        
        all_table_names = set()
        
        db_names = [ name for ( index, name, path ) in self._Execute( 'PRAGMA database_list;' ) if name not in ( 'mem', 'temp', 'durable_temp' ) ]
        
        for db_name in db_names:
            
            table_names = self._STS( self._Execute( 'SELECT name FROM {}.sqlite_master WHERE type = ?;'.format( db_name ), ( 'table', ) ) )
            
            if db_name != 'main':
                
                table_names = { '{}.{}'.format( db_name, table_name ) for table_name in table_names }
                
            
            all_table_names.update( table_names )
            
        
        all_surplus_table_names = set()
        
        for module in self._modules:
            
            surplus_table_names = module.GetSurplusServiceTableNames( all_table_names )
            
            all_surplus_table_names.update( surplus_table_names )
            
        
        if len( surplus_table_names ) == 0:
            
            HydrusData.ShowText( 'No orphan tables!' )
            
        
        for table_name in surplus_table_names:
            
            HydrusData.ShowText( 'Dropping ' + table_name )
            
            self._Execute( 'DROP table ' + table_name + ';' )
            
        
    
    def _CreateDB( self ):
        
        client_files_default = os.path.join( self._db_dir, 'client_files' )
        
        HydrusPaths.MakeSureDirectoryExists( client_files_default )
        
        # main
        
        for module in self._modules:
            
            module.CreateInitialTables()
            module.CreateInitialIndices()
            
        
        # intentionally not IF NOT EXISTS here, to catch double-creation accidents early and on a good table
        self._Execute( 'CREATE TABLE version ( version INTEGER );' )
        
        #
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS client_files_locations ( prefix TEXT, location TEXT );' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS ideal_client_files_locations ( location TEXT, weight INTEGER );' )
        self._Execute( 'CREATE TABLE IF NOT EXISTS ideal_thumbnail_override_location ( location TEXT );' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS file_notes ( hash_id INTEGER, name_id INTEGER, note_id INTEGER, PRIMARY KEY ( hash_id, name_id ) );' )
        self._CreateIndex( 'file_notes', [ 'note_id' ] )
        self._CreateIndex( 'file_notes', [ 'name_id' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS local_ratings ( service_id INTEGER, hash_id INTEGER, rating REAL, PRIMARY KEY ( service_id, hash_id ) );' )
        self._CreateIndex( 'local_ratings', [ 'hash_id' ] )
        self._CreateIndex( 'local_ratings', [ 'rating' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS file_modified_timestamps ( hash_id INTEGER PRIMARY KEY, file_modified_timestamp INTEGER );' )
        self._CreateIndex( 'file_modified_timestamps', [ 'file_modified_timestamp' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS options ( options TEXT_YAML );', )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS recent_tags ( service_id INTEGER, tag_id INTEGER, timestamp INTEGER, PRIMARY KEY ( service_id, tag_id ) );' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS remote_thumbnails ( service_id INTEGER, hash_id INTEGER, PRIMARY KEY( service_id, hash_id ) );' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS service_filenames ( service_id INTEGER, hash_id INTEGER, filename TEXT, PRIMARY KEY ( service_id, hash_id ) );' )
        self._CreateIndex( 'service_filenames', [ 'hash_id' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS service_directories ( service_id INTEGER, directory_id INTEGER, num_files INTEGER, total_size INTEGER, note TEXT, PRIMARY KEY ( service_id, directory_id ) );' )
        self._CreateIndex( 'service_directories', [ 'directory_id' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS service_directory_file_map ( service_id INTEGER, directory_id INTEGER, hash_id INTEGER, PRIMARY KEY ( service_id, directory_id, hash_id ) );' )
        self._CreateIndex( 'service_directory_file_map', [ 'directory_id' ] )
        self._CreateIndex( 'service_directory_file_map', [ 'hash_id' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS service_info ( service_id INTEGER, info_type INTEGER, info INTEGER, PRIMARY KEY ( service_id, info_type ) );' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS statuses ( status_id INTEGER PRIMARY KEY, status TEXT UNIQUE );' )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS url_map ( hash_id INTEGER, url_id INTEGER, PRIMARY KEY ( hash_id, url_id ) );' )
        self._CreateIndex( 'url_map', [ 'url_id' ] )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS file_viewing_stats ( hash_id INTEGER, canvas_type INTEGER, last_viewed_timestamp INTEGER, views INTEGER, viewtime INTEGER, PRIMARY KEY ( hash_id, canvas_type ) );' )
        self._CreateIndex( 'file_viewing_stats', [ 'last_viewed_timestamp' ] )
        self._CreateIndex( 'file_viewing_stats', [ 'views' ] )
        self._CreateIndex( 'file_viewing_stats', [ 'viewtime' ] )
        
        # inserts
        
        location = HydrusPaths.ConvertAbsPathToPortablePath( client_files_default )
        
        for prefix in HydrusData.IterateHexPrefixes():
            
            self._Execute( 'INSERT INTO client_files_locations ( prefix, location ) VALUES ( ?, ? );', ( 'f' + prefix, location ) )
            self._Execute( 'INSERT INTO client_files_locations ( prefix, location ) VALUES ( ?, ? );', ( 't' + prefix, location ) )
            
        
        self._Execute( 'INSERT INTO ideal_client_files_locations ( location, weight ) VALUES ( ?, ? );', ( location, 1 ) )
        
        init_service_info = []
        
        init_service_info.append( ( CC.COMBINED_TAG_SERVICE_KEY, HC.COMBINED_TAG, 'all known tags' ) )
        init_service_info.append( ( CC.COMBINED_FILE_SERVICE_KEY, HC.COMBINED_FILE, 'all known files' ) )
        init_service_info.append( ( CC.COMBINED_DELETED_FILE_SERVICE_KEY, HC.COMBINED_DELETED_FILE, 'all deleted files' ) )
        init_service_info.append( ( CC.COMBINED_LOCAL_FILE_SERVICE_KEY, HC.COMBINED_LOCAL_FILE, 'all local files' ) )
        init_service_info.append( ( CC.LOCAL_FILE_SERVICE_KEY, HC.LOCAL_FILE_DOMAIN, 'my files' ) )
        init_service_info.append( ( CC.TRASH_SERVICE_KEY, HC.LOCAL_FILE_TRASH_DOMAIN, 'trash' ) )
        init_service_info.append( ( CC.LOCAL_UPDATE_SERVICE_KEY, HC.LOCAL_FILE_DOMAIN, 'repository updates' ) )
        init_service_info.append( ( CC.DEFAULT_LOCAL_TAG_SERVICE_KEY, HC.LOCAL_TAG, 'my tags' ) )
        init_service_info.append( ( CC.DEFAULT_LOCAL_DOWNLOADER_TAG_SERVICE_KEY, HC.LOCAL_TAG, 'downloader tags' ) )
        init_service_info.append( ( CC.LOCAL_BOORU_SERVICE_KEY, HC.LOCAL_BOORU, 'local booru' ) )
        init_service_info.append( ( CC.LOCAL_NOTES_SERVICE_KEY, HC.LOCAL_NOTES, 'local notes' ) )
        init_service_info.append( ( CC.DEFAULT_FAVOURITES_RATING_SERVICE_KEY, HC.LOCAL_RATING_LIKE, 'favourites' ) )
        init_service_info.append( ( CC.CLIENT_API_SERVICE_KEY, HC.CLIENT_API_SERVICE, 'client api' ) )
        
        for ( service_key, service_type, name ) in init_service_info:
            
            dictionary = ClientServices.GenerateDefaultServiceDictionary( service_type )
            
            if service_key == CC.DEFAULT_FAVOURITES_RATING_SERVICE_KEY:
                
                from hydrus.client.metadata import ClientRatings
                
                dictionary[ 'shape' ] = ClientRatings.STAR
                
                like_colours = {}
                
                like_colours[ ClientRatings.LIKE ] = ( ( 0, 0, 0 ), ( 240, 240, 65 ) )
                like_colours[ ClientRatings.DISLIKE ] = ( ( 0, 0, 0 ), ( 200, 80, 120 ) )
                like_colours[ ClientRatings.NULL ] = ( ( 0, 0, 0 ), ( 191, 191, 191 ) )
                like_colours[ ClientRatings.MIXED ] = ( ( 0, 0, 0 ), ( 95, 95, 95 ) )
                
                dictionary[ 'colours' ] = list( like_colours.items() )
                
            
            self._AddService( service_key, service_type, name, dictionary )
            
        
        self._ExecuteMany( 'INSERT INTO yaml_dumps VALUES ( ?, ?, ? );', ( ( ClientDBSerialisable.YAML_DUMP_ID_IMAGEBOARD, name, imageboards ) for ( name, imageboards ) in ClientDefaults.GetDefaultImageboards() ) )
        
        new_options = ClientOptions.ClientOptions()
        
        new_options.SetSimpleDownloaderFormulae( ClientDefaults.GetDefaultSimpleDownloaderFormulae() )
        
        names_to_tag_filters = {}
        
        tag_filter = HydrusTags.TagFilter()
        
        tag_filter.SetRule( 'diaper', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'gore', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'guro', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'scat', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'vore', HC.FILTER_BLACKLIST )
        
        names_to_tag_filters[ 'example blacklist' ] = tag_filter
        
        tag_filter = HydrusTags.TagFilter()
        
        tag_filter.SetRule( '', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( ':', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'series:', HC.FILTER_WHITELIST )
        tag_filter.SetRule( 'creator:', HC.FILTER_WHITELIST )
        tag_filter.SetRule( 'studio:', HC.FILTER_WHITELIST )
        tag_filter.SetRule( 'character:', HC.FILTER_WHITELIST )
        
        names_to_tag_filters[ 'basic namespaces only' ] = tag_filter
        
        tag_filter = HydrusTags.TagFilter()
        
        tag_filter.SetRule( ':', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'series:', HC.FILTER_WHITELIST )
        tag_filter.SetRule( 'creator:', HC.FILTER_WHITELIST )
        tag_filter.SetRule( 'studio:', HC.FILTER_WHITELIST )
        tag_filter.SetRule( 'character:', HC.FILTER_WHITELIST )
        
        names_to_tag_filters[ 'basic booru tags only' ] = tag_filter
        
        tag_filter = HydrusTags.TagFilter()
        
        tag_filter.SetRule( 'title:', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'filename:', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'source:', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'booru:', HC.FILTER_BLACKLIST )
        tag_filter.SetRule( 'url:', HC.FILTER_BLACKLIST )
        
        names_to_tag_filters[ 'exclude long/spammy namespaces' ] = tag_filter
        
        new_options.SetFavouriteTagFilters( names_to_tag_filters )
        
        self.modules_serialisable.SetJSONDump( new_options )
        
        list_of_shortcuts = ClientDefaults.GetDefaultShortcuts()
        
        for shortcuts in list_of_shortcuts:
            
            self.modules_serialisable.SetJSONDump( shortcuts )
            
        
        client_api_manager = ClientAPI.APIManager()
        
        self.modules_serialisable.SetJSONDump( client_api_manager )
        
        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
        
        bandwidth_manager.SetDirty()
        
        ClientDefaults.SetDefaultBandwidthManagerRules( bandwidth_manager )
        
        self.modules_serialisable.SetJSONDump( bandwidth_manager )
        
        domain_manager = ClientNetworkingDomain.NetworkDomainManager()
        
        ClientDefaults.SetDefaultDomainManagerData( domain_manager )
        
        self.modules_serialisable.SetJSONDump( domain_manager )
        
        session_manager = ClientNetworkingSessions.NetworkSessionManager()
        
        session_manager.SetDirty()
        
        self.modules_serialisable.SetJSONDump( session_manager )
        
        login_manager = ClientNetworkingLogin.NetworkLoginManager()
        
        ClientDefaults.SetDefaultLoginManagerScripts( login_manager )
        
        self.modules_serialisable.SetJSONDump( login_manager )
        
        favourite_search_manager = ClientSearch.FavouriteSearchManager()
        
        ClientDefaults.SetDefaultFavouriteSearchManagerData( favourite_search_manager )
        
        self.modules_serialisable.SetJSONDump( favourite_search_manager )
        
        tag_display_manager = ClientTagsHandling.TagDisplayManager()
        
        self.modules_serialisable.SetJSONDump( tag_display_manager )
        
        from hydrus.client.gui.lists import ClientGUIListManager
        
        column_list_manager = ClientGUIListManager.ColumnListManager()
        
        self.modules_serialisable.SetJSONDump( column_list_manager )
        
        self._Execute( 'INSERT INTO namespaces ( namespace_id, namespace ) VALUES ( ?, ? );', ( 1, '' ) )
        
        self._Execute( 'INSERT INTO version ( version ) VALUES ( ? );', ( HC.SOFTWARE_VERSION, ) )
        
        self._ExecuteMany( 'INSERT INTO json_dumps_named VALUES ( ?, ?, ?, ?, ? );', ClientDefaults.GetDefaultScriptRows() )
        
    
    def _CullFileViewingStatistics( self ):
        
        media_min = self._controller.new_options.GetNoneableInteger( 'file_viewing_statistics_media_min_time' )
        media_max = self._controller.new_options.GetNoneableInteger( 'file_viewing_statistics_media_max_time' )
        preview_min = self._controller.new_options.GetNoneableInteger( 'file_viewing_statistics_preview_min_time' )
        preview_max = self._controller.new_options.GetNoneableInteger( 'file_viewing_statistics_preview_max_time' )
        
        if media_min is not None and media_max is not None and media_min > media_max:
            
            raise Exception( 'Media min was greater than media max! Abandoning cull now!' )
            
        
        if preview_min is not None and preview_max is not None and preview_min > preview_max:
            
            raise Exception( 'Preview min was greater than preview max! Abandoning cull now!' )
            
        
        if media_min is not None:
            
            self._Execute( 'UPDATE file_viewing_stats SET views = CAST( viewtime / ? AS INTEGER ) WHERE views * ? > viewtime AND canvas_type = ?;', ( media_min, media_min, CC.CANVAS_MEDIA_VIEWER ) )
            
        
        if media_max is not None:
            
            self._Execute( 'UPDATE file_viewing_stats SET viewtime = views * ? WHERE viewtime > views * ? AND canvas_type = ?;', ( media_max, media_max, CC.CANVAS_MEDIA_VIEWER ) )
            
        
        if preview_min is not None:
            
            self._Execute( 'UPDATE file_viewing_stats SET views = CAST( viewtime / ? AS INTEGER ) WHERE views * ? > viewtime AND canvas_type = ?;', ( preview_min, preview_min, CC.CANVAS_PREVIEW ) )
            
        
        if preview_max is not None:
            
            self._Execute( 'UPDATE file_viewing_stats SET viewtime = views * ? WHERE viewtime > views * ? AND canvas_type = ?;', ( preview_max, preview_max, CC.CANVAS_PREVIEW ) )
            
        
    
    def _DeleteFiles( self, service_id, hash_ids, only_if_current = False ):
        
        # the gui sometimes gets out of sync and sends a DELETE FROM TRASH call before the SEND TO TRASH call
        # in this case, let's make sure the local file domains are clear before deleting from the umbrella domain
        
        local_file_service_ids = self.modules_services.GetServiceIds( ( HC.LOCAL_FILE_DOMAIN, ) )
        
        if service_id == self.modules_services.combined_local_file_service_id:
            
            for local_file_service_id in local_file_service_ids:
                
                self._DeleteFiles( local_file_service_id, hash_ids, only_if_current = True )
                
            
            self._DeleteFiles( self.modules_services.trash_service_id, hash_ids )
            
        
        service = self.modules_services.GetService( service_id )
        
        service_type = service.GetServiceType()
        
        existing_hash_ids_to_timestamps = self.modules_files_storage.GetCurrentHashIdsToTimestamps( service_id, hash_ids )
        
        existing_hash_ids = set( existing_hash_ids_to_timestamps.keys() )
        
        service_info_updates = []
        
        # do delete outside, file repos and perhaps some other bananas situation can delete without ever having added
        
        now = HydrusData.GetNow()
        
        if service_type not in HC.FILE_SERVICES_WITH_NO_DELETE_RECORD:
            
            if only_if_current:
                
                deletee_hash_ids = existing_hash_ids
                
            else:
                
                deletee_hash_ids = hash_ids
                
            
            if len( deletee_hash_ids ) > 0:
                
                insert_rows = [ ( hash_id, existing_hash_ids_to_timestamps[ hash_id ] if hash_id in existing_hash_ids_to_timestamps else None ) for hash_id in deletee_hash_ids ]
                
                num_new_deleted_files = self.modules_files_storage.RecordDeleteFiles( service_id, insert_rows )
                
                service_info_updates.append( ( num_new_deleted_files, service_id, HC.SERVICE_INFO_NUM_DELETED_FILES ) )
                
            
        
        if len( existing_hash_ids_to_timestamps ) > 0:
            
            # remove them from the service
            
            pending_changed = self.modules_files_storage.RemoveFiles( service_id, existing_hash_ids )
            
            if pending_changed:
                
                self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
                
            
            delta_size = self.modules_files_metadata_basic.GetTotalSize( existing_hash_ids )
            num_viewable_files = self.modules_files_metadata_basic.GetNumViewable( existing_hash_ids )
            num_existing_files_removed = len( existing_hash_ids )
            num_inbox = len( existing_hash_ids.intersection( self.modules_files_metadata_basic.inbox_hash_ids ) )
            
            service_info_updates.append( ( -delta_size, service_id, HC.SERVICE_INFO_TOTAL_SIZE ) )
            service_info_updates.append( ( -num_viewable_files, service_id, HC.SERVICE_INFO_NUM_VIEWABLE_FILES ) )
            service_info_updates.append( ( -num_existing_files_removed, service_id, HC.SERVICE_INFO_NUM_FILES ) )
            service_info_updates.append( ( -num_inbox, service_id, HC.SERVICE_INFO_NUM_INBOX ) )
            
            # now do special stuff
            
            # if we maintain tag counts for this service, update
            
            if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
                with self._MakeTemporaryIntegerTable( existing_hash_ids, 'hash_id' ) as temp_hash_id_table_name:
                    
                    for tag_service_id in tag_service_ids:
                        
                        self._CacheSpecificMappingsDeleteFiles( service_id, tag_service_id, existing_hash_ids, temp_hash_id_table_name )
                        
                    
                
            
            # update the combined deleted file service
            
            if service_type in HC.FILE_SERVICES_COVERED_BY_COMBINED_DELETED_FILE:
                
                now = HydrusData.GetNow()
                
                rows = [ ( hash_id, now ) for hash_id in existing_hash_ids ]
                
                self._AddFiles( self.modules_services.combined_deleted_file_service_id, rows )
                
            
            # if any files are no longer in any local file services, send them to the trash
            
            if service_id in local_file_service_ids:
                
                hash_ids_still_in_another_service = set()
                
                other_local_file_service_ids = set( local_file_service_ids )
                other_local_file_service_ids.discard( service_id )
                
                hash_ids_still_in_another_service = self.modules_files_storage.FilterAllCurrentHashIds( existing_hash_ids, just_these_service_ids = other_local_file_service_ids )
                
                trashed_hash_ids = existing_hash_ids.difference( hash_ids_still_in_another_service )
                
                if len( trashed_hash_ids ) > 0:
                    
                    now = HydrusData.GetNow()
                    
                    delete_rows = [ ( hash_id, now ) for hash_id in trashed_hash_ids ]
                    
                    self._AddFiles( self.modules_services.trash_service_id, delete_rows )
                    
                
            
            # if the files are being fully deleted, then physically delete them
            
            if service_id == self.modules_services.combined_local_file_service_id:
                
                self._ArchiveFiles( hash_ids )
                
                for hash_id in hash_ids:
                    
                    self.modules_similar_files.StopSearchingFile( hash_id )
                    
                
                self.modules_files_maintenance_queue.CancelFiles( hash_ids )
                
                self.modules_hashes_local_cache.DropHashIdsFromCache( existing_hash_ids )
                
            
        
        # push the info updates, notify
        
        self._ExecuteMany( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', service_info_updates )
        
    
    def _DeletePending( self, service_key ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        service = self.modules_services.GetService( service_id )
        
        if service.GetServiceType() == HC.TAG_REPOSITORY:
            
            ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( service_id )
            
            pending_rescinded_mappings_ids = list( HydrusData.BuildKeyToListDict( self._Execute( 'SELECT tag_id, hash_id FROM ' + pending_mappings_table_name + ';' ) ).items() )
            
            petitioned_rescinded_mappings_ids = list( HydrusData.BuildKeyToListDict( self._Execute( 'SELECT tag_id, hash_id FROM ' + petitioned_mappings_table_name + ';' ) ).items() )
            
            self._UpdateMappings( service_id, pending_rescinded_mappings_ids = pending_rescinded_mappings_ids, petitioned_rescinded_mappings_ids = petitioned_rescinded_mappings_ids )
            
            self._Execute( 'DELETE FROM tag_sibling_petitions WHERE service_id = ?;', ( service_id, ) )
            self._Execute( 'DELETE FROM tag_parent_petitions WHERE service_id = ?;', ( service_id, ) )
            
        elif service.GetServiceType() in ( HC.FILE_REPOSITORY, HC.IPFS ):
            
            self.modules_files_storage.DeletePending( service_id )
            
        
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
        
        self.pub_service_updates_after_commit( { service_key : [ HydrusData.ServiceUpdate( HC.SERVICE_UPDATE_DELETE_PENDING ) ] } )
        
    
    def _DeleteService( self, service_id ):
        
        service = self.modules_services.GetService( service_id )
        
        service_key = service.GetServiceKey()
        service_type = service.GetServiceType()
        
        # for a long time, much of this was done with foreign keys, which had to be turned on especially for this operation
        # however, this seemed to cause some immense temp drive space bloat when dropping the mapping tables, as there seems to be a trigger/foreign reference check for every row to be deleted
        # so now we just blat all tables and trust in the Lord that we don't forget to add any new ones in future
        
        self._Execute( 'DELETE FROM local_ratings WHERE service_id = ?;', ( service_id, ) )
        self._Execute( 'DELETE FROM recent_tags WHERE service_id = ?;', ( service_id, ) )
        self._Execute( 'DELETE FROM service_info WHERE service_id = ?;', ( service_id, ) )
        
        self._DeleteServiceDropFiles( service_id, service_type )
        
        if service_type in HC.REPOSITORIES:
            
            self.modules_repositories.DropRepositoryTables( service_id )
            
        
        self._DeleteServiceDropMappings( service_id, service_type )
        
        if service_type in HC.REAL_TAG_SERVICES:
            
            interested_service_ids = set( self.modules_tag_display.GetInterestedServiceIds( service_id ) )
            
            interested_service_ids.discard( service_id ) # lmao, not any more!
            
            self.modules_tag_parents.Drop( service_id )
            
            self.modules_tag_siblings.Drop( service_id )
            
            if len( interested_service_ids ) > 0:
                
                self.modules_tag_display.RegenerateTagSiblingsAndParentsCache( only_these_service_ids = interested_service_ids )
                
            
            self.modules_tag_search.Drop( self.modules_services.combined_file_service_id, service_id )
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            for file_service_id in file_service_ids:
                
                self.modules_tag_search.Drop( file_service_id, service_id )
                
            
        
        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
            for tag_service_id in tag_service_ids:
                
                self.modules_tag_search.Drop( service_id, tag_service_id )
                
            
        
        self.modules_services.DeleteService( service_id )
        
        service_update = HydrusData.ServiceUpdate( HC.SERVICE_UPDATE_RESET )
        
        service_keys_to_service_updates = { service_key : [ service_update ] }
        
        self.pub_service_updates_after_commit( service_keys_to_service_updates )
        
    
    def _DeleteServiceDirectory( self, service_id, dirname ):
        
        directory_id = self.modules_texts.GetTextId( dirname )
        
        self._Execute( 'DELETE FROM service_directories WHERE service_id = ? AND directory_id = ?;', ( service_id, directory_id ) )
        self._Execute( 'DELETE FROM service_directory_file_map WHERE service_id = ? AND directory_id = ?;', ( service_id, directory_id ) )
        
    
    def _DeleteServiceDropFiles( self, service_id, service_type ):
        
        if service_type == HC.FILE_REPOSITORY:
            
            self._Execute( 'DELETE FROM remote_thumbnails WHERE service_id = ?;', ( service_id, ) )
            
        
        if service_type == HC.IPFS:
            
            self._Execute( 'DELETE FROM service_filenames WHERE service_id = ?;', ( service_id, ) )
            self._Execute( 'DELETE FROM service_directories WHERE service_id = ?;', ( service_id, ) )
            self._Execute( 'DELETE FROM service_directory_file_map WHERE service_id = ?;', ( service_id, ) )
            
        
        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
            
            self.modules_files_storage.DropFilesTables( service_id )
            
        
        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
            for tag_service_id in tag_service_ids:
                
                self._CacheSpecificMappingsDrop( service_id, tag_service_id )
                
            
        
    
    def _DeleteServiceDropMappings( self, service_id, service_type ):
        
        if service_type in HC.REAL_TAG_SERVICES:
            
            self.modules_mappings_storage.DropMappingsTables( service_id )
            
            self._CacheCombinedFilesMappingsDrop( service_id )
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for file_service_id in file_service_ids:
                
                self._CacheSpecificMappingsDrop( file_service_id, service_id )
                
            
        
    
    def _DeleteServiceInfo( self, service_key = None, types_to_delete = None ):
        
        predicates = []
        
        if service_key is not None:
            
            service_id = self.modules_services.GetServiceId( service_key )
            
            predicates.append( 'service_id = {}'.format( service_id ) )
            
        
        if types_to_delete is not None:
            
            predicates.append( 'info_type IN {}'.format( HydrusData.SplayListForDB( types_to_delete ) ) )
            
        
        if len( predicates ) > 0:
            
            predicates_string = ' WHERE {}'.format( ' AND '.join( predicates ) )
            
        else:
            
            predicates_string = ''
            
        
        self._Execute( 'DELETE FROM service_info{};'.format( predicates_string ) )
        
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
        
    
    def _DisplayCatastrophicError( self, text ):
        
        message = 'The db encountered a serious error! This is going to be written to the log as well, but here it is for a screenshot:'
        message += os.linesep * 2
        message += text
        
        HydrusData.DebugPrint( message )
        
        self._controller.SafeShowCriticalMessage( 'hydrus db failed', message )
        
    
    def _DoAfterJobWork( self ):
        
        for service_keys_to_content_updates in self._after_job_content_update_jobs:
            
            self._weakref_media_result_cache.ProcessContentUpdates( service_keys_to_content_updates )
            
            self._cursor_transaction_wrapper.pub_after_job( 'content_updates_gui', service_keys_to_content_updates )
            
        
        if len( self._regen_tags_managers_hash_ids ) > 0:
            
            hash_ids_to_do = self._weakref_media_result_cache.FilterFiles( self._regen_tags_managers_hash_ids )
            
            if len( hash_ids_to_do ) > 0:
                
                hash_ids_to_tags_managers = self._GetForceRefreshTagsManagers( hash_ids_to_do )
                
                self._weakref_media_result_cache.SilentlyTakeNewTagsManagers( hash_ids_to_tags_managers )
                
            
        
        if len( self._regen_tags_managers_tag_ids ) > 0:
            
            tag_ids_to_tags = self.modules_tags_local_cache.GetTagIdsToTags( tag_ids = self._regen_tags_managers_tag_ids )
            
            tags = { tag_ids_to_tags[ tag_id ] for tag_id in self._regen_tags_managers_tag_ids }
            
            hash_ids_to_do = self._weakref_media_result_cache.FilterFilesWithTags( tags )
            
            if len( hash_ids_to_do ) > 0:
                
                hash_ids_to_tags_managers = self._GetForceRefreshTagsManagers( hash_ids_to_do )
            
                self._weakref_media_result_cache.SilentlyTakeNewTagsManagers( hash_ids_to_tags_managers )
                
                self._cursor_transaction_wrapper.pub_after_job( 'refresh_all_tag_presentation_gui' )
                
            
        
        HydrusDB.HydrusDB._DoAfterJobWork( self )
        
    
    def _DuplicatesGetRandomPotentialDuplicateHashes( self, file_search_context: ClientSearch.FileSearchContext, both_files_match, pixel_dupes_preference, max_hamming_distance ):
        
        db_location_context = self.modules_files_storage.GetDBLocationContext( file_search_context.GetLocationContext() )
        
        is_complicated_search = False
        
        with self._MakeTemporaryIntegerTable( [], 'hash_id' ) as temp_table_name:
            
            # first we get a sample of current potential pairs in the db, given our limiting search context
            
            allowed_hash_ids = None
            preferred_hash_ids = None
            
            if file_search_context.IsJustSystemEverything() or file_search_context.HasNoPredicates():
                
                table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnEverythingSearchResults( db_location_context, pixel_dupes_preference, max_hamming_distance )
                
            else:
                
                is_complicated_search = True
                
                query_hash_ids = self._GetHashIdsFromQuery( file_search_context, apply_implicit_limit = False )
                
                if both_files_match:
                    
                    allowed_hash_ids = query_hash_ids
                    
                else:
                    
                    preferred_hash_ids = query_hash_ids
                    
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO {} ( hash_id ) VALUES ( ? );'.format( temp_table_name ), ( ( hash_id, ) for hash_id in query_hash_ids ) )
                
                self._AnalyzeTempTable( temp_table_name )
                
                table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnSearchResults( db_location_context, temp_table_name, both_files_match, pixel_dupes_preference, max_hamming_distance )
                
            
            potential_media_ids = set()
            
            # distinct important here for the search results table join
            for ( smaller_media_id, larger_media_id ) in self._Execute( 'SELECT DISTINCT smaller_media_id, larger_media_id FROM {};'.format( table_join ) ):
                
                potential_media_ids.add( smaller_media_id )
                potential_media_ids.add( larger_media_id )
                
                if len( potential_media_ids ) >= 1000:
                    
                    break
                    
                
            
            # now let's randomly select a file in these medias
            
            potential_media_ids = list( potential_media_ids )
            
            random.shuffle( potential_media_ids )
            
            chosen_hash_id = None
            
            for potential_media_id in potential_media_ids:
                
                best_king_hash_id = self.modules_files_duplicates.DuplicatesGetBestKingId( potential_media_id, db_location_context, allowed_hash_ids = allowed_hash_ids, preferred_hash_ids = preferred_hash_ids )
                
                if best_king_hash_id is not None:
                    
                    chosen_hash_id = best_king_hash_id
                    
                    break
                    
                
            
        
        if chosen_hash_id is None:
            
            return []
            
        
        hash = self.modules_hashes_local_cache.GetHash( chosen_hash_id )
        
        if is_complicated_search and both_files_match:
            
            allowed_hash_ids = query_hash_ids
            
        else:
            
            allowed_hash_ids = None
            
        
        location_context = file_search_context.GetLocationContext()
        
        return self.modules_files_duplicates.DuplicatesGetFileHashesByDuplicateType( location_context, hash, HC.DUPLICATE_POTENTIAL, allowed_hash_ids = allowed_hash_ids, preferred_hash_ids = preferred_hash_ids )
        
    
    def _DuplicatesGetPotentialDuplicatePairsForFiltering( self, file_search_context: ClientSearch.FileSearchContext, both_files_match, pixel_dupes_preference, max_hamming_distance ):
        
        # we need to batch non-intersecting decisions here to keep it simple at the gui-level
        # we also want to maximise per-decision value
        
        # now we will fetch some unknown pairs
        
        db_location_context = self.modules_files_storage.GetDBLocationContext( file_search_context.GetLocationContext() )
        
        with self._MakeTemporaryIntegerTable( [], 'hash_id' ) as temp_table_name:
            
            allowed_hash_ids = None
            preferred_hash_ids = None
            
            if file_search_context.IsJustSystemEverything() or file_search_context.HasNoPredicates():
                
                table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnEverythingSearchResults( db_location_context, pixel_dupes_preference, max_hamming_distance )
                
            else:
                
                query_hash_ids = self._GetHashIdsFromQuery( file_search_context, apply_implicit_limit = False )
                
                if both_files_match:
                    
                    allowed_hash_ids = query_hash_ids
                    
                else:
                    
                    preferred_hash_ids = query_hash_ids
                    
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO {} ( hash_id ) VALUES ( ? );'.format( temp_table_name ), ( ( hash_id, ) for hash_id in query_hash_ids ) )
                
                self._AnalyzeTempTable( temp_table_name )
                
                table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnSearchResults( db_location_context, temp_table_name, both_files_match, pixel_dupes_preference, max_hamming_distance )
                
            
            # distinct important here for the search results table join
            result = self._Execute( 'SELECT DISTINCT smaller_media_id, larger_media_id, distance FROM {} LIMIT 2500;'.format( table_join ) ).fetchall()
            
        
        MAX_BATCH_SIZE = HG.client_controller.new_options.GetInteger( 'duplicate_filter_max_batch_size' )
        
        batch_of_pairs_of_media_ids = []
        seen_media_ids = set()
        
        distances_to_pairs = HydrusData.BuildKeyToListDict( ( ( distance, ( smaller_media_id, larger_media_id ) ) for ( smaller_media_id, larger_media_id, distance ) in result ) )
        
        distances = sorted( distances_to_pairs.keys() )
        
        # we want to preference pairs that have the smallest distance between them. deciding on more similar files first helps merge dupes before dealing with alts so reduces potentials more quickly
        for distance in distances:
            
            result_pairs_for_this_distance = distances_to_pairs[ distance ]
            
            # convert them into possible groups per each possible 'master hash_id', and value them
            
            master_media_ids_to_groups = collections.defaultdict( list )
            
            for pair in result_pairs_for_this_distance:
                
                ( smaller_media_id, larger_media_id ) = pair
                
                master_media_ids_to_groups[ smaller_media_id ].append( pair )
                master_media_ids_to_groups[ larger_media_id ].append( pair )
                
            
            master_hash_ids_to_values = collections.Counter()
            
            for ( media_id, pairs ) in master_media_ids_to_groups.items():
                
                # negative so we later serve up smallest groups first
                # we shall say for now that smaller groups are more useful to front-load because it lets us solve simple problems first
                master_hash_ids_to_values[ media_id ] = - len( pairs )
                
            
            # now let's add decision groups to our batch
            # we exclude hashes we have seen before in each batch so we aren't treading over ground that was implicitly solved by a previous decision in the batch
            
            for ( master_media_id, count ) in master_hash_ids_to_values.most_common():
                
                if master_media_id in seen_media_ids:
                    
                    continue
                    
                
                seen_media_ids_for_this_master_media_id = set()
                
                for pair in master_media_ids_to_groups[ master_media_id ]:
                    
                    ( smaller_media_id, larger_media_id ) = pair
                    
                    if smaller_media_id in seen_media_ids or larger_media_id in seen_media_ids:
                        
                        continue
                        
                    
                    seen_media_ids_for_this_master_media_id.add( smaller_media_id )
                    seen_media_ids_for_this_master_media_id.add( larger_media_id )
                    
                    batch_of_pairs_of_media_ids.append( pair )
                    
                    if len( batch_of_pairs_of_media_ids ) >= MAX_BATCH_SIZE:
                        
                        break
                        
                    
                
                seen_media_ids.update( seen_media_ids_for_this_master_media_id )
                
                if len( batch_of_pairs_of_media_ids ) >= MAX_BATCH_SIZE:
                    
                    break
                    
                
            
            if len( batch_of_pairs_of_media_ids ) >= MAX_BATCH_SIZE:
                
                break
                
            
        
        seen_hash_ids = set()
        
        media_ids_to_best_king_ids = {}
        
        for media_id in seen_media_ids:
            
            best_king_hash_id = self.modules_files_duplicates.DuplicatesGetBestKingId( media_id, db_location_context, allowed_hash_ids = allowed_hash_ids, preferred_hash_ids = preferred_hash_ids )
            
            if best_king_hash_id is not None:
                
                seen_hash_ids.add( best_king_hash_id )
                
                media_ids_to_best_king_ids[ media_id ] = best_king_hash_id
                
            
        
        batch_of_pairs_of_hash_ids = [ ( media_ids_to_best_king_ids[ smaller_media_id ], media_ids_to_best_king_ids[ larger_media_id ] ) for ( smaller_media_id, larger_media_id ) in batch_of_pairs_of_media_ids if smaller_media_id in media_ids_to_best_king_ids and larger_media_id in media_ids_to_best_king_ids ]
        
        hash_ids_to_hashes = self.modules_hashes_local_cache.GetHashIdsToHashes( hash_ids = seen_hash_ids )
        
        batch_of_pairs_of_hashes = [ ( hash_ids_to_hashes[ hash_id_a ], hash_ids_to_hashes[ hash_id_b ] ) for ( hash_id_a, hash_id_b ) in batch_of_pairs_of_hash_ids ]
        
        return batch_of_pairs_of_hashes
        
    
    def _DuplicatesGetPotentialDuplicatesCount( self, file_search_context, both_files_match, pixel_dupes_preference, max_hamming_distance ):
        
        db_location_context = self.modules_files_storage.GetDBLocationContext( file_search_context.GetLocationContext() )
        
        with self._MakeTemporaryIntegerTable( [], 'hash_id' ) as temp_table_name:
            
            if file_search_context.IsJustSystemEverything() or file_search_context.HasNoPredicates():
                
                table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnEverythingSearchResults( db_location_context, pixel_dupes_preference, max_hamming_distance )
                
            else:
                
                query_hash_ids = self._GetHashIdsFromQuery( file_search_context, apply_implicit_limit = False )
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO {} ( hash_id ) VALUES ( ? );'.format( temp_table_name ), ( ( hash_id, ) for hash_id in query_hash_ids ) )
                
                self._AnalyzeTempTable( temp_table_name )
                
                table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnSearchResults( db_location_context, temp_table_name, both_files_match, pixel_dupes_preference, max_hamming_distance )
                
            
            # distinct important here for the search results table join
            ( potential_duplicates_count, ) = self._Execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT smaller_media_id, larger_media_id FROM {} );'.format( table_join ) ).fetchone()
            
        
        return potential_duplicates_count
        
    
    def _DuplicatesSetDuplicatePairStatus( self, pair_info ):
        
        for ( duplicate_type, hash_a, hash_b, service_keys_to_content_updates ) in pair_info:
            
            if len( service_keys_to_content_updates ) > 0:
                
                self._ProcessContentUpdates( service_keys_to_content_updates )
                
            
            hash_id_a = self.modules_hashes_local_cache.GetHashId( hash_a )
            hash_id_b = self.modules_hashes_local_cache.GetHashId( hash_b )
            
            media_id_a = self.modules_files_duplicates.DuplicatesGetMediaId( hash_id_a )
            media_id_b = self.modules_files_duplicates.DuplicatesGetMediaId( hash_id_b )
            
            smaller_media_id = min( media_id_a, media_id_b )
            larger_media_id = max( media_id_a, media_id_b )
            
            # this shouldn't be strictly needed, but lets do it here anyway to catch unforeseen problems
            # it is ok to remove this even if we are just about to add it back in--this clears out invalid pairs and increases priority with distance 0
            self._Execute( 'DELETE FROM potential_duplicate_pairs WHERE smaller_media_id = ? AND larger_media_id = ?;', ( smaller_media_id, larger_media_id ) )
            
            if hash_id_a == hash_id_b:
                
                continue
                
            
            if duplicate_type in ( HC.DUPLICATE_FALSE_POSITIVE, HC.DUPLICATE_ALTERNATE ):
                
                if duplicate_type == HC.DUPLICATE_FALSE_POSITIVE:
                    
                    alternates_group_id_a = self.modules_files_duplicates.DuplicatesGetAlternatesGroupId( media_id_a )
                    alternates_group_id_b = self.modules_files_duplicates.DuplicatesGetAlternatesGroupId( media_id_b )
                    
                    self.modules_files_duplicates.DuplicatesSetFalsePositive( alternates_group_id_a, alternates_group_id_b )
                    
                elif duplicate_type == HC.DUPLICATE_ALTERNATE:
                    
                    self.modules_files_duplicates.DuplicatesSetAlternates( media_id_a, media_id_b )
                    
                
            elif duplicate_type in ( HC.DUPLICATE_BETTER, HC.DUPLICATE_WORSE, HC.DUPLICATE_SAME_QUALITY ):
                
                if duplicate_type == HC.DUPLICATE_WORSE:
                    
                    ( hash_id_a, hash_id_b ) = ( hash_id_b, hash_id_a )
                    ( media_id_a, media_id_b ) = ( media_id_b, media_id_a )
                    
                    duplicate_type = HC.DUPLICATE_BETTER
                    
                
                king_hash_id_a = self.modules_files_duplicates.DuplicatesGetKingHashId( media_id_a )
                king_hash_id_b = self.modules_files_duplicates.DuplicatesGetKingHashId( media_id_b )
                
                if duplicate_type == HC.DUPLICATE_BETTER:
                    
                    if media_id_a == media_id_b:
                        
                        if hash_id_b == king_hash_id_b:
                            
                            # user manually set that a > King A, hence we are setting a new king within a group
                            
                            self.modules_files_duplicates.DuplicatesSetKing( hash_id_a, media_id_a )
                            
                        
                    else:
                        
                        if hash_id_b != king_hash_id_b:
                            
                            # user manually set that a member of A is better than a non-King of B. remove b from B and merge it into A
                            
                            self.modules_files_duplicates.DuplicatesRemoveMediaIdMember( hash_id_b )
                            
                            media_id_b = self.modules_files_duplicates.DuplicatesGetMediaId( hash_id_b )
                            
                            # b is now the King of its new group
                            
                        
                        # a member of A is better than King B, hence B can merge into A
                        
                        self.modules_files_duplicates.DuplicatesMergeMedias( media_id_a, media_id_b )
                        
                    
                elif duplicate_type == HC.DUPLICATE_SAME_QUALITY:
                    
                    if media_id_a != media_id_b:
                        
                        a_is_king = hash_id_a == king_hash_id_a
                        b_is_king = hash_id_b == king_hash_id_b
                        
                        if not ( a_is_king or b_is_king ):
                            
                            # if neither file is the king, remove B from B and merge it into A
                            
                            self.modules_files_duplicates.DuplicatesRemoveMediaIdMember( hash_id_b )
                            
                            media_id_b = self.modules_files_duplicates.DuplicatesGetMediaId( hash_id_b )
                            
                            superior_media_id = media_id_a
                            mergee_media_id = media_id_b
                            
                        elif not a_is_king:
                            
                            # if one of our files is not the king, merge into that group, as the king of that is better than all of the other
                            
                            superior_media_id = media_id_a
                            mergee_media_id = media_id_b
                            
                        elif not b_is_king:
                            
                            superior_media_id = media_id_b
                            mergee_media_id = media_id_a
                            
                        else:
                            
                            # if both are king, merge into A
                            
                            superior_media_id = media_id_a
                            mergee_media_id = media_id_b
                            
                        
                        self.modules_files_duplicates.DuplicatesMergeMedias( superior_media_id, mergee_media_id )
                        
                    
                
            elif duplicate_type == HC.DUPLICATE_POTENTIAL:
                
                potential_duplicate_media_ids_and_distances = [ ( media_id_b, 0 ) ]
                
                self.modules_files_duplicates.DuplicatesAddPotentialDuplicates( media_id_a, potential_duplicate_media_ids_and_distances )
                
            
        
    
    def _FilterExistingTags( self, service_key, tags ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        tag_ids_to_tags = { self.modules_tags.GetTagId( tag ) : tag for tag in tags }
        
        tag_ids = set( tag_ids_to_tags.keys() )
        
        with self._MakeTemporaryIntegerTable( tag_ids, 'tag_id' ) as temp_tag_id_table_name:
            
            counts = self.modules_mappings_counts.GetCountsForTags( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, service_id, temp_tag_id_table_name )
            
        
        existing_tag_ids = [ tag_id for ( tag_id, current_count, pending_count ) in counts if current_count > 0 ]
        
        filtered_tags = { tag_ids_to_tags[ tag_id ] for tag_id in existing_tag_ids }
        
        return filtered_tags
        
    
    def _FilterExistingUpdateMappings( self, tag_service_id, mappings_ids, action ):
        
        if len( mappings_ids ) == 0:
            
            return mappings_ids
            
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        culled_mappings_ids = []
        
        for ( tag_id, hash_ids ) in mappings_ids:
            
            if len( hash_ids ) == 0:
                
                continue
                
            elif len( hash_ids ) == 1:
                
                ( hash_id, ) = hash_ids
                
                if action == HC.CONTENT_UPDATE_ADD:
                    
                    result = self._Execute( 'SELECT 1 FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( current_mappings_table_name ), ( tag_id, hash_id ) ).fetchone()
                    
                    if result is None:
                        
                        valid_hash_ids = hash_ids
                        
                    else:
                        
                        continue
                        
                    
                elif action == HC.CONTENT_UPDATE_DELETE:
                    
                    result = self._Execute( 'SELECT 1 FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( deleted_mappings_table_name ), ( tag_id, hash_id ) ).fetchone()
                    
                    if result is None:
                        
                        valid_hash_ids = hash_ids
                        
                    else:
                        
                        continue
                        
                    
                elif action == HC.CONTENT_UPDATE_PEND:
                    
                    result = self._Execute( 'SELECT 1 FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( current_mappings_table_name ), ( tag_id, hash_id ) ).fetchone()
                    
                    if result is None:
                        
                        result = self._Execute( 'SELECT 1 FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( pending_mappings_table_name ), ( tag_id, hash_id ) ).fetchone()
                        
                        if result is None:
                            
                            valid_hash_ids = hash_ids
                            
                        else:
                            
                            continue
                            
                        
                    else:
                        
                        continue
                        
                    
                elif action == HC.CONTENT_UPDATE_RESCIND_PEND:
                    
                    result = self._Execute( 'SELECT 1 FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( pending_mappings_table_name ), ( tag_id, hash_id ) ).fetchone()
                    
                    if result is None:
                        
                        continue
                        
                    else:
                        
                        valid_hash_ids = hash_ids
                        
                    
                
            else:
                
                with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
                    
                    if action == HC.CONTENT_UPDATE_ADD:
                        
                        existing_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = ?;'.format( temp_hash_ids_table_name, current_mappings_table_name ), ( tag_id, ) ) )
                        
                        valid_hash_ids = set( hash_ids ).difference( existing_hash_ids )
                        
                    elif action == HC.CONTENT_UPDATE_DELETE:
                        
                        existing_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = ?;'.format( temp_hash_ids_table_name, deleted_mappings_table_name ), ( tag_id, ) ) )
                        
                        valid_hash_ids = set( hash_ids ).difference( existing_hash_ids )
                        
                    elif action == HC.CONTENT_UPDATE_PEND:
                        
                        existing_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = ?;'.format( temp_hash_ids_table_name, current_mappings_table_name ), ( tag_id, ) ) )
                        existing_hash_ids.update( self._STI( self._Execute( 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = ?;'.format( temp_hash_ids_table_name, pending_mappings_table_name ), ( tag_id, ) ) ) )
                        
                        valid_hash_ids = set( hash_ids ).difference( existing_hash_ids )
                        
                    elif action == HC.CONTENT_UPDATE_RESCIND_PEND:
                        
                        valid_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = ?;'.format( temp_hash_ids_table_name, pending_mappings_table_name ), ( tag_id, ) ) )
                        
                    
                
            
            if len( valid_hash_ids ) > 0:
                
                culled_mappings_ids.append( ( tag_id, valid_hash_ids ) )
                
            
        
        return culled_mappings_ids
        
    
    def _FilterForFileDeleteLock( self, service_id, hash_ids ):
        
        # eventually extend this to the metadata conditional object
        
        if HG.client_controller.new_options.GetBoolean( 'delete_lock_for_archived_files' ):
            
            service = self.modules_services.GetService( service_id )
            
            if service.GetServiceType() in HC.LOCAL_FILE_SERVICES:
                
                hash_ids = set( hash_ids ).intersection( self.modules_files_metadata_basic.inbox_hash_ids )
                
            
        
        return hash_ids
        
    
    def _FilterHashesByService( self, file_service_key: bytes, hashes: typing.Sequence[ bytes ] ) -> typing.List[ bytes ]:
        
        # returns hashes in order, to be nice to UI
        
        if file_service_key == CC.COMBINED_FILE_SERVICE_KEY:
            
            return list( hashes )
            
        
        service_id = self.modules_services.GetServiceId( file_service_key )
        
        hashes_to_hash_ids = { hash : self.modules_hashes_local_cache.GetHashId( hash ) for hash in hashes if self.modules_hashes.HasHash( hash ) }
        
        valid_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( service_id, set( hashes_to_hash_ids.values() ), HC.CONTENT_STATUS_CURRENT )
        
        return [ hash for hash in hashes if hash in hashes_to_hash_ids and hashes_to_hash_ids[ hash ] in valid_hash_ids ]
        
    
    def _FixLogicallyInconsistentMappings( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        total_fixed = 0
        
        try:
            
            job_key.SetStatusTitle( 'fixing logically inconsistent mappings' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'fixing {}'.format( tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                
                time.sleep( 0.01 )
                
                ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
                
                #
                
                both_current_and_pending_mappings = list(
                    HydrusData.BuildKeyToSetDict(
                        self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( tag_id, hash_id );'.format( pending_mappings_table_name, current_mappings_table_name ) )
                    ).items()
                )
                
                total_fixed += sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in both_current_and_pending_mappings ) )
                
                self._UpdateMappings( tag_service_id, pending_rescinded_mappings_ids = both_current_and_pending_mappings )
                
                #
                
                both_deleted_and_petitioned_mappings = list(
                    HydrusData.BuildKeyToSetDict(
                        self._Execute( 'SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( tag_id, hash_id );'.format( petitioned_mappings_table_name, deleted_mappings_table_name ) )
                    ).items()
                )
                
                total_fixed += sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in both_deleted_and_petitioned_mappings ) )
                
                self._UpdateMappings( tag_service_id, petitioned_rescinded_mappings_ids = both_deleted_and_petitioned_mappings )
                
            
        finally:
            
            if total_fixed == 0:
                
                HydrusData.ShowText( 'No inconsistent mappings found!' )
                
            else:
                
                self._Execute( 'DELETE FROM service_info where info_type IN ( ?, ? );', ( HC.SERVICE_INFO_NUM_PENDING_MAPPINGS, HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS ) )
                
                self._controller.pub( 'notify_new_pending' )
                
                HydrusData.ShowText( 'Found {} bad mappings! They _should_ be deleted, and your pending counts should be updated.'.format( HydrusData.ToHumanInt( total_fixed ) ) )
                
            
            job_key.DeleteVariable( 'popup_text_2' )
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
        
    
    def _GenerateDBJob( self, job_type, synchronous, action, *args, **kwargs ):
        
        return JobDatabaseClient( job_type, synchronous, action, *args, **kwargs )
        
    
    def _GeneratePredicatesFromTagIdsAndCounts( self, tag_display_type: int, display_tag_service_id: int, tag_ids_to_full_counts, inclusive, job_key = None ):
        
        tag_ids = set( tag_ids_to_full_counts.keys() )
        
        predicates = []
        
        if tag_display_type == ClientTags.TAG_DISPLAY_STORAGE:
            
            if display_tag_service_id != self.modules_services.combined_tag_service_id:
                
                tag_ids_to_ideal_tag_ids = self.modules_tag_siblings.GetTagsToIdeals( ClientTags.TAG_DISPLAY_ACTUAL, display_tag_service_id, tag_ids )
                
                tag_ids_that_are_sibling_chained = self.modules_tag_siblings.FilterChained( ClientTags.TAG_DISPLAY_ACTUAL, display_tag_service_id, tag_ids )
                
                tag_ids_to_ideal_tag_ids_for_siblings = { tag_id : ideal_tag_id for ( tag_id, ideal_tag_id ) in tag_ids_to_ideal_tag_ids.items() if tag_id in tag_ids_that_are_sibling_chained }
                
                ideal_tag_ids_to_sibling_chain_tag_ids = self.modules_tag_siblings.GetIdealsToChains( ClientTags.TAG_DISPLAY_ACTUAL, display_tag_service_id, set( tag_ids_to_ideal_tag_ids_for_siblings.values() ) )
                
                #
                
                ideal_tag_ids = set( tag_ids_to_ideal_tag_ids.values() )
                
                ideal_tag_ids_that_are_parent_chained = self.modules_tag_parents.FilterChained( ClientTags.TAG_DISPLAY_ACTUAL, display_tag_service_id, ideal_tag_ids )
                
                tag_ids_to_ideal_tag_ids_for_parents = { tag_id : ideal_tag_id for ( tag_id, ideal_tag_id ) in tag_ids_to_ideal_tag_ids.items() if ideal_tag_id in ideal_tag_ids_that_are_parent_chained }
                
                ideal_tag_ids_to_ancestor_tag_ids = self.modules_tag_parents.GetTagsToAncestors( ClientTags.TAG_DISPLAY_ACTUAL, display_tag_service_id, set( tag_ids_to_ideal_tag_ids_for_parents.values() ) )
                
            else:
                
                # shouldn't ever happen with storage display
                
                tag_ids_to_ideal_tag_ids_for_siblings = {}
                tag_ids_to_ideal_tag_ids_for_parents = {}
                
                ideal_tag_ids_to_sibling_chain_tag_ids = {}
                
                ideal_tag_ids_to_ancestor_tag_ids = {}
                
            
            tag_ids_we_want_to_look_up = set( tag_ids )
            tag_ids_we_want_to_look_up.update( itertools.chain.from_iterable( ideal_tag_ids_to_sibling_chain_tag_ids.values() ) )
            tag_ids_we_want_to_look_up.update( itertools.chain.from_iterable( ideal_tag_ids_to_ancestor_tag_ids.values() ) )
            
            if job_key is not None and job_key.IsCancelled():
                
                return []
                
            
            tag_ids_to_tags = self.modules_tags_local_cache.GetTagIdsToTags( tag_ids = tag_ids_we_want_to_look_up )
            
            if job_key is not None and job_key.IsCancelled():
                
                return []
                
            
            ideal_tag_ids_to_chain_tags = { ideal_tag_id : { tag_ids_to_tags[ chain_tag_id ] for chain_tag_id in chain_tag_ids } for ( ideal_tag_id, chain_tag_ids ) in ideal_tag_ids_to_sibling_chain_tag_ids.items() }
            
            ideal_tag_ids_to_ancestor_tags = { ideal_tag_id : { tag_ids_to_tags[ ancestor_tag_id ] for ancestor_tag_id in ancestor_tag_ids } for ( ideal_tag_id, ancestor_tag_ids ) in ideal_tag_ids_to_ancestor_tag_ids.items() }
            
            for ( tag_id, ( min_current_count, max_current_count, min_pending_count, max_pending_count ) ) in tag_ids_to_full_counts.items():
                
                tag = tag_ids_to_tags[ tag_id ]
                
                predicate = ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_TAG, value = tag, inclusive = inclusive, count = ClientSearch.PredicateCount( min_current_count, min_pending_count, max_current_count, max_pending_count ) )
                
                if tag_id in tag_ids_to_ideal_tag_ids_for_siblings:
                    
                    ideal_tag_id = tag_ids_to_ideal_tag_ids_for_siblings[ tag_id ]
                    
                    if ideal_tag_id != tag_id:
                        
                        predicate.SetIdealSibling( tag_ids_to_tags[ ideal_tag_id ] )
                        
                    
                    predicate.SetKnownSiblings( ideal_tag_ids_to_chain_tags[ ideal_tag_id ] )
                    
                
                if tag_id in tag_ids_to_ideal_tag_ids_for_parents:
                    
                    ideal_tag_id = tag_ids_to_ideal_tag_ids_for_parents[ tag_id ]
                    
                    parents = ideal_tag_ids_to_ancestor_tags[ ideal_tag_id ]
                    
                    if len( parents ) > 0:
                        
                        predicate.SetKnownParents( parents )
                        
                    
                
                predicates.append( predicate )
                
            
        elif tag_display_type == ClientTags.TAG_DISPLAY_ACTUAL:
            
            tag_ids_to_known_chain_tag_ids = collections.defaultdict( set )
            
            if display_tag_service_id == self.modules_services.combined_tag_service_id:
                
                search_tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                search_tag_service_ids = ( display_tag_service_id, )
                
            
            for search_tag_service_id in search_tag_service_ids:
                
                tag_ids_that_are_sibling_chained = self.modules_tag_siblings.FilterChained( ClientTags.TAG_DISPLAY_ACTUAL, search_tag_service_id, tag_ids )
                
                tag_ids_to_ideal_tag_ids_for_siblings = self.modules_tag_siblings.GetTagsToIdeals( ClientTags.TAG_DISPLAY_ACTUAL, search_tag_service_id, tag_ids_that_are_sibling_chained )
                
                ideal_tag_ids = set( tag_ids_to_ideal_tag_ids_for_siblings.values() )
                
                ideal_tag_ids_to_sibling_chain_tag_ids = self.modules_tag_siblings.GetIdealsToChains( ClientTags.TAG_DISPLAY_ACTUAL, search_tag_service_id, ideal_tag_ids )
                
                for ( tag_id, ideal_tag_id ) in tag_ids_to_ideal_tag_ids_for_siblings.items():
                    
                    tag_ids_to_known_chain_tag_ids[ tag_id ].update( ideal_tag_ids_to_sibling_chain_tag_ids[ ideal_tag_id ] )
                    
                
            
            tag_ids_we_want_to_look_up = set( tag_ids ).union( itertools.chain.from_iterable( tag_ids_to_known_chain_tag_ids.values() ) )
            
            if job_key is not None and job_key.IsCancelled():
                
                return []
                
            
            tag_ids_to_tags = self.modules_tags_local_cache.GetTagIdsToTags( tag_ids = tag_ids_we_want_to_look_up )
            
            if job_key is not None and job_key.IsCancelled():
                
                return []
                
            
            for ( tag_id, ( min_current_count, max_current_count, min_pending_count, max_pending_count ) ) in tag_ids_to_full_counts.items():
                
                tag = tag_ids_to_tags[ tag_id ]
                
                predicate = ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_TAG, value = tag, inclusive = inclusive, count = ClientSearch.PredicateCount( min_current_count, min_pending_count, max_current_count, max_pending_count ) )
                
                if tag_id in tag_ids_to_known_chain_tag_ids:
                    
                    chain_tags = { tag_ids_to_tags[ chain_tag_id ] for chain_tag_id in tag_ids_to_known_chain_tag_ids[ tag_id ] }
                    
                    predicate.SetKnownSiblings( chain_tags )
                    
                
                predicates.append( predicate )
                
            
        
        return predicates
        
    
    def _GetAllTagIds( self, leaf: ClientDBServices.FileSearchContextLeaf, job_key = None ):
        
        tag_ids = set()
        
        query = '{};'.format( self.modules_tag_search.GetQueryPhraseForTagIds( leaf.file_service_id, leaf.tag_service_id ) )
        
        cursor = self._Execute( query )
        
        cancelled_hook = None
        
        if job_key is not None:
            
            cancelled_hook = job_key.IsCancelled
            
        
        loop_of_tag_ids = self._STS( HydrusDB.ReadFromCancellableCursor( cursor, 1024, cancelled_hook = cancelled_hook ) )
        
        if job_key is not None and job_key.IsCancelled():
            
            return set()
            
        
        tag_ids.update( loop_of_tag_ids )
        
        return tag_ids
        
    
    def _GetAutocompleteCountEstimate( self, tag_display_type: int, tag_service_id: int, file_service_id: int, tag_ids: typing.Collection[ int ], include_current_tags: bool, include_pending_tags: bool ):
        
        count = 0
        
        if not include_current_tags and not include_pending_tags:
            
            return count
            
        
        ( current_count, pending_count ) = self._GetAutocompleteCountEstimateStatuses( tag_display_type, tag_service_id, file_service_id, tag_ids )
        
        if include_current_tags:
            
            count += current_count
            
        
        if include_current_tags:
            
            count += pending_count
            
        
        return count
        
    
    def _GetAutocompleteCountEstimateStatuses( self, tag_display_type: int, tag_service_id: int, file_service_id: int, tag_ids: typing.Collection[ int ] ):
        
        include_current_tags = True
        include_pending_tags = True
        
        ids_to_count = self.modules_mappings_counts.GetCounts( tag_display_type, tag_service_id, file_service_id, tag_ids, include_current_tags, include_pending_tags )
        
        current_count = 0
        pending_count = 0
        
        for ( current_min, current_max, pending_min, pending_max ) in ids_to_count.values():
            
            current_count += current_min
            pending_count += pending_min
            
        
        return ( current_count, pending_count )
        
    
    def _GetAutocompleteTagIdsLeaf( self, tag_display_type: int, leaf: ClientDBServices.FileSearchContextLeaf, search_text, exact_match, job_key = None ):
        
        if search_text == '':
            
            return set()
            
        
        ( namespace, half_complete_searchable_subtag ) = HydrusTags.SplitTag( search_text )
        
        if half_complete_searchable_subtag == '':
            
            return set()
            
        
        if namespace == '*':
            
            namespace = ''
            
        
        if exact_match:
            
            if '*' in namespace or '*' in half_complete_searchable_subtag:
                
                return []
                
            
        
        if namespace == '':
            
            namespace_ids = []
            
        elif '*' in namespace:
            
            namespace_ids = self.modules_tag_search.GetNamespaceIdsFromWildcard( namespace )
            
        else:
            
            if not self.modules_tags.NamespaceExists( namespace ):
                
                return set()
                
            
            namespace_ids = ( self.modules_tags.GetNamespaceId( namespace ), )
            
        
        if half_complete_searchable_subtag == '*':
            
            if namespace == '':
                
                # hellmode 'get all tags' search
                
                tag_ids = self._GetAllTagIds( leaf, job_key = job_key )
                
            else:
                
                tag_ids = self._GetTagIdsFromNamespaceIds( leaf, namespace_ids, job_key = job_key )
                
            
        else:
            
            tag_ids = set()
            
            with self._MakeTemporaryIntegerTable( [], 'subtag_id' ) as temp_subtag_ids_table_name:
                
                self.modules_tag_search.GetSubtagIdsFromWildcardIntoTable( leaf.file_service_id, leaf.tag_service_id, half_complete_searchable_subtag, temp_subtag_ids_table_name, job_key = job_key )
                
                if namespace == '':
                    
                    loop_of_tag_ids = self._GetTagIdsFromSubtagIdsTable( leaf.file_service_id, leaf.tag_service_id, temp_subtag_ids_table_name, job_key = job_key )
                    
                else:
                    
                    with self._MakeTemporaryIntegerTable( namespace_ids, 'namespace_id' ) as temp_namespace_ids_table_name:
                        
                        loop_of_tag_ids = self._GetTagIdsFromNamespaceIdsSubtagIdsTables( leaf.file_service_id, leaf.tag_service_id, temp_namespace_ids_table_name, temp_subtag_ids_table_name, job_key = job_key )
                        
                    
                
                tag_ids.update( loop_of_tag_ids )
                
            
        
        # now fetch siblings, add to set
        
        if not isinstance( tag_ids, set ):
            
            tag_ids = set( tag_ids )
            
        
        tag_ids_without_siblings = list( tag_ids )
        
        seen_ideal_tag_ids = collections.defaultdict( set )
        
        for batch_of_tag_ids in HydrusData.SplitListIntoChunks( tag_ids_without_siblings, 10240 ):
            
            with self._MakeTemporaryIntegerTable( batch_of_tag_ids, 'tag_id' ) as temp_tag_ids_table_name:
                
                if job_key is not None and job_key.IsCancelled():
                    
                    return set()
                    
                
                with self._MakeTemporaryIntegerTable( [], 'ideal_tag_id' ) as temp_ideal_tag_ids_table_name:
                    
                    self.modules_tag_siblings.FilterChainedIdealsIntoTable( ClientTags.TAG_DISPLAY_ACTUAL, leaf.tag_service_id, temp_tag_ids_table_name, temp_ideal_tag_ids_table_name )
                    
                    with self._MakeTemporaryIntegerTable( [], 'tag_id' ) as temp_chained_tag_ids_table_name:
                        
                        self.modules_tag_siblings.GetChainsMembersFromIdealsTables( ClientTags.TAG_DISPLAY_ACTUAL, leaf.tag_service_id, temp_ideal_tag_ids_table_name, temp_chained_tag_ids_table_name )
                        
                        tag_ids.update( self._STI( self._Execute( 'SELECT tag_id FROM {};'.format( temp_chained_tag_ids_table_name ) ) ) )
                        
                    
                
            
        
        return tag_ids
        
    
    def _GetAutocompletePredicates(
        self,
        tag_display_type: int,
        file_search_context: ClientSearch.FileSearchContext,
        search_text: str = '',
        exact_match = False,
        inclusive = True,
        add_namespaceless = False,
        search_namespaces_into_full_tags = False,
        zero_count_ok = False,
        job_key = None
    ):
        
        location_context = file_search_context.GetLocationContext()
        tag_search_context = file_search_context.GetTagSearchContext()
        
        display_tag_service_id = self.modules_services.GetServiceId( tag_search_context.display_service_key )
        
        if tag_search_context.IsAllKnownTags() and location_context.IsAllKnownFiles():
            
            return []
            
        
        include_current = tag_search_context.include_current_tags
        include_pending = tag_search_context.include_pending_tags
        
        all_predicates = []
        
        file_search_context_branch = self._GetFileSearchContextBranch( file_search_context )
        
        for leaf in file_search_context_branch.IterateLeaves():
            
            tag_ids = self._GetAutocompleteTagIdsLeaf( tag_display_type, leaf, search_text, exact_match, job_key = job_key )
            
            if ':' not in search_text and search_namespaces_into_full_tags and not exact_match:
                
                # 'char' -> 'character:samus aran'
                
                special_search_text = '{}*:*'.format( search_text )
                
                tag_ids.update( self._GetAutocompleteTagIdsLeaf( tag_display_type, leaf, special_search_text, exact_match, job_key = job_key ) )
                
            
            if job_key is not None and job_key.IsCancelled():
                
                return []
                
            
            domain_is_cross_referenced = leaf.file_service_id != self.modules_services.combined_deleted_file_service_id
            
            for group_of_tag_ids in HydrusData.SplitIteratorIntoChunks( tag_ids, 1000 ):
                
                if job_key is not None and job_key.IsCancelled():
                    
                    return []
                    
                
                ids_to_count = self.modules_mappings_counts.GetCounts( tag_display_type, leaf.tag_service_id, leaf.file_service_id, group_of_tag_ids, include_current, include_pending, domain_is_cross_referenced = domain_is_cross_referenced, zero_count_ok = zero_count_ok, job_key = job_key )
                
                if len( ids_to_count ) == 0:
                    
                    continue
                    
                
                #
                
                predicates = self._GeneratePredicatesFromTagIdsAndCounts( tag_display_type, display_tag_service_id, ids_to_count, inclusive, job_key = job_key )
                
                all_predicates.extend( predicates )
                
            
            if job_key is not None and job_key.IsCancelled():
                
                return []
                
            
        
        predicates = ClientSearch.MergePredicates( all_predicates, add_namespaceless = add_namespaceless )
        
        return predicates
        
    
    def _GetBonedStats( self ):
        
        boned_stats = {}
        
        with self._MakeTemporaryIntegerTable( [], 'hash_id' ) as temp_hash_id_table_name:
            
            current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_CURRENT )
            
            self._Execute( 'INSERT INTO {} ( hash_id ) SELECT hash_id FROM {};'.format( temp_hash_id_table_name, current_files_table_name ) )
            
            for service_id in ( self.modules_services.trash_service_id, self.modules_services.local_update_service_id ):
                
                current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( service_id, HC.CONTENT_STATUS_CURRENT )
                
                self._Execute( 'DELETE FROM {} WHERE hash_id IN ( SELECT hash_id FROM {} );'.format( temp_hash_id_table_name, current_files_table_name ) )
                
            
            ( num_total, size_total ) = self._Execute( 'SELECT COUNT( hash_id ), SUM( size ) FROM {} CROSS JOIN files_info USING ( hash_id );'.format( temp_hash_id_table_name ) ).fetchone()
            ( num_inbox, size_inbox ) = self._Execute( 'SELECT COUNT( hash_id ), SUM( size ) FROM files_info NATURAL JOIN {} NATURAL JOIN file_inbox;'.format( temp_hash_id_table_name ) ).fetchone()
            
            if size_total is None:
                
                size_total = 0
                
            
            if size_inbox is None:
                
                size_inbox = 0
                
            
        
        with self._MakeTemporaryIntegerTable( [], 'hash_id' ) as temp_hash_id_table_name:
            
            deleted_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_DELETED )
            
            self._Execute( 'INSERT INTO {} ( hash_id ) SELECT hash_id FROM {};'.format( temp_hash_id_table_name, deleted_files_table_name ) )
            
            current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.trash_service_id, HC.CONTENT_STATUS_CURRENT )
            
            self._Execute( 'INSERT OR IGNORE INTO {} ( hash_id ) SELECT hash_id FROM {};'.format( temp_hash_id_table_name, current_files_table_name ) )
            
            ( num_deleted, size_deleted ) = self._Execute( 'SELECT COUNT( hash_id ), SUM( size ) FROM {} CROSS JOIN files_info USING ( hash_id );'.format( temp_hash_id_table_name ) ).fetchone()
            
            if size_deleted is None:
                
                size_deleted = 0
                
            
        
        num_archive = num_total - num_inbox
        size_archive = size_total - size_inbox
        
        boned_stats[ 'num_inbox' ] = num_inbox
        boned_stats[ 'num_archive' ] = num_archive
        boned_stats[ 'num_deleted' ] = num_deleted
        boned_stats[ 'size_inbox' ] = size_inbox
        boned_stats[ 'size_archive' ] = size_archive
        boned_stats[ 'size_deleted' ] = size_deleted
        
        canvas_types_to_total_viewtimes = { canvas_type : ( views, viewtime ) for ( canvas_type, views, viewtime ) in self._Execute( 'SELECT canvas_type, SUM( views ), SUM( viewtime ) FROM file_viewing_stats GROUP BY canvas_type;' ) }
        
        if CC.CANVAS_PREVIEW not in canvas_types_to_total_viewtimes:
            
            canvas_types_to_total_viewtimes[ CC.CANVAS_PREVIEW ] = ( 0, 0 )
            
        
        if CC.CANVAS_MEDIA_VIEWER not in canvas_types_to_total_viewtimes:
            
            canvas_types_to_total_viewtimes[ CC.CANVAS_MEDIA_VIEWER ] = ( 0, 0 )
            
        
        total_viewtime = canvas_types_to_total_viewtimes[ CC.CANVAS_MEDIA_VIEWER ] + canvas_types_to_total_viewtimes[ CC.CANVAS_PREVIEW ]
        
        #
        
        earliest_import_time = 0
        
        current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_CURRENT )
        
        result = self._Execute( 'SELECT MIN( timestamp ) FROM {};'.format( current_files_table_name ) ).fetchone()
        
        if result is not None and result[0] is not None:
            
            earliest_import_time = result[0]
            
        
        deleted_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_DELETED )
        
        result = self._Execute( 'SELECT MIN( original_timestamp ) FROM {};'.format( deleted_files_table_name ) ).fetchone()
        
        if result is not None and result[0] is not None:
            
            if earliest_import_time == 0:
                
                earliest_import_time = result[0]
                
            else:
                
                earliest_import_time = min( earliest_import_time, result[0] )
                
            
        
        if earliest_import_time > 0:
            
            boned_stats[ 'earliest_import_time' ] = earliest_import_time
            
        
        #
        
        boned_stats[ 'total_viewtime' ] = total_viewtime
        
        total_alternate_files = sum( ( count for ( alternates_group_id, count ) in self._Execute( 'SELECT alternates_group_id, COUNT( * ) FROM alternate_file_group_members GROUP BY alternates_group_id;' ) if count > 1 ) )
        total_duplicate_files = sum( ( count for ( media_id, count ) in self._Execute( 'SELECT media_id, COUNT( * ) FROM duplicate_file_members GROUP BY media_id;' ) if count > 1 ) )
        
        location_context = ClientLocation.GetLocationContextForAllLocalMedia()
        
        db_location_context = self.modules_files_storage.GetDBLocationContext( location_context )
        
        table_join = self.modules_files_duplicates.DuplicatesGetPotentialDuplicatePairsTableJoinOnFileService( db_location_context )
        
        ( total_potential_pairs, ) = self._Execute( 'SELECT COUNT( * ) FROM ( SELECT DISTINCT smaller_media_id, larger_media_id FROM {} );'.format( table_join ) ).fetchone()
        
        boned_stats[ 'total_alternate_files' ] = total_alternate_files
        boned_stats[ 'total_duplicate_files' ] = total_duplicate_files
        boned_stats[ 'total_potential_pairs' ] = total_potential_pairs
        
        return boned_stats
        
    
    def _GetClientFilesLocations( self ):
        
        result = { prefix : HydrusPaths.ConvertPortablePathToAbsPath( location ) for ( prefix, location ) in self._Execute( 'SELECT prefix, location FROM client_files_locations;' ) }
        
        if len( result ) < 512:
            
            message = 'When fetching the directories where your files are stored, the database discovered some entries were missing!'
            message += os.linesep * 2
            message += 'Default values will now be inserted. If you have previously migrated your files or thumbnails, and assuming this is occuring on boot, you will next be presented with a dialog to remap them to the correct location.'
            message += os.linesep * 2
            message += 'If this is not happening on client boot, you should kill the hydrus process right now, as a serious hard drive fault has likely recently occurred.'
            
            self._DisplayCatastrophicError( message )
            
            client_files_default = os.path.join( self._db_dir, 'client_files' )
            
            HydrusPaths.MakeSureDirectoryExists( client_files_default )
            
            location = HydrusPaths.ConvertAbsPathToPortablePath( client_files_default )
            
            for prefix in HydrusData.IterateHexPrefixes():
                
                self._Execute( 'INSERT OR IGNORE INTO client_files_locations ( prefix, location ) VALUES ( ?, ? );', ( 'f' + prefix, location ) )
                self._Execute( 'INSERT OR IGNORE INTO client_files_locations ( prefix, location ) VALUES ( ?, ? );', ( 't' + prefix, location ) )
                
            
        
        return result
        
    
    def _GetFileNotes( self, hash ):
        
        hash_id = self.modules_hashes_local_cache.GetHashId( hash )
        
        names_to_notes = { name : note for ( name, note ) in self._Execute( 'SELECT label, note FROM file_notes, labels, notes ON ( file_notes.name_id = labels.label_id AND file_notes.note_id = notes.note_id ) WHERE hash_id = ?;', ( hash_id, ) ) }
        
        return names_to_notes
        
    
    def _GetFileSearchContextBranch( self, file_search_context: ClientSearch.FileSearchContext ) -> ClientDBServices.FileSearchContextBranch:
        
        location_context = file_search_context.GetLocationContext()
        tag_search_context = file_search_context.GetTagSearchContext()
        
        ( file_service_keys, file_location_is_cross_referenced ) = location_context.GetCoveringCurrentFileServiceKeys()
        
        search_file_service_ids = []
        
        for file_service_key in file_service_keys:
            
            try:
                
                search_file_service_id = self.modules_services.GetServiceId( file_service_key )
                
            except HydrusExceptions.DataMissing:
                
                HydrusData.ShowText( 'A query was run for a file service that does not exist! If you just removed a service, you might want to try checking the search and/or restarting the client.' )
                
                continue
                
            
            search_file_service_ids.append( search_file_service_id )
            
        
        if tag_search_context.IsAllKnownTags():
            
            search_tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            try:
                
                search_tag_service_ids = ( self.modules_services.GetServiceId( tag_search_context.service_key ), )
                
            except HydrusExceptions.DataMissing:
                
                HydrusData.ShowText( 'A query was run for a tag service that does not exist! If you just removed a service, you might want to try checking the search and/or restarting the client.' )
                
                search_tag_service_ids = []
                
            
        
        return ClientDBServices.FileSearchContextBranch( file_search_context, search_file_service_ids, search_tag_service_ids, file_location_is_cross_referenced )
        
    
    def _GetFileSystemPredicates( self, file_search_context: ClientSearch.FileSearchContext, force_system_everything = False ):
        
        location_context = file_search_context.GetLocationContext()
        
        system_everything_limit = 10000
        system_everything_suffix = ''
        
        predicates = []
        
        system_everythings = [ ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_EVERYTHING ) ]
        
        blank_pred_types = {
            ClientSearch.PREDICATE_TYPE_SYSTEM_NUM_TAGS,
            ClientSearch.PREDICATE_TYPE_SYSTEM_LIMIT,
            ClientSearch.PREDICATE_TYPE_SYSTEM_KNOWN_URLS,
            ClientSearch.PREDICATE_TYPE_SYSTEM_HASH,
            ClientSearch.PREDICATE_TYPE_SYSTEM_FILE_SERVICE,
            ClientSearch.PREDICATE_TYPE_SYSTEM_FILE_RELATIONSHIPS,
            ClientSearch.PREDICATE_TYPE_SYSTEM_TAG_AS_NUMBER,
            ClientSearch.PREDICATE_TYPE_SYSTEM_FILE_VIEWING_STATS
        }
        
        if len( self.modules_services.GetServiceIds( HC.RATINGS_SERVICES ) ) > 0:
            
            blank_pred_types.add( ClientSearch.PREDICATE_TYPE_SYSTEM_RATING )
            
        
        if location_context.IsAllKnownFiles():
            
            tag_service_key = file_search_context.GetTagSearchContext().service_key
            
            if tag_service_key == CC.COMBINED_TAG_SERVICE_KEY:
                
                # this shouldn't happen, combined on both sides, but let's do our best anyway
                
                if force_system_everything or self._controller.new_options.GetBoolean( 'always_show_system_everything' ):
                    
                    predicates.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_EVERYTHING ) )
                    
                
            else:
                
                service_id = self.modules_services.GetServiceId( tag_service_key )
                
                service_type = self.modules_services.GetServiceType( service_id )
                
                service_info = self._GetServiceInfoSpecific( service_id, service_type, { HC.SERVICE_INFO_NUM_FILES }, calculate_missing = False )
                
                if HC.SERVICE_INFO_NUM_FILES in service_info:
                    
                    num_everything = service_info[ HC.SERVICE_INFO_NUM_FILES ]
                    
                    system_everythings.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_EVERYTHING, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_everything ) ) )
                    
                
            
        else:
            
            # specific file service(s)
            
            jobs = []
            
            jobs.extend( ( ( file_service_key, HC.CONTENT_STATUS_CURRENT ) for file_service_key in location_context.current_service_keys ) )
            jobs.extend( ( ( file_service_key, HC.CONTENT_STATUS_DELETED ) for file_service_key in location_context.deleted_service_keys ) )
            
            file_repo_preds = []
            inbox_archive_preds = []
            
            we_saw_a_file_repo = False
            
            for ( file_service_key, status ) in jobs:
                
                service_id = self.modules_services.GetServiceId( file_service_key )
                
                service_type = self.modules_services.GetServiceType( service_id )
                
                if service_type not in HC.FILE_SERVICES:
                    
                    continue
                    
                
                if status == HC.CONTENT_STATUS_CURRENT:
                    
                    service_info = self._GetServiceInfoSpecific( service_id, service_type, { HC.SERVICE_INFO_NUM_VIEWABLE_FILES, HC.SERVICE_INFO_NUM_INBOX } )
                    
                    num_everything = service_info[ HC.SERVICE_INFO_NUM_VIEWABLE_FILES ]
                    
                    system_everythings.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_EVERYTHING, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_everything ) ) )
                    
                    if location_context.IncludesDeleted():
                        
                        # inbox/archive and local/remote are too difficult to get good numbers for and merge for deleted, so we'll exclude if this is a mix
                        
                        continue
                        
                    
                    num_inbox = service_info[ HC.SERVICE_INFO_NUM_INBOX ]
                    num_archive = num_everything - num_inbox
                    
                    if service_type == HC.FILE_REPOSITORY:
                        
                        we_saw_a_file_repo = True
                        
                        num_local = self.modules_files_storage.GetNumLocal( service_id )
                        
                        num_not_local = num_everything - num_local
                        
                    else:
                        
                        num_local = num_everything
                        num_not_local = 0
                        
                    
                    file_repo_preds.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_LOCAL, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_local ) ) )
                    file_repo_preds.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_NOT_LOCAL, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_not_local ) ) )
                    
                    num_archive = num_local - num_inbox
                    
                    inbox_archive_preds.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_INBOX, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_inbox ) ) )
                    inbox_archive_preds.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_ARCHIVE, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_archive ) ) )
                    
                elif status == HC.CONTENT_STATUS_DELETED:
                    
                    service_info = self._GetServiceInfoSpecific( service_id, service_type, { HC.SERVICE_INFO_NUM_DELETED_FILES } )
                    
                    num_everything = service_info[ HC.SERVICE_INFO_NUM_DELETED_FILES ]
                    
                    system_everythings.append( ClientSearch.Predicate( ClientSearch.PREDICATE_TYPE_SYSTEM_EVERYTHING, count = ClientSearch.PredicateCount.STATICCreateCurrentCount( num_everything ) ) )
                    
                
            
            if we_saw_a_file_repo:
                
                predicates.extend( file_repo_preds )
                
            
            if len( inbox_archive_preds ) > 0:
                
                inbox_archive_preds = ClientSearch.MergePredicates( inbox_archive_preds )
                
                zero_counts = [ pred.GetCount().HasZeroCount() for pred in inbox_archive_preds ]
                
                if True in zero_counts and self._controller.new_options.GetBoolean( 'filter_inbox_and_archive_predicates' ):
                    
                    if False in zero_counts and location_context.IsOneDomain():
                        
                        # something is in here, but we are hiding, so let's inform system everything
                        useful_pred = list( ( pred for pred in inbox_archive_preds if pred.GetCount().HasNonZeroCount() ) )[0]
                        
                        if useful_pred.GetType() == ClientSearch.PREDICATE_TYPE_SYSTEM_INBOX:
                            
                            system_everything_suffix = 'all in inbox'
                            
                        else:
                            
                            system_everything_suffix = 'all in archive'
                            
                        
                    
                else:
                    
                    predicates.extend( inbox_archive_preds )
                    
                
            
            blank_pred_types.update( [
                ClientSearch.PREDICATE_TYPE_SYSTEM_SIZE,
                ClientSearch.PREDICATE_TYPE_SYSTEM_TIME,
                ClientSearch.PREDICATE_TYPE_SYSTEM_DIMENSIONS,
                ClientSearch.PREDICATE_TYPE_SYSTEM_DURATION,
                ClientSearch.PREDICATE_TYPE_SYSTEM_HAS_AUDIO,
                ClientSearch.PREDICATE_TYPE_SYSTEM_HAS_ICC_PROFILE,
                ClientSearch.PREDICATE_TYPE_SYSTEM_NOTES,
                ClientSearch.PREDICATE_TYPE_SYSTEM_NUM_WORDS,
                ClientSearch.PREDICATE_TYPE_SYSTEM_MIME,
                ClientSearch.PREDICATE_TYPE_SYSTEM_SIMILAR_TO
                ] )
            
        
        if len( system_everythings ) > 0:
            
            system_everythings = ClientSearch.MergePredicates( system_everythings )
            
            system_everything = list( system_everythings )[0]
            
            system_everything.SetCountTextSuffix( system_everything_suffix )
            
            num_everything = system_everything.GetCount().GetMinCount()
            
            if force_system_everything or ( num_everything <= system_everything_limit or self._controller.new_options.GetBoolean( 'always_show_system_everything' ) ):
                
                predicates.append( system_everything )
                
            
        
        predicates.extend( [ ClientSearch.Predicate( predicate_type ) for predicate_type in blank_pred_types ] )
        
        predicates = ClientSearch.MergePredicates( predicates )
        
        def sys_preds_key( s ):
            
            t = s.GetType()
            
            if t == ClientSearch.PREDICATE_TYPE_SYSTEM_EVERYTHING:
                
                return ( 0, 0 )
                
            elif t == ClientSearch.PREDICATE_TYPE_SYSTEM_INBOX:
                
                return ( 1, 0 )
                
            elif t == ClientSearch.PREDICATE_TYPE_SYSTEM_ARCHIVE:
                
                return ( 2, 0 )
                
            elif t == ClientSearch.PREDICATE_TYPE_SYSTEM_LOCAL:
                
                return ( 3, 0 )
                
            elif t == ClientSearch.PREDICATE_TYPE_SYSTEM_NOT_LOCAL:
                
                return ( 4, 0 )
                
            else:
                
                return ( 5, s.ToString() )
                
            
        
        predicates.sort( key = sys_preds_key )
        
        return predicates
        
    
    def _GetForceRefreshTagsManagers( self, hash_ids, hash_ids_to_current_file_service_ids = None ):
        
        with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_table_name:
            
            self._AnalyzeTempTable( temp_table_name )
            
            return self._GetForceRefreshTagsManagersWithTableHashIds( hash_ids, temp_table_name, hash_ids_to_current_file_service_ids = hash_ids_to_current_file_service_ids )
            
        
    
    def _GetForceRefreshTagsManagersWithTableHashIds( self, hash_ids, hash_ids_table_name, hash_ids_to_current_file_service_ids = None ):
        
        if hash_ids_to_current_file_service_ids is None:
            
            hash_ids_to_current_file_service_ids = self.modules_files_storage.GetHashIdsToCurrentServiceIds( hash_ids_table_name )
            
        
        common_file_service_ids_to_hash_ids = self._GroupHashIdsByTagCachedFileServiceId( hash_ids, hash_ids_table_name, hash_ids_to_current_file_service_ids = hash_ids_to_current_file_service_ids )
        
        #
        
        tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
        
        storage_tag_data = []
        display_tag_data = []
        
        for ( common_file_service_id, batch_of_hash_ids ) in common_file_service_ids_to_hash_ids.items():
            
            if len( batch_of_hash_ids ) == len( hash_ids ):
                
                ( batch_of_storage_tag_data, batch_of_display_tag_data ) = self._GetForceRefreshTagsManagersWithTableHashIdsTagData( common_file_service_id, tag_service_ids, hash_ids_table_name )
                
            else:
                
                with self._MakeTemporaryIntegerTable( batch_of_hash_ids, 'hash_id' ) as temp_batch_hash_ids_table_name:
                    
                    ( batch_of_storage_tag_data, batch_of_display_tag_data ) = self._GetForceRefreshTagsManagersWithTableHashIdsTagData( common_file_service_id, tag_service_ids, temp_batch_hash_ids_table_name )
                    
                
            
            storage_tag_data.extend( batch_of_storage_tag_data )
            display_tag_data.extend( batch_of_display_tag_data )
            
        
        seen_tag_ids = { tag_id for ( hash_id, ( tag_service_id, status, tag_id ) ) in storage_tag_data }
        seen_tag_ids.update( ( tag_id for ( hash_id, ( tag_service_id, status, tag_id ) ) in display_tag_data ) )
        
        tag_ids_to_tags = self.modules_tags_local_cache.GetTagIdsToTags( tag_ids = seen_tag_ids )
        
        service_ids_to_service_keys = self.modules_services.GetServiceIdsToServiceKeys()
        
        hash_ids_to_raw_storage_tag_data = HydrusData.BuildKeyToListDict( storage_tag_data )
        hash_ids_to_raw_display_tag_data = HydrusData.BuildKeyToListDict( display_tag_data )
        
        hash_ids_to_tag_managers = {}
        
        for hash_id in hash_ids:
            
            # service_id, status, tag_id
            raw_storage_tag_data = hash_ids_to_raw_storage_tag_data[ hash_id ]
            
            # service_id -> ( status, tag )
            service_ids_to_storage_tag_data = HydrusData.BuildKeyToListDict( ( ( tag_service_id, ( status, tag_ids_to_tags[ tag_id ] ) ) for ( tag_service_id, status, tag_id ) in raw_storage_tag_data ) )
            
            service_keys_to_statuses_to_storage_tags = collections.defaultdict(
                HydrusData.default_dict_set,
                { service_ids_to_service_keys[ tag_service_id ] : HydrusData.BuildKeyToSetDict( status_and_tag ) for ( tag_service_id, status_and_tag ) in service_ids_to_storage_tag_data.items() }
            )
            
            # service_id, status, tag_id
            raw_display_tag_data = hash_ids_to_raw_display_tag_data[ hash_id ]
            
            # service_id -> ( status, tag )
            service_ids_to_display_tag_data = HydrusData.BuildKeyToListDict( ( ( tag_service_id, ( status, tag_ids_to_tags[ tag_id ] ) ) for ( tag_service_id, status, tag_id ) in raw_display_tag_data ) )
            
            service_keys_to_statuses_to_display_tags = collections.defaultdict(
                HydrusData.default_dict_set,
                { service_ids_to_service_keys[ tag_service_id ] : HydrusData.BuildKeyToSetDict( status_and_tag ) for ( tag_service_id, status_and_tag ) in service_ids_to_display_tag_data.items() }
            )
            
            tags_manager = ClientMediaManagers.TagsManager( service_keys_to_statuses_to_storage_tags, service_keys_to_statuses_to_display_tags )
            
            hash_ids_to_tag_managers[ hash_id ] = tags_manager
            
        
        return hash_ids_to_tag_managers
        
    
    def _GetForceRefreshTagsManagersWithTableHashIdsTagData( self, common_file_service_id, tag_service_ids, hash_ids_table_name ):
        
        storage_tag_data = []
        display_tag_data = []
        
        for tag_service_id in tag_service_ids:
            
            statuses_to_table_names = self.modules_mappings_storage.GetFastestStorageMappingTableNames( common_file_service_id, tag_service_id )
            
            for ( status, mappings_table_name ) in statuses_to_table_names.items():
                
                # temp hashes to mappings
                storage_tag_data.extend( ( hash_id, ( tag_service_id, status, tag_id ) ) for ( hash_id, tag_id ) in self._Execute( 'SELECT hash_id, tag_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_ids_table_name, mappings_table_name ) ) )
                
            
            if common_file_service_id != self.modules_services.combined_file_service_id:
                
                ( cache_current_display_mappings_table_name, cache_pending_display_mappings_table_name ) = ClientDBMappingsCacheSpecificDisplay.GenerateSpecificDisplayMappingsCacheTableNames( common_file_service_id, tag_service_id )
                
                # temp hashes to mappings
                display_tag_data.extend( ( hash_id, ( tag_service_id, HC.CONTENT_STATUS_CURRENT, tag_id ) ) for ( hash_id, tag_id ) in self._Execute( 'SELECT hash_id, tag_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_ids_table_name, cache_current_display_mappings_table_name ) ) )
                display_tag_data.extend( ( hash_id, ( tag_service_id, HC.CONTENT_STATUS_PENDING, tag_id ) ) for ( hash_id, tag_id ) in self._Execute( 'SELECT hash_id, tag_id FROM {} CROSS JOIN {} USING ( hash_id );'.format( hash_ids_table_name, cache_pending_display_mappings_table_name ) ) )
                
            
        
        if common_file_service_id == self.modules_services.combined_file_service_id:
            
            # this is likely a 'all known files' query, which means we are in deep water without a cache
            # time to compute manually, which is semi hell mode, but not dreadful
            
            current_and_pending_storage_tag_data = [ ( hash_id, ( tag_service_id, status, tag_id ) ) for ( hash_id, ( tag_service_id, status, tag_id ) ) in storage_tag_data if status in ( HC.CONTENT_STATUS_CURRENT, HC.CONTENT_STATUS_PENDING ) ]
            
            seen_service_ids_to_seen_tag_ids = HydrusData.BuildKeyToSetDict( ( ( tag_service_id, tag_id ) for ( hash_id, ( tag_service_id, status, tag_id ) ) in current_and_pending_storage_tag_data ) )
            
            seen_service_ids_to_tag_ids_to_implied_tag_ids = { tag_service_id : self.modules_tag_display.GetTagsToImplies( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, tag_ids ) for ( tag_service_id, tag_ids ) in seen_service_ids_to_seen_tag_ids.items() }
            
            display_tag_data = []
            
            for ( hash_id, ( tag_service_id, status, tag_id ) ) in current_and_pending_storage_tag_data:
                
                display_tag_data.extend( ( ( hash_id, ( tag_service_id, status, implied_tag_id ) ) for implied_tag_id in seen_service_ids_to_tag_ids_to_implied_tag_ids[ tag_service_id ][ tag_id ] ) )
                
            
        
        return ( storage_tag_data, display_tag_data )
        
    
    def _GetHashIdsAndNonZeroTagCounts( self, tag_display_type: int, location_context: ClientLocation.LocationContext, tag_search_context: ClientSearch.TagSearchContext, hash_ids, namespace_wildcard = None, job_key = None ):
        
        if namespace_wildcard == '*':
            
            namespace_wildcard = None
            
        
        if namespace_wildcard is None:
            
            namespace_ids = []
            
        else:
            
            namespace_ids = self.modules_tag_search.GetNamespaceIdsFromWildcard( namespace_wildcard )
            
        
        with self._MakeTemporaryIntegerTable( namespace_ids, 'namespace_id' ) as temp_namespace_ids_table_name:
            
            ( file_service_keys, file_location_is_cross_referenced ) = location_context.GetCoveringCurrentFileServiceKeys()
            
            mapping_and_tag_table_names = set()
            
            for file_service_key in file_service_keys:
                
                mapping_and_tag_table_names.update( self._GetMappingAndTagTables( tag_display_type, file_service_key, tag_search_context ) )
                
            
            # reason why I (JOIN each table) rather than (join the UNION) is based on previous hell with having query planner figure out a "( a UNION b UNION c ) NATURAL JOIN stuff" situation
            # although the following sometimes makes certifiable 2KB ( 6 UNION * 4-table ) queries, it actually works fast
            
            # OK, a new problem is mass UNION leads to terrible cancelability because the first row cannot be fetched until the first n - 1 union queries are done
            # I tried some gubbins to try to do a pseudo table-union rather than query union and do 'get files->distinct tag count for this union of tables, and fetch hash_ids first on the union', but did not have luck
            
            # so NOW we are just going to do it in bits of files mate. this also reduces memory use from the distinct-making UNION with large numbers of hash_ids
            
            results = []
            
            BLOCK_SIZE = max( 64, int( len( hash_ids ) ** 0.5 ) ) # go for square root for now
            
            for group_of_hash_ids in HydrusData.SplitIteratorIntoChunks( hash_ids, BLOCK_SIZE ):
                
                with self._MakeTemporaryIntegerTable( group_of_hash_ids, 'hash_id' ) as hash_ids_table_name:
                    
                    if namespace_wildcard is None:
                        
                        # temp hashes to mappings
                        select_statements = [ 'SELECT hash_id, tag_id FROM {} CROSS JOIN {} USING ( hash_id )'.format( hash_ids_table_name, mappings_table_name ) for ( mappings_table_name, tags_table_name ) in mapping_and_tag_table_names ]
                        
                    else:
                        
                        # temp hashes to mappings to tags to namespaces
                        select_statements = [ 'SELECT hash_id, tag_id FROM {} CROSS JOIN {} USING ( hash_id ) CROSS JOIN {} USING ( tag_id ) CROSS JOIN {} USING ( namespace_id )'.format( hash_ids_table_name, mappings_table_name, tags_table_name, temp_namespace_ids_table_name ) for ( mappings_table_name, tags_table_name ) in mapping_and_tag_table_names ]
                        
                    
                    unions = '( {} )'.format( ' UNION '.join( select_statements ) )
                    
                    query = 'SELECT hash_id, COUNT( tag_id ) FROM {} GROUP BY hash_id;'.format( unions )
                    
                    cursor = self._Execute( query )
                    
                    cancelled_hook = None
                    
                    if job_key is not None:
                        
                        cancelled_hook = job_key.IsCancelled
                        
                    
                    loop_of_results = HydrusDB.ReadFromCancellableCursor( cursor, 64, cancelled_hook = cancelled_hook )
                    
                    if job_key is not None and job_key.IsCancelled():
                        
                        return results
                        
                    
                    results.extend( loop_of_results )
                    
                
            
            return results
            
        
    
    def _GetHashIdsFromFileViewingStatistics( self, view_type, viewing_locations, operator, viewing_value ):
        
        # only works for positive values like '> 5'. won't work for '= 0' or '< 1' since those are absent from the table
        
        include_media = 'media' in viewing_locations
        include_preview = 'preview' in viewing_locations
        
        group_by_phrase = ''
        
        if view_type == 'views':
            
            content_phrase = 'views'
            
        elif view_type == 'viewtime':
            
            content_phrase = 'viewtime'
            
        
        if include_media and include_preview:
            
            group_by_phrase = ' GROUP BY hash_id'
            
            if view_type == 'views':
                
                content_phrase = 'SUM( views )'
                
            elif view_type == 'viewtime':
                
                content_phrase = 'SUM( viewtime )'
                
            
            canvas_type_predicate = '1=1'
            
        elif include_media:
            
            canvas_type_predicate = 'canvas_type = {}'.format( CC.CANVAS_MEDIA_VIEWER )
            
        elif include_preview:
            
            canvas_type_predicate = 'canvas_type = {}'.format( CC.CANVAS_PREVIEW )
            
        else:
            
            return []
            
        
        if operator == CC.UNICODE_ALMOST_EQUAL_TO:
            
            lower_bound = int( 0.8 * viewing_value )
            upper_bound = int( 1.2 * viewing_value )
            
            test_phrase = content_phrase + ' BETWEEN ' + str( lower_bound ) + ' AND ' + str( upper_bound )
            
        else:
            
            test_phrase = content_phrase + operator + str( viewing_value )
            
        
        select_statement = 'SELECT hash_id FROM file_viewing_stats WHERE {} AND {}{};'.format( test_phrase, canvas_type_predicate, group_by_phrase )
        
        hash_ids = self._STS( self._Execute( select_statement ) )
        
        return hash_ids
        
    
    def _GetHashIdsFromNamespaceIdsSubtagIds( self, tag_display_type: int, file_service_key, tag_search_context: ClientSearch.TagSearchContext, namespace_ids, subtag_ids, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
        
        tag_ids = self._GetTagIdsFromNamespaceIdsSubtagIds( file_service_id, tag_service_id, namespace_ids, subtag_ids, job_key = job_key )
        
        return self._GetHashIdsFromTagIds( tag_display_type, file_service_key, tag_search_context, tag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
        
    
    def _GetHashIdsFromNamespaceIdsSubtagIdsTables( self, tag_display_type: int, file_service_key, tag_search_context: ClientSearch.TagSearchContext, namespace_ids_table_name, subtag_ids_table_name, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
        
        tag_ids = self._GetTagIdsFromNamespaceIdsSubtagIdsTables( file_service_id, tag_service_id, namespace_ids_table_name, subtag_ids_table_name, job_key = job_key )
        
        return self._GetHashIdsFromTagIds( tag_display_type, file_service_key, tag_search_context, tag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
        
    
    def _GetHashIdsFromNoteName( self, name: str, hash_ids_table_name: str ):
        
        label_id = self.modules_texts.GetLabelId( name )
        
        # as note name is rare, we force this to run opposite to typical: notes to temp hashes
        return self._STS( self._Execute( 'SELECT hash_id FROM file_notes CROSS JOIN {} USING ( hash_id ) WHERE name_id = ?;'.format( hash_ids_table_name ), ( label_id, ) ) )
        
    
    def _GetHashIdsFromNumNotes( self, min_num_notes: typing.Optional[ int ], max_num_notes: typing.Optional[ int ], hash_ids_table_name: str ):
        
        has_notes = max_num_notes is None and min_num_notes == 1
        not_has_notes = ( min_num_notes is None or min_num_notes == 0 ) and max_num_notes is not None and max_num_notes == 0
        
        if has_notes or not_has_notes:
            
            has_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {} WHERE EXISTS ( SELECT 1 FROM file_notes WHERE file_notes.hash_id = {}.hash_id );'.format( hash_ids_table_name, hash_ids_table_name ) ) )
            
            if has_notes:
                
                hash_ids = has_hash_ids
                
            else:
                
                all_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {};'.format( hash_ids_table_name ) ) )
                
                hash_ids = all_hash_ids.difference( has_hash_ids )
                
            
        else:
            
            if min_num_notes is None:
                
                filt = lambda c: c <= max_num_notes
                
            elif max_num_notes is None:
                
                filt = lambda c: min_num_notes <= c
                
            else:
                
                filt = lambda c: min_num_notes <= c <= max_num_notes
                
            
            # temp hashes to notes
            query = 'SELECT hash_id, COUNT( * ) FROM {} CROSS JOIN file_notes USING ( hash_id ) GROUP BY hash_id;'.format( hash_ids_table_name )
            
            hash_ids = { hash_id for ( hash_id, count ) in self._Execute( query ) if filt( count ) }
            
        
        return hash_ids
        
    
    def _GetHashIdsFromQuery( self, file_search_context: ClientSearch.FileSearchContext, job_key = None, query_hash_ids = None, apply_implicit_limit = True, sort_by = None, limit_sort_by = None ):
        
        if job_key is None:
            
            job_key = ClientThreading.JobKey( cancellable = True )
            
        
        if query_hash_ids is not None:
            
            query_hash_ids = set( query_hash_ids )
            
        
        have_cross_referenced_file_locations = False
        
        self._controller.ResetIdleTimer()
        
        system_predicates = file_search_context.GetSystemPredicates()
        
        location_context = file_search_context.GetLocationContext()
        tag_search_context = file_search_context.GetTagSearchContext()
        
        tag_service_key = tag_search_context.service_key
        
        include_current_tags = tag_search_context.include_current_tags
        include_pending_tags = tag_search_context.include_pending_tags
        
        if not location_context.SearchesAnything():
            
            return set()
            
        
        current_file_service_ids = set()
        
        for current_service_key in location_context.current_service_keys:
            
            try:
                
                current_file_service_id = self.modules_services.GetServiceId( current_service_key )
                
            except HydrusExceptions.DataMissing:
                
                HydrusData.ShowText( 'A file search query was run for a file service that does not exist! If you just removed a service, you might want to try checking the search and/or restarting the client.' )
                
                return set()
                
            
            current_file_service_ids.add( current_file_service_id )
            
        
        deleted_file_service_ids = set()
        
        for deleted_service_key in location_context.deleted_service_keys:
            
            try:
                
                deleted_file_service_id = self.modules_services.GetServiceId( deleted_service_key )
                
            except HydrusExceptions.DataMissing:
                
                HydrusData.ShowText( 'A file search query was run for a file service that does not exist! If you just removed a service, you might want to try checking the search and/or restarting the client.' )
                
                return set()
                
            
            deleted_file_service_ids.add( deleted_file_service_id )
            
        
        db_location_context = self.modules_files_storage.GetDBLocationContext( location_context )
        
        try:
            
            tag_service_id = self.modules_services.GetServiceId( tag_service_key )
            
        except HydrusExceptions.DataMissing:
            
            HydrusData.ShowText( 'A file search query was run for a tag service that does not exist! If you just removed a service, you might want to try checking the search and/or restarting the client.' )
            
            return set()
            
        
        tags_to_include = file_search_context.GetTagsToInclude()
        tags_to_exclude = file_search_context.GetTagsToExclude()
        
        namespaces_to_include = file_search_context.GetNamespacesToInclude()
        namespaces_to_exclude = file_search_context.GetNamespacesToExclude()
        
        wildcards_to_include = file_search_context.GetWildcardsToInclude()
        wildcards_to_exclude = file_search_context.GetWildcardsToExclude()
        
        simple_preds = system_predicates.GetSimpleInfo()
        
        king_filter = system_predicates.GetKingFilter()
        
        or_predicates = file_search_context.GetORPredicates()
        
        need_file_domain_cross_reference = not location_context.IsAllKnownFiles()
        there_are_tags_to_search = len( tags_to_include ) > 0 or len( namespaces_to_include ) > 0 or len( wildcards_to_include ) > 0
        
        # ok, let's set up the big list of simple search preds
        
        files_info_predicates = []
        
        if 'min_size' in simple_preds: files_info_predicates.append( 'size > ' + str( simple_preds[ 'min_size' ] ) )
        if 'size' in simple_preds: files_info_predicates.append( 'size = ' + str( simple_preds[ 'size' ] ) )
        if 'not_size' in simple_preds: files_info_predicates.append( 'size != ' + str( simple_preds[ 'not_size' ] ) )
        if 'max_size' in simple_preds: files_info_predicates.append( 'size < ' + str( simple_preds[ 'max_size' ] ) )
        
        if 'mimes' in simple_preds:
            
            mimes = simple_preds[ 'mimes' ]
            
            if len( mimes ) == 1:
                
                ( mime, ) = mimes
                
                files_info_predicates.append( 'mime = ' + str( mime ) )
                
            else:
                
                files_info_predicates.append( 'mime IN ' + HydrusData.SplayListForDB( mimes ) )
                
            
        
        if 'has_audio' in simple_preds:
            
            has_audio = simple_preds[ 'has_audio' ]
            
            files_info_predicates.append( 'has_audio = {}'.format( int( has_audio ) ) )
            
        
        if 'min_width' in simple_preds: files_info_predicates.append( 'width > ' + str( simple_preds[ 'min_width' ] ) )
        if 'width' in simple_preds: files_info_predicates.append( 'width = ' + str( simple_preds[ 'width' ] ) )
        if 'not_width' in simple_preds: files_info_predicates.append( 'width != ' + str( simple_preds[ 'not_width' ] ) )
        if 'max_width' in simple_preds: files_info_predicates.append( 'width < ' + str( simple_preds[ 'max_width' ] ) )
        
        if 'min_height' in simple_preds: files_info_predicates.append( 'height > ' + str( simple_preds[ 'min_height' ] ) )
        if 'height' in simple_preds: files_info_predicates.append( 'height = ' + str( simple_preds[ 'height' ] ) )
        if 'not_height' in simple_preds: files_info_predicates.append( 'height != ' + str( simple_preds[ 'not_height' ] ) )
        if 'max_height' in simple_preds: files_info_predicates.append( 'height < ' + str( simple_preds[ 'max_height' ] ) )
        
        if 'min_num_pixels' in simple_preds: files_info_predicates.append( 'width * height > ' + str( simple_preds[ 'min_num_pixels' ] ) )
        if 'num_pixels' in simple_preds: files_info_predicates.append( 'width * height = ' + str( simple_preds[ 'num_pixels' ] ) )
        if 'not_num_pixels' in simple_preds: files_info_predicates.append( 'width * height != ' + str( simple_preds[ 'not_num_pixels' ] ) )
        if 'max_num_pixels' in simple_preds: files_info_predicates.append( 'width * height < ' + str( simple_preds[ 'max_num_pixels' ] ) )
        
        if 'min_ratio' in simple_preds:
            
            ( ratio_width, ratio_height ) = simple_preds[ 'min_ratio' ]
            
            files_info_predicates.append( '( width * 1.0 ) / height > ' + str( float( ratio_width ) ) + ' / ' + str( ratio_height ) )
            
        if 'ratio' in simple_preds:
            
            ( ratio_width, ratio_height ) = simple_preds[ 'ratio' ]
            
            files_info_predicates.append( '( width * 1.0 ) / height = ' + str( float( ratio_width ) ) + ' / ' + str( ratio_height ) )
            
        if 'not_ratio' in simple_preds:
            
            ( ratio_width, ratio_height ) = simple_preds[ 'not_ratio' ]
            
            files_info_predicates.append( '( width * 1.0 ) / height != ' + str( float( ratio_width ) ) + ' / ' + str( ratio_height ) )
            
        if 'max_ratio' in simple_preds:
            
            ( ratio_width, ratio_height ) = simple_preds[ 'max_ratio' ]
            
            files_info_predicates.append( '( width * 1.0 ) / height < ' + str( float( ratio_width ) ) + ' / ' + str( ratio_height ) )
            
        
        if 'min_num_words' in simple_preds: files_info_predicates.append( 'num_words > ' + str( simple_preds[ 'min_num_words' ] ) )
        if 'num_words' in simple_preds:
            
            num_words = simple_preds[ 'num_words' ]
            
            if num_words == 0: files_info_predicates.append( '( num_words IS NULL OR num_words = 0 )' )
            else: files_info_predicates.append( 'num_words = ' + str( num_words ) )
            
        if 'not_num_words' in simple_preds:
            
            num_words = simple_preds[ 'not_num_words' ]
            
            files_info_predicates.append( '( num_words IS NULL OR num_words != {} )'.format( num_words ) )
            
        if 'max_num_words' in simple_preds:
            
            max_num_words = simple_preds[ 'max_num_words' ]
            
            if max_num_words == 0: files_info_predicates.append( 'num_words < ' + str( max_num_words ) )
            else: files_info_predicates.append( '( num_words < ' + str( max_num_words ) + ' OR num_words IS NULL )' )
            
        
        if 'min_duration' in simple_preds: files_info_predicates.append( 'duration > ' + str( simple_preds[ 'min_duration' ] ) )
        if 'duration' in simple_preds:
            
            duration = simple_preds[ 'duration' ]
            
            if duration == 0:
                
                files_info_predicates.append( '( duration = 0 OR duration IS NULL )' )
                
            else:
                
                files_info_predicates.append( 'duration = ' + str( duration ) )
                
            
        if 'not_duration' in simple_preds:
            
            duration = simple_preds[ 'not_duration' ]
            
            files_info_predicates.append( '( duration IS NULL OR duration != {} )'.format( duration ) )
            
        if 'max_duration' in simple_preds:
            
            max_duration = simple_preds[ 'max_duration' ]
            
            if max_duration == 0: files_info_predicates.append( 'duration < ' + str( max_duration ) )
            else: files_info_predicates.append( '( duration < ' + str( max_duration ) + ' OR duration IS NULL )' )
            
        
        if 'min_framerate' in simple_preds or 'framerate' in simple_preds or 'max_framerate' in simple_preds or 'not_framerate' in simple_preds:
            
            if 'not_framerate' in simple_preds:
                
                pred = '( duration IS NULL OR num_frames = 0 OR ( duration IS NOT NULL AND duration != 0 AND num_frames != 0 AND num_frames IS NOT NULL AND {} ) )'
                
                min_framerate_sql = simple_preds[ 'not_framerate' ] * 0.95
                max_framerate_sql = simple_preds[ 'not_framerate' ] * 1.05
                
                pred = pred.format( '( num_frames * 1.0 ) / ( duration / 1000.0 ) NOT BETWEEN {} AND {}'.format( min_framerate_sql, max_framerate_sql ) )
                
            else:
                
                min_framerate_sql = None
                max_framerate_sql = None
                
                pred = '( duration IS NOT NULL AND duration != 0 AND num_frames != 0 AND num_frames IS NOT NULL AND {} )'
                
                if 'min_framerate' in simple_preds:
                    
                    min_framerate_sql = simple_preds[ 'min_framerate' ] * 1.05
                    
                if 'framerate' in simple_preds:
                    
                    min_framerate_sql = simple_preds[ 'framerate' ] * 0.95
                    max_framerate_sql = simple_preds[ 'framerate' ] * 1.05
                    
                if 'max_framerate' in simple_preds:
                    
                    max_framerate_sql = simple_preds[ 'max_framerate' ] * 0.95
                    
                
                if min_framerate_sql is None:
                    
                    pred = pred.format( '( num_frames * 1.0 ) / ( duration / 1000.0 ) < {}'.format( max_framerate_sql ) )
                    
                elif max_framerate_sql is None:
                    
                    pred = pred.format( '( num_frames * 1.0 ) / ( duration / 1000.0 ) > {}'.format( min_framerate_sql ) )
                    
                else:
                    
                    pred = pred.format( '( num_frames * 1.0 ) / ( duration / 1000.0 ) BETWEEN {} AND {}'.format( min_framerate_sql, max_framerate_sql ) )
                    
                
            
            files_info_predicates.append( pred )
            
        
        if 'min_num_frames' in simple_preds: files_info_predicates.append( 'num_frames > ' + str( simple_preds[ 'min_num_frames' ] ) )
        if 'num_frames' in simple_preds:
            
            num_frames = simple_preds[ 'num_frames' ]
            
            if num_frames == 0: files_info_predicates.append( '( num_frames IS NULL OR num_frames = 0 )' )
            else: files_info_predicates.append( 'num_frames = ' + str( num_frames ) )
            
        if 'not_num_frames' in simple_preds:
            
            num_frames = simple_preds[ 'not_num_frames' ]
            
            files_info_predicates.append( '( num_frames IS NULL OR num_frames != {} )'.format( num_frames ) )
            
        if 'max_num_frames' in simple_preds:
            
            max_num_frames = simple_preds[ 'max_num_frames' ]
            
            if max_num_frames == 0: files_info_predicates.append( 'num_frames < ' + str( max_num_frames ) )
            else: files_info_predicates.append( '( num_frames < ' + str( max_num_frames ) + ' OR num_frames IS NULL )' )
            
        
        there_are_simple_files_info_preds_to_search_for = len( files_info_predicates ) > 0
        
        # start with some quick ways to populate query_hash_ids
        
        def intersection_update_qhi( query_hash_ids, some_hash_ids, force_create_new_set = False ):
            
            if query_hash_ids is None:
                
                if not isinstance( some_hash_ids, set ) or force_create_new_set:
                    
                    some_hash_ids = set( some_hash_ids )
                    
                
                return some_hash_ids
                
            else:
                
                query_hash_ids.intersection_update( some_hash_ids )
                
                return query_hash_ids
                
            
        
        #
        
        def do_or_preds( or_predicates, query_hash_ids ):
            
            # better typically to sort by fewest num of preds first, establishing query_hash_ids for longer chains
            def or_sort_key( p ):
                
                return len( p.GetValue() )
                
            
            or_predicates = sorted( or_predicates, key = or_sort_key )
            
            for or_predicate in or_predicates:
                
                # blue eyes OR green eyes
                
                or_query_hash_ids = set()
                
                for or_subpredicate in or_predicate.GetValue():
                    
                    # blue eyes
                    
                    or_search_context = file_search_context.Duplicate()
                    
                    or_search_context.SetPredicates( [ or_subpredicate ] )
                    
                    # I pass current query_hash_ids here to make these inefficient sub-searches (like -tag) potentially much faster
                    or_query_hash_ids.update( self._GetHashIdsFromQuery( or_search_context, job_key, query_hash_ids = query_hash_ids, apply_implicit_limit = False, sort_by = None, limit_sort_by = None ) )
                    
                    if job_key.IsCancelled():
                        
                        return set()
                        
                    
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, or_query_hash_ids )
                
            
            return query_hash_ids
            
        
        #
        
        done_or_predicates = len( or_predicates ) == 0
        
        # OR round one--if nothing else will be fast, let's prep query_hash_ids now
        if not done_or_predicates and not ( there_are_tags_to_search or there_are_simple_files_info_preds_to_search_for ):
            
            query_hash_ids = do_or_preds( or_predicates, query_hash_ids )
            
            have_cross_referenced_file_locations = True
            
            done_or_predicates = True
            
        
        #
        
        if 'hash' in simple_preds:
            
            specific_hash_ids = set()
            
            ( search_hashes, search_hash_type ) = simple_preds[ 'hash' ]
            
            if search_hash_type == 'sha256':
                
                matching_sha256_hashes = [ search_hash for search_hash in search_hashes if self.modules_hashes.HasHash( search_hash ) ]
                
            else:
                
                matching_sha256_hashes = self.modules_hashes.GetFileHashes( search_hashes, search_hash_type, 'sha256' )
                
            
            specific_hash_ids = self.modules_hashes_local_cache.GetHashIds( matching_sha256_hashes )
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, specific_hash_ids )
            
        
        #
        
        if need_file_domain_cross_reference:
            
            # in future we will hang an explicit service off this predicate and specify import/deleted time
            # for now we'll wangle a compromise and just check all, and if domain is deleted, then search deletion time
            
            import_timestamp_predicates = []
            
            if 'min_import_timestamp' in simple_preds: import_timestamp_predicates.append( 'timestamp >= ' + str( simple_preds[ 'min_import_timestamp' ] ) )
            if 'max_import_timestamp' in simple_preds: import_timestamp_predicates.append( 'timestamp <= ' + str( simple_preds[ 'max_import_timestamp' ] ) )
            
            if len( import_timestamp_predicates ) > 0:
                
                pred_string = ' AND '.join( import_timestamp_predicates )
                
                table_names = []
                table_names.extend( ( ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.GetServiceId( service_key ), HC.CONTENT_STATUS_CURRENT ) for service_key in location_context.current_service_keys ) )
                table_names.extend( ( ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.GetServiceId( service_key ), HC.CONTENT_STATUS_DELETED ) for service_key in location_context.deleted_service_keys ) )
                
                import_timestamp_hash_ids = set()
                
                for table_name in table_names:
                    
                    import_timestamp_hash_ids.update( self._STS( self._Execute( 'SELECT hash_id FROM {} WHERE {};'.format( table_name, pred_string ) ) ) )
                    
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, import_timestamp_hash_ids )
                
                have_cross_referenced_file_locations = True
                
            
        
        modified_timestamp_predicates = []
        
        if 'min_modified_timestamp' in simple_preds: modified_timestamp_predicates.append( 'file_modified_timestamp >= ' + str( simple_preds[ 'min_modified_timestamp' ] ) )
        if 'max_modified_timestamp' in simple_preds: modified_timestamp_predicates.append( 'file_modified_timestamp <= ' + str( simple_preds[ 'max_modified_timestamp' ] ) )
        
        if len( modified_timestamp_predicates ) > 0:
            
            pred_string = ' AND '.join( modified_timestamp_predicates )
            
            modified_timestamp_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM file_modified_timestamps WHERE {};'.format( pred_string ) ) )
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, modified_timestamp_hash_ids )
            
        
        last_viewed_timestamp_predicates = []
        
        if 'min_last_viewed_timestamp' in simple_preds: last_viewed_timestamp_predicates.append( 'last_viewed_timestamp >= ' + str( simple_preds[ 'min_last_viewed_timestamp' ] ) )
        if 'max_last_viewed_timestamp' in simple_preds: last_viewed_timestamp_predicates.append( 'last_viewed_timestamp <= ' + str( simple_preds[ 'max_last_viewed_timestamp' ] ) )
        
        if len( last_viewed_timestamp_predicates ) > 0:
            
            pred_string = ' AND '.join( last_viewed_timestamp_predicates )
            
            last_viewed_timestamp_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM file_viewing_stats WHERE canvas_type = ? AND {};'.format( pred_string ), ( CC.CANVAS_MEDIA_VIEWER, ) ) )
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, last_viewed_timestamp_hash_ids )
            
        
        #
        
        if system_predicates.HasSimilarTo():
            
            ( similar_to_hashes, max_hamming ) = system_predicates.GetSimilarTo()
            
            all_similar_hash_ids = set()
            
            for similar_to_hash in similar_to_hashes:
                
                hash_id = self.modules_hashes_local_cache.GetHashId( similar_to_hash )
                
                similar_hash_ids_and_distances = self.modules_similar_files.Search( hash_id, max_hamming )
                
                similar_hash_ids = [ similar_hash_id for ( similar_hash_id, distance ) in similar_hash_ids_and_distances ]
                
                all_similar_hash_ids.update( similar_hash_ids )
                
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, all_similar_hash_ids )
            
        
        for ( operator, value, rating_service_key ) in system_predicates.GetRatingsPredicates():
            
            service_id = self.modules_services.GetServiceId( rating_service_key )
            
            if value == 'not rated':
                
                continue
                
            
            if value == 'rated':
                
                rating_hash_ids = self._STI( self._Execute( 'SELECT hash_id FROM local_ratings WHERE service_id = ?;', ( service_id, ) ) )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, rating_hash_ids )
                
            else:
                
                service = HG.client_controller.services_manager.GetService( rating_service_key )
                
                if service.GetServiceType() == HC.LOCAL_RATING_LIKE:
                    
                    half_a_star_value = 0.5
                    
                else:
                    
                    one_star_value = service.GetOneStarValue()
                    
                    half_a_star_value = one_star_value / 2
                    
                
                if isinstance( value, str ):
                    
                    value = float( value )
                    
                
                # floats are a pain! as is storing rating as 0.0-1.0 and then allowing number of stars to change!
                
                if operator == CC.UNICODE_ALMOST_EQUAL_TO:
                    
                    predicate = str( ( value - half_a_star_value ) * 0.8 ) + ' < rating AND rating < ' + str( ( value + half_a_star_value ) * 1.2 )
                    
                elif operator == '<':
                    
                    predicate = 'rating <= ' + str( value - half_a_star_value )
                    
                elif operator == '>':
                    
                    predicate = 'rating > ' + str( value + half_a_star_value )
                    
                elif operator == '=':
                    
                    predicate = str( value - half_a_star_value ) + ' < rating AND rating <= ' + str( value + half_a_star_value )
                    
                
                rating_hash_ids = self._STI( self._Execute( 'SELECT hash_id FROM local_ratings WHERE service_id = ? AND ' + predicate + ';', ( service_id, ) ) )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, rating_hash_ids )
                
            
        
        is_inbox = system_predicates.MustBeInbox()
        
        if is_inbox:
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, self.modules_files_metadata_basic.inbox_hash_ids, force_create_new_set = True )
            
        
        for ( operator, num_relationships, dupe_type ) in system_predicates.GetDuplicateRelationshipCountPredicates():
            
            only_do_zero = ( operator in ( '=', CC.UNICODE_ALMOST_EQUAL_TO ) and num_relationships == 0 ) or ( operator == '<' and num_relationships == 1 )
            include_zero = operator == '<'
            
            if only_do_zero:
                
                continue
                
            elif include_zero:
                
                continue
                
            else:
                
                dupe_hash_ids = self.modules_files_duplicates.DuplicatesGetHashIdsFromDuplicateCountPredicate( db_location_context, operator, num_relationships, dupe_type )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, dupe_hash_ids )
                
                have_cross_referenced_file_locations = True
                
            
        
        for ( view_type, viewing_locations, operator, viewing_value ) in system_predicates.GetFileViewingStatsPredicates():
            
            only_do_zero = ( operator in ( '=', CC.UNICODE_ALMOST_EQUAL_TO ) and viewing_value == 0 ) or ( operator == '<' and viewing_value == 1 )
            include_zero = operator == '<'
            
            if only_do_zero:
                
                continue
                
            elif include_zero:
                
                continue
                
            else:
                
                viewing_hash_ids = self._GetHashIdsFromFileViewingStatistics( view_type, viewing_locations, operator, viewing_value )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, viewing_hash_ids )
                
            
        
        # first tags
        
        if there_are_tags_to_search:
            
            def sort_longest_first_key( s ):
                
                return ( 1 if HydrusTags.IsUnnamespaced( s ) else 0, -len( s ) )
                
            
            tags_to_include = list( tags_to_include )
            
            tags_to_include.sort( key = sort_longest_first_key )
            
            for tag in tags_to_include:
                
                if query_hash_ids is None:
                    
                    tag_query_hash_ids = self._GetHashIdsFromTag( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, tag, job_key = job_key )
                    
                elif is_inbox and len( query_hash_ids ) == len( self.modules_files_metadata_basic.inbox_hash_ids ):
                    
                    tag_query_hash_ids = self._GetHashIdsFromTag( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, tag, hash_ids = self.modules_files_metadata_basic.inbox_hash_ids, hash_ids_table_name = 'file_inbox', job_key = job_key )
                    
                else:
                    
                    with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                        
                        tag_query_hash_ids = self._GetHashIdsFromTag( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, tag, hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name, job_key = job_key )
                        
                    
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, tag_query_hash_ids )
                
                have_cross_referenced_file_locations = True
                
                if query_hash_ids == set():
                    
                    return query_hash_ids
                    
                
            
            for namespace in namespaces_to_include:
                
                if query_hash_ids is None or ( is_inbox and len( query_hash_ids ) == len( self.modules_files_metadata_basic.inbox_hash_ids ) ):
                    
                    namespace_query_hash_ids = self._GetHashIdsThatHaveTagsComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, namespace_wildcard = namespace, job_key = job_key )
                    
                else:
                    
                    with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                        
                        self._AnalyzeTempTable( temp_table_name )
                        
                        namespace_query_hash_ids = self._GetHashIdsThatHaveTagsComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, namespace_wildcard = namespace, hash_ids_table_name = temp_table_name, job_key = job_key )
                        
                    
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, namespace_query_hash_ids )
                
                have_cross_referenced_file_locations = True
                
                if query_hash_ids == set():
                    
                    return query_hash_ids
                    
                
            
            for wildcard in wildcards_to_include:
                
                if query_hash_ids is None:
                    
                    wildcard_query_hash_ids = self._GetHashIdsFromWildcardComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, wildcard, job_key = job_key )
                    
                else:
                    
                    with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                        
                        self._AnalyzeTempTable( temp_table_name )
                        
                        wildcard_query_hash_ids = self._GetHashIdsFromWildcardComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, wildcard, hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name, job_key = job_key )
                        
                    
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, wildcard_query_hash_ids )
                
                have_cross_referenced_file_locations = True
                
                if query_hash_ids == set():
                    
                    return query_hash_ids
                    
                
            
        
        #
        
        # OR round two--if file preds will not be fast, let's step in to reduce the file domain search space
        if not done_or_predicates and not there_are_simple_files_info_preds_to_search_for:
            
            query_hash_ids = do_or_preds( or_predicates, query_hash_ids )
            
            have_cross_referenced_file_locations = True
            
            done_or_predicates = True
            
        
        # now the simple preds and desperate last shot to populate query_hash_ids
        
        done_files_info_predicates = False
        
        we_need_some_results = query_hash_ids is None
        we_need_to_cross_reference = need_file_domain_cross_reference and not have_cross_referenced_file_locations
        
        if we_need_some_results or we_need_to_cross_reference:
            
            if location_context.IsAllKnownFiles():
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, self._GetHashIdsThatHaveTagsComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, job_key = job_key ) )
                
            else:
                
                files_table_name = db_location_context.files_table_name
                
                if len( files_info_predicates ) == 0:
                    
                    files_info_predicates.insert( 0, '1=1' )
                    
                else:
                    
                    # if a file is missing a files_info row, we can't search it with a file system pred. it is just unknown
                    files_table_name = '{} NATURAL JOIN files_info'.format( files_table_name )
                    
                
                if query_hash_ids is None:
                    
                    query_hash_ids = intersection_update_qhi( query_hash_ids, self._STS( self._Execute( 'SELECT hash_id FROM {} WHERE {};'.format( files_table_name, ' AND '.join( files_info_predicates ) ) ) ) )
                    
                else:
                    
                    if is_inbox and len( query_hash_ids ) == len( self.modules_files_metadata_basic.inbox_hash_ids ):
                        
                        query_hash_ids = intersection_update_qhi( query_hash_ids, self._STS( self._Execute( 'SELECT hash_id FROM {} NATURAL JOIN {} WHERE {};'.format( 'file_inbox', files_table_name, ' AND '.join( files_info_predicates ) ) ) ) )
                        
                    else:
                        
                        with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                            
                            self._AnalyzeTempTable( temp_table_name )
                            
                            query_hash_ids = intersection_update_qhi( query_hash_ids, self._STS( self._Execute( 'SELECT hash_id FROM {} NATURAL JOIN {} WHERE {};'.format( temp_table_name, files_table_name, ' AND '.join( files_info_predicates ) ) ) ) )
                            
                        
                    
                
                have_cross_referenced_file_locations = True
                done_files_info_predicates = True
                
            
        
        # at this point, query_hash_ids has something in it
        
        if 'has_icc_profile' in simple_preds:
            
            has_icc_profile = simple_preds[ 'has_icc_profile' ]
            
            has_icc_profile_has_ids = self.modules_files_metadata_basic.GetHasICCProfileHashIds( query_hash_ids )
            
            if has_icc_profile:
                
                query_hash_ids.intersection_update( has_icc_profile_has_ids )
                
            else:
                
                query_hash_ids.difference_update( has_icc_profile_has_ids )
                
            
        
        if system_predicates.MustBeArchive():
            
            query_hash_ids.difference_update( self.modules_files_metadata_basic.inbox_hash_ids )
            
        
        if king_filter is not None and king_filter:
            
            king_hash_ids = self.modules_files_duplicates.DuplicatesFilterKingHashIds( query_hash_ids )
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, king_hash_ids )
            
        
        if there_are_simple_files_info_preds_to_search_for and not done_files_info_predicates:
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                predicate_string = ' AND '.join( files_info_predicates )
                
                select = 'SELECT hash_id FROM {} NATURAL JOIN files_info WHERE {};'.format( temp_table_name, predicate_string )
                
                files_info_hash_ids = self._STI( self._Execute( select ) )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, files_info_hash_ids )
                
            
            done_files_info_predicates = True
            
        
        if job_key.IsCancelled():
            
            return set()
            
        
        #
        
        # OR round three--final chance to kick in, and the preferred one. query_hash_ids is now set, so this shouldn't be super slow for most scenarios
        if not done_or_predicates:
            
            query_hash_ids = do_or_preds( or_predicates, query_hash_ids )
            
            done_or_predicates = True
            
        
        # hide update files
        
        if location_context.IsAllLocalFiles():
            
            repo_update_hash_ids = set( self.modules_files_storage.GetCurrentHashIdsList( self.modules_services.local_update_service_id ) )
            
            query_hash_ids.difference_update( repo_update_hash_ids )
            
        
        # now subtract bad results
        
        for tag in tags_to_exclude:
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                unwanted_hash_ids = self._GetHashIdsFromTag( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, tag, hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name, job_key = job_key )
                
                query_hash_ids.difference_update( unwanted_hash_ids )
                
            
            if len( query_hash_ids ) == 0:
                
                return query_hash_ids
                
            
        
        for namespace in namespaces_to_exclude:
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                unwanted_hash_ids = self._GetHashIdsThatHaveTagsComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, namespace_wildcard = namespace, hash_ids_table_name = temp_table_name, job_key = job_key )
                
                query_hash_ids.difference_update( unwanted_hash_ids )
                
            
            if len( query_hash_ids ) == 0:
                
                return query_hash_ids
                
            
        
        for wildcard in wildcards_to_exclude:
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                unwanted_hash_ids = self._GetHashIdsFromWildcardComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, wildcard, hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name, job_key = job_key )
                
                query_hash_ids.difference_update( unwanted_hash_ids )
                
            
            if len( query_hash_ids ) == 0:
                
                return query_hash_ids
                
            
        
        if job_key.IsCancelled():
            
            return set()
            
        
        #
        
        ( required_file_service_statuses, excluded_file_service_statuses ) = system_predicates.GetFileServiceStatuses()
        
        for ( service_key, statuses ) in required_file_service_statuses.items():
            
            service_id = self.modules_services.GetServiceId( service_key )
            
            for status in statuses:
                
                required_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( service_id, query_hash_ids, status )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, required_hash_ids )
                
            
        
        for ( service_key, statuses ) in excluded_file_service_statuses.items():
            
            service_id = self.modules_services.GetServiceId( service_key )
            
            for status in statuses:
                
                excluded_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( service_id, query_hash_ids, status )
                
                query_hash_ids.difference_update( excluded_hash_ids )
                
            
        
        #
        
        for ( operator, value, service_key ) in system_predicates.GetRatingsPredicates():
            
            service_id = self.modules_services.GetServiceId( service_key )
            
            if value == 'not rated':
                
                query_hash_ids.difference_update( self._STI( self._Execute( 'SELECT hash_id FROM local_ratings WHERE service_id = ?;', ( service_id, ) ) ) )
                
            
        
        if king_filter is not None and not king_filter:
            
            king_hash_ids = self.modules_files_duplicates.DuplicatesFilterKingHashIds( query_hash_ids )
            
            query_hash_ids.difference_update( king_hash_ids )
            
        
        for ( operator, num_relationships, dupe_type ) in system_predicates.GetDuplicateRelationshipCountPredicates():
            
            only_do_zero = ( operator in ( '=', CC.UNICODE_ALMOST_EQUAL_TO ) and num_relationships == 0 ) or ( operator == '<' and num_relationships == 1 )
            include_zero = operator == '<'
            
            if only_do_zero:
                
                nonzero_hash_ids = self.modules_files_duplicates.DuplicatesGetHashIdsFromDuplicateCountPredicate( db_location_context, '>', 0, dupe_type )
                
                query_hash_ids.difference_update( nonzero_hash_ids )
                
            elif include_zero:
                
                nonzero_hash_ids = self.modules_files_duplicates.DuplicatesGetHashIdsFromDuplicateCountPredicate( db_location_context, '>', 0, dupe_type )
                
                zero_hash_ids = query_hash_ids.difference( nonzero_hash_ids )
                
                accurate_except_zero_hash_ids = self.modules_files_duplicates.DuplicatesGetHashIdsFromDuplicateCountPredicate( db_location_context, operator, num_relationships, dupe_type )
                
                hash_ids = zero_hash_ids.union( accurate_except_zero_hash_ids )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, hash_ids )
                
            
        
        min_num_notes = None
        max_num_notes = None
        
        if 'num_notes' in simple_preds:
            
            min_num_notes = simple_preds[ 'num_notes' ]
            max_num_notes = min_num_notes
            
        else:
            
            if 'min_num_notes' in simple_preds:
                
                min_num_notes = simple_preds[ 'min_num_notes' ] + 1
                
            if 'max_num_notes' in simple_preds:
                
                max_num_notes = simple_preds[ 'max_num_notes' ] - 1
                
            
        
        if min_num_notes is not None or max_num_notes is not None:
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                num_notes_hash_ids = self._GetHashIdsFromNumNotes( min_num_notes, max_num_notes, temp_table_name )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, num_notes_hash_ids )
                
            
        
        if 'has_note_names' in simple_preds:
            
            inclusive_note_names = simple_preds[ 'has_note_names' ]
            
            for note_name in inclusive_note_names:
                
                with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                    
                    self._AnalyzeTempTable( temp_table_name )
                    
                    notes_hash_ids = self._GetHashIdsFromNoteName( note_name, temp_table_name )
                    
                    query_hash_ids = intersection_update_qhi( query_hash_ids, notes_hash_ids )
                    
                
            
        
        if 'not_has_note_names' in simple_preds:
            
            exclusive_note_names = simple_preds[ 'not_has_note_names' ]
            
            for note_name in exclusive_note_names:
                
                with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                    
                    self._AnalyzeTempTable( temp_table_name )
                    
                    notes_hash_ids = self._GetHashIdsFromNoteName( note_name, temp_table_name )
                    
                    query_hash_ids.difference_update( notes_hash_ids )
                    
                
            
        
        for ( view_type, viewing_locations, operator, viewing_value ) in system_predicates.GetFileViewingStatsPredicates():
            
            only_do_zero = ( operator in ( '=', CC.UNICODE_ALMOST_EQUAL_TO ) and viewing_value == 0 ) or ( operator == '<' and viewing_value == 1 )
            include_zero = operator == '<'
            
            if only_do_zero:
                
                nonzero_hash_ids = self._GetHashIdsFromFileViewingStatistics( view_type, viewing_locations, '>', 0 )
                
                query_hash_ids.difference_update( nonzero_hash_ids )
                
            elif include_zero:
                
                nonzero_hash_ids = self._GetHashIdsFromFileViewingStatistics( view_type, viewing_locations, '>', 0 )
                
                zero_hash_ids = query_hash_ids.difference( nonzero_hash_ids )
                
                accurate_except_zero_hash_ids = self._GetHashIdsFromFileViewingStatistics( view_type, viewing_locations, operator, viewing_value )
                
                hash_ids = zero_hash_ids.union( accurate_except_zero_hash_ids )
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, hash_ids )
                
            
        
        if job_key.IsCancelled():
            
            return set()
            
        
        #
        
        file_location_is_all_local = self.modules_services.LocationContextIsCoveredByCombinedLocalFiles( location_context )
        file_location_is_all_combined_local_files_deleted = location_context.IsOneDomain() and CC.COMBINED_LOCAL_FILE_SERVICE_KEY in location_context.deleted_service_keys
        
        must_be_local = system_predicates.MustBeLocal() or system_predicates.MustBeArchive()
        must_not_be_local = system_predicates.MustNotBeLocal()
        
        if file_location_is_all_local:
            
            # if must be all local, we are great already
            
            if must_not_be_local:
                
                query_hash_ids = set()
                
            
        elif file_location_is_all_combined_local_files_deleted:
            
            if must_be_local:
                
                query_hash_ids = set()
                
            
        elif must_be_local or must_not_be_local:
            
            if must_be_local:
                
                query_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( self.modules_services.combined_local_file_service_id, query_hash_ids, HC.CONTENT_STATUS_CURRENT )
                
            elif must_not_be_local:
                
                local_hash_ids = self.modules_files_storage.GetCurrentHashIdsList( self.modules_services.combined_local_file_service_id )
                
                query_hash_ids.difference_update( local_hash_ids )
                
            
        
        #
        
        if 'known_url_rules' in simple_preds:
            
            for ( operator, rule_type, rule ) in simple_preds[ 'known_url_rules' ]:
                
                if rule_type == 'exact_match' or ( is_inbox and len( query_hash_ids ) == len( self.modules_files_metadata_basic.inbox_hash_ids ) ):
                    
                    url_hash_ids = self._GetHashIdsFromURLRule( rule_type, rule )
                    
                else:
                    
                    with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                        
                        self._AnalyzeTempTable( temp_table_name )
                        
                        url_hash_ids = self._GetHashIdsFromURLRule( rule_type, rule, hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name )
                        
                    
                
                if operator: # inclusive
                    
                    query_hash_ids = intersection_update_qhi( query_hash_ids, url_hash_ids )
                    
                else:
                    
                    query_hash_ids.difference_update( url_hash_ids )
                    
                
            
        
        #
        
        namespaces_to_tests = system_predicates.GetNumTagsNumberTests()
        
        for ( namespace, number_tests ) in namespaces_to_tests.items():
            
            is_zero = True in ( number_test.IsZero() for number_test in number_tests )
            is_anything_but_zero = True in ( number_test.IsAnythingButZero() for number_test in number_tests )
            
            specific_number_tests = [ number_test for number_test in number_tests if not ( number_test.IsZero() or number_test.IsAnythingButZero() ) ]
            
            lambdas = [ number_test.GetLambda() for number_test in specific_number_tests ]
            
            megalambda = lambda x: False not in ( l( x ) for l in lambdas )
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                nonzero_tag_query_hash_ids = set()
                nonzero_tag_query_hash_ids_populated = False
                
                if is_zero or is_anything_but_zero:
                    
                    nonzero_tag_query_hash_ids = self._GetHashIdsThatHaveTagsComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, hash_ids_table_name = temp_table_name, namespace_wildcard = namespace, job_key = job_key )
                    nonzero_tag_query_hash_ids_populated = True
                    
                    if is_zero:
                        
                        query_hash_ids.difference_update( nonzero_tag_query_hash_ids )
                        
                    
                    if is_anything_but_zero:
                        
                        query_hash_ids = intersection_update_qhi( query_hash_ids, nonzero_tag_query_hash_ids )
                        
                    
                
            
            if len( specific_number_tests ) > 0:
                
                hash_id_tag_counts = self._GetHashIdsAndNonZeroTagCounts( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, query_hash_ids, namespace_wildcard = namespace, job_key = job_key )
                
                good_tag_count_hash_ids = { hash_id for ( hash_id, count ) in hash_id_tag_counts if megalambda( count ) }
                
                if megalambda( 0 ): # files with zero count are needed
                    
                    if not nonzero_tag_query_hash_ids_populated:
                        
                        nonzero_tag_query_hash_ids = { hash_id for ( hash_id, count ) in hash_id_tag_counts }
                        
                    
                    zero_hash_ids = query_hash_ids.difference( nonzero_tag_query_hash_ids )
                    
                    good_tag_count_hash_ids.update( zero_hash_ids )
                    
                
                query_hash_ids = intersection_update_qhi( query_hash_ids, good_tag_count_hash_ids )
                
            
            
        
        if job_key.IsCancelled():
            
            return set()
            
        
        #
        
        if 'min_tag_as_number' in simple_preds:
            
            ( namespace, num ) = simple_preds[ 'min_tag_as_number' ]
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                good_hash_ids = self._GetHashIdsThatHaveTagAsNumComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, namespace, num, '>', hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name, job_key = job_key )
                
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, good_hash_ids )
            
        
        if 'max_tag_as_number' in simple_preds:
            
            ( namespace, num ) = simple_preds[ 'max_tag_as_number' ]
            
            with self._MakeTemporaryIntegerTable( query_hash_ids, 'hash_id' ) as temp_table_name:
                
                self._AnalyzeTempTable( temp_table_name )
                
                good_hash_ids = self._GetHashIdsThatHaveTagAsNumComplexLocation( ClientTags.TAG_DISPLAY_ACTUAL, location_context, tag_search_context, namespace, num, '<', hash_ids = query_hash_ids, hash_ids_table_name = temp_table_name, job_key = job_key )
                
            
            query_hash_ids = intersection_update_qhi( query_hash_ids, good_hash_ids )
            
        
        if job_key.IsCancelled():
            
            return set()
            
        
        #
        
        query_hash_ids = list( query_hash_ids )
        
        #
        
        limit = system_predicates.GetLimit( apply_implicit_limit = apply_implicit_limit )
        
        we_are_applying_limit = limit is not None and limit < len( query_hash_ids )
        
        if we_are_applying_limit and limit_sort_by is not None and sort_by is None:
            
            sort_by = limit_sort_by
            
        
        did_sort = False
        
        if sort_by is not None and not location_context.IsAllKnownFiles():
            
            ( did_sort, query_hash_ids ) = self._TryToSortHashIds( location_context, query_hash_ids, sort_by )
            
        
        #
        
        if we_are_applying_limit:
            
            if not did_sort:
                
                query_hash_ids = random.sample( query_hash_ids, limit )
                
            else:
                
                query_hash_ids = query_hash_ids[:limit]
                
            
        
        return query_hash_ids
        
    
    def _GetHashIdsFromSubtagIds( self, tag_display_type: int, file_service_key, tag_search_context: ClientSearch.TagSearchContext, subtag_ids, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
        
        tag_ids = self._GetTagIdsFromSubtagIds( file_service_id, tag_service_id, subtag_ids, job_key = job_key )
        
        return self._GetHashIdsFromTagIds( tag_display_type, file_service_key, tag_search_context, tag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
        
    
    def _GetHashIdsFromSubtagIdsTable( self, tag_display_type: int, file_service_key, tag_search_context: ClientSearch.TagSearchContext, subtag_ids_table_name, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
        
        tag_ids = self._GetTagIdsFromSubtagIdsTable( file_service_id, tag_service_id, subtag_ids_table_name, job_key = job_key )
        
        return self._GetHashIdsFromTagIds( tag_display_type, file_service_key, tag_search_context, tag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
        
    
    def _GetHashIdsFromTag( self, tag_display_type: int, location_context: ClientLocation.LocationContext, tag_search_context: ClientSearch.TagSearchContext, tag, hash_ids = None, hash_ids_table_name = None, allow_unnamespaced_to_fetch_namespaced = True, job_key = None ):
        
        ( file_service_keys, file_location_is_cross_referenced ) = location_context.GetCoveringCurrentFileServiceKeys()
        
        if not file_location_is_cross_referenced and hash_ids_table_name is not None:
            
            file_location_is_cross_referenced = True
            
        
        ( namespace, subtag ) = HydrusTags.SplitTag( tag )
        
        subtag_id = self.modules_tags.GetSubtagId( subtag )
        
        if not self.modules_tags.SubtagExists( subtag ):
            
            return set()
            
        
        tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
        
        results = set()
        
        for file_service_key in file_service_keys:
            
            if namespace == '' and allow_unnamespaced_to_fetch_namespaced:
                
                file_service_id = self.modules_services.GetServiceId( file_service_key )
                
                tag_ids = self._GetTagIdsFromSubtagIds( file_service_id, tag_service_id, ( subtag_id, ) )
                
            else:
                
                if not self.modules_tags.TagExists( tag ):
                    
                    return set()
                    
                
                tag_id = self.modules_tags.GetTagId( tag )
                
                tag_ids = ( tag_id, )
                
            
            some_results = self._GetHashIdsFromTagIds( tag_display_type, file_service_key, tag_search_context, tag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
            
            if len( results ) == 0:
                
                results = some_results
                
            else:
                
                results.update( some_results )
                
            
        
        if not file_location_is_cross_referenced:
            
            results = self.modules_files_storage.FilterHashIds( location_context, results )
            
        
        return results
        
    
    def _GetHashIdsFromTagIds( self, tag_display_type: int, file_service_key: bytes, tag_search_context: ClientSearch.TagSearchContext, tag_ids: typing.Collection[ int ], hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        do_hash_table_join = False
        
        if hash_ids_table_name is not None and hash_ids is not None:
            
            tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
            file_service_id = self.modules_services.GetServiceId( file_service_key )
            
            estimated_count = self._GetAutocompleteCountEstimate( tag_display_type, tag_service_id, file_service_id, tag_ids, tag_search_context.include_current_tags, tag_search_context.include_pending_tags )
            
            # experimentally, file lookups are about 2.5x as slow as tag lookups
            
            if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( len( hash_ids ), estimated_count ):
                
                do_hash_table_join = True
                
            
        
        result_hash_ids = set()
        
        table_names = self._GetMappingTables( tag_display_type, file_service_key, tag_search_context )
        
        cancelled_hook = None
        
        if job_key is not None:
            
            cancelled_hook = job_key.IsCancelled
            
        
        if len( tag_ids ) == 1:
            
            ( tag_id, ) = tag_ids
            
            if do_hash_table_join:
                
                # temp hashes to mappings
                queries = [ 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = ?'.format( hash_ids_table_name, table_name ) for table_name in table_names ]
                
            else:
                
                queries = [ 'SELECT hash_id FROM {} WHERE tag_id = ?;'.format( table_name ) for table_name in table_names ]
                
            
            for query in queries:
                
                cursor = self._Execute( query, ( tag_id, ) )
                
                result_hash_ids.update( self._STI( HydrusDB.ReadFromCancellableCursor( cursor, 1024, cancelled_hook ) ) )
                
            
        else:
            
            with self._MakeTemporaryIntegerTable( tag_ids, 'tag_id' ) as temp_tag_ids_table_name:
                
                if do_hash_table_join:
                    
                    # temp hashes to mappings to temp tags
                    # old method, does not do EXISTS efficiently, it makes a list instead and checks that
                    # queries = [ 'SELECT hash_id FROM {} WHERE EXISTS ( SELECT 1 FROM {} CROSS JOIN {} USING ( tag_id ) WHERE {}.hash_id = {}.hash_id );'.format( hash_ids_table_name, table_name, temp_tag_ids_table_name, table_name, hash_ids_table_name ) for table_name in table_names ]
                    # new method, this seems to actually do the correlated scalar subquery, although it does seem to be sqlite voodoo
                    queries = [ 'SELECT hash_id FROM {} WHERE EXISTS ( SELECT 1 FROM {} WHERE {}.hash_id = {}.hash_id AND EXISTS ( SELECT 1 FROM {} WHERE {}.tag_id = {}.tag_id ) );'.format( hash_ids_table_name, table_name, table_name, hash_ids_table_name, temp_tag_ids_table_name, table_name, temp_tag_ids_table_name ) for table_name in table_names ]
                    
                else:
                    
                    # temp tags to mappings
                    queries = [ 'SELECT hash_id FROM {} CROSS JOIN {} USING ( tag_id );'.format( temp_tag_ids_table_name, table_name ) for table_name in table_names ]
                    
                
                for query in queries:
                    
                    cursor = self._Execute( query )
                    
                    result_hash_ids.update( self._STI( HydrusDB.ReadFromCancellableCursor( cursor, 1024, cancelled_hook ) ) )
                    
                
            
        
        return result_hash_ids
        
    
    def _GetHashIdsFromURLRule( self, rule_type, rule, hash_ids = None, hash_ids_table_name = None ):
        
        if rule_type == 'exact_match':
            
            url = rule
            
            table_name = 'url_map NATURAL JOIN urls'
            
            if hash_ids_table_name is not None and hash_ids is not None and len( hash_ids ) < 50000:
                
                table_name += ' NATURAL JOIN {}'.format( hash_ids_table_name )
                
            
            select = 'SELECT hash_id FROM {} WHERE url = ?;'.format( table_name )
            
            result_hash_ids = self._STS( self._Execute( select, ( url, ) ) )
            
            return result_hash_ids
            
        elif rule_type in ( 'url_class', 'url_match' ):
            
            url_class = rule
            
            domain = url_class.GetDomain()
            
            if url_class.MatchesSubdomains():
                
                domain_ids = self.modules_urls.GetURLDomainAndSubdomainIds( domain )
                
            else:
                
                domain_ids = self.modules_urls.GetURLDomainAndSubdomainIds( domain, only_www_subdomains = True )
                
            
            result_hash_ids = set()
            
            with self._MakeTemporaryIntegerTable( domain_ids, 'domain_id' ) as temp_domain_table_name:
                
                if hash_ids_table_name is not None and hash_ids is not None and len( hash_ids ) < 50000:
                    
                    # if we aren't gonk mode with the number of files, temp hashes to url map to urls to domains
                    # next step here is irl profiling and a domain->url_count cache so I can decide whether to do this or not based on url domain count
                    select = 'SELECT hash_id, url FROM {} CROSS JOIN url_map USING ( hash_id ) CROSS JOIN urls USING ( url_id ) CROSS JOIN {} USING ( domain_id );'.format( hash_ids_table_name, temp_domain_table_name )
                    
                else:
                    
                    # domains to urls to url map
                    select = 'SELECT hash_id, url FROM {} CROSS JOIN urls USING ( domain_id ) CROSS JOIN url_map USING ( url_id );'.format( temp_domain_table_name )
                    
                
                for ( hash_id, url ) in self._Execute( select ):
                    
                    # this is actually insufficient, as more detailed url classes may match
                    if hash_id not in result_hash_ids and url_class.Matches( url ):
                        
                        result_hash_ids.add( hash_id )
                        
                    
                
            
            return result_hash_ids
            
        elif rule_type in 'domain':
            
            domain = rule
            
            # if we search for site.com, we also want artist.site.com or www.site.com or cdn2.site.com
            domain_ids = self.modules_urls.GetURLDomainAndSubdomainIds( domain )
            
            result_hash_ids = set()
            
            with self._MakeTemporaryIntegerTable( domain_ids, 'domain_id' ) as temp_domain_table_name:
                
                if hash_ids_table_name is not None and hash_ids is not None and len( hash_ids ) < 50000:
                    
                    # if we aren't gonk mode with the number of files, temp hashes to url map to urls to domains
                    # next step here is irl profiling and a domain->url_count cache so I can decide whether to do this or not based on url domain count
                    select = 'SELECT hash_id FROM {} CROSS JOIN url_map USING ( hash_id ) CROSS JOIN urls USING ( url_id ) CROSS JOIN {} USING ( domain_id )'.format( hash_ids_table_name, temp_domain_table_name )
                    
                else:
                    
                    # domains to urls to url map
                    select = 'SELECT hash_id FROM {} CROSS JOIN urls USING ( domain_id ) CROSS JOIN url_map USING ( url_id );'.format( temp_domain_table_name )
                    
                
                result_hash_ids = self._STS( self._Execute( select ) )
                
            
            return result_hash_ids
            
        else:
            
            regex = rule
            
            if hash_ids_table_name is not None and hash_ids is not None and len( hash_ids ) < 50000:
                
                # if we aren't gonk mode with the number of files, temp hashes to url map to urls
                # next step here is irl profiling and a domain->url_count cache so I can decide whether to do this or not based on _TOTAL_ url count
                select = 'SELECT hash_id, url FROM {} CROSS JOIN url_map USING ( hash_id ) CROSS JOIN urls USING ( url_id );'.format( hash_ids_table_name )
                
            else:
                
                select = 'SELECT hash_id, url FROM url_map NATURAL JOIN urls;'
                
            
            result_hash_ids = set()
            
            for ( hash_id, url ) in self._Execute( select ):
                
                if hash_id not in result_hash_ids and re.search( regex, url ) is not None:
                    
                    result_hash_ids.add( hash_id )
                    
                
            
            return result_hash_ids
            
        
    
    def _GetHashIdsFromWildcardComplexLocation( self, tag_display_type: int, location_context: ClientLocation.LocationContext, tag_search_context: ClientSearch.TagSearchContext, wildcard, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        ( namespace_wildcard, subtag_wildcard ) = HydrusTags.SplitTag( wildcard )
        
        if namespace_wildcard in ( '*', '' ):
            
            namespace_wildcard = None
            
        
        if subtag_wildcard == '*':
            
            return self._GetHashIdsThatHaveTagsComplexLocation( tag_display_type, location_context, tag_search_context, namespace_wildcard = namespace_wildcard, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
            
        
        results = set()
        
        ( file_service_keys, file_location_is_cross_referenced ) = location_context.GetCoveringCurrentFileServiceKeys()
        
        if not file_location_is_cross_referenced and hash_ids_table_name is not None:
            
            file_location_is_cross_referenced = True
            
        
        if namespace_wildcard is None:
            
            possible_namespace_ids = []
            
        else:
            
            possible_namespace_ids = self.modules_tag_search.GetNamespaceIdsFromWildcard( namespace_wildcard )
            
            if len( possible_namespace_ids ) == 0:
                
                return set()
                
            
        
        with self._MakeTemporaryIntegerTable( possible_namespace_ids, 'namespace_id' ) as temp_namespace_ids_table_name:
            
            if namespace_wildcard is None:
                
                namespace_ids_table_name = None
                
            else:
                
                namespace_ids_table_name = temp_namespace_ids_table_name
                
            
            for file_service_key in file_service_keys:
                
                some_results = self._GetHashIdsFromWildcardSimpleLocation( tag_display_type, file_service_key, tag_search_context, subtag_wildcard, namespace_ids_table_name = namespace_ids_table_name, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
                
                if len( results ) == 0:
                    
                    results = some_results
                    
                else:
                    
                    results.update( some_results )
                    
                
            
        
        if not file_location_is_cross_referenced:
            
            results = self.modules_files_storage.FilterHashIds( location_context, results )
            
        
        return results
        
    
    def _GetHashIdsFromWildcardSimpleLocation( self, tag_display_type: int, file_service_key: bytes, tag_search_context: ClientSearch.TagSearchContext, subtag_wildcard, namespace_ids_table_name = None, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        with self._MakeTemporaryIntegerTable( [], 'subtag_id' ) as temp_subtag_ids_table_name:
            
            file_service_id = self.modules_services.GetServiceId( file_service_key )
            tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
            
            self.modules_tag_search.GetSubtagIdsFromWildcardIntoTable( file_service_id, tag_service_id, subtag_wildcard, temp_subtag_ids_table_name, job_key = job_key )
            
            if namespace_ids_table_name is None:
                
                return self._GetHashIdsFromSubtagIdsTable( tag_display_type, file_service_key, tag_search_context, temp_subtag_ids_table_name, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
                
            else:
                
                return self._GetHashIdsFromNamespaceIdsSubtagIdsTables( tag_display_type, file_service_key, tag_search_context, namespace_ids_table_name, temp_subtag_ids_table_name, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
                
            
        
    
    def _GetHashIdsThatHaveTagsComplexLocation( self, tag_display_type: int, location_context: ClientLocation.LocationContext, tag_search_context: ClientSearch.TagSearchContext, namespace_wildcard = None, hash_ids_table_name = None, job_key = None ):
        
        if not location_context.SearchesAnything():
            
            return set()
            
        
        if namespace_wildcard == '*':
            
            namespace_wildcard = None
            
        
        if namespace_wildcard is None:
            
            possible_namespace_ids = []
            
        else:
            
            possible_namespace_ids = self.modules_tag_search.GetNamespaceIdsFromWildcard( namespace_wildcard )
            
            if len( possible_namespace_ids ) == 0:
                
                return set()
                
            
        
        results = set()
        
        with self._MakeTemporaryIntegerTable( possible_namespace_ids, 'namespace_id' ) as temp_namespace_ids_table_name:
            
            if namespace_wildcard is None:
                
                namespace_ids_table_name = None
                
            else:
                
                namespace_ids_table_name = temp_namespace_ids_table_name
                
            
            ( file_service_keys, file_location_is_cross_referenced ) = location_context.GetCoveringCurrentFileServiceKeys()
            
            if not file_location_is_cross_referenced and hash_ids_table_name is not None:
                
                file_location_is_cross_referenced = True
                
            
            for file_service_key in file_service_keys:
                
                some_results = self._GetHashIdsThatHaveTagsSimpleLocation( tag_display_type, file_service_key, tag_search_context, namespace_ids_table_name = namespace_ids_table_name, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
                
                if len( results ) == 0:
                    
                    results = some_results
                    
                else:
                    
                    results.update( some_results )
                    
                
            
        
        if not file_location_is_cross_referenced:
            
            results = self.modules_files_storage.FilterHashIds( location_context, results )
            
        
        return results
        
    
    def _GetHashIdsThatHaveTagsSimpleLocation( self, tag_display_type: int, file_service_key: bytes, tag_search_context: ClientSearch.TagSearchContext, namespace_ids_table_name = None, hash_ids_table_name = None, job_key = None ):
        
        mapping_and_tag_table_names = self._GetMappingAndTagTables( tag_display_type, file_service_key, tag_search_context )
        
        if hash_ids_table_name is None:
            
            if namespace_ids_table_name is None:
                
                # hellmode
                queries = [ 'SELECT DISTINCT hash_id FROM {};'.format( mappings_table_name ) for ( mappings_table_name, tags_table_name ) in mapping_and_tag_table_names ]
                
            else:
                
                # temp namespaces to tags to mappings
                queries = [ 'SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( namespace_id ) CROSS JOIN {} USING ( tag_id );'.format( namespace_ids_table_name, tags_table_name, mappings_table_name ) for ( mappings_table_name, tags_table_name ) in mapping_and_tag_table_names ]
                
            
        else:
            
            if namespace_ids_table_name is None:
                
                queries = [ 'SELECT hash_id FROM {} WHERE EXISTS ( SELECT 1 FROM {} WHERE {}.hash_id = {}.hash_id );'.format( hash_ids_table_name, mappings_table_name, mappings_table_name, hash_ids_table_name ) for ( mappings_table_name, tags_table_name ) in mapping_and_tag_table_names ]
                
            else:
                
                # temp hashes to mappings to tags to temp namespaces
                # this was originally a 'WHERE EXISTS' thing, but doing that on a three way cross join is too complex for that to work well
                # let's hope DISTINCT can save time too
                queries = [ 'SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) CROSS JOIN {} USING ( tag_id ) CROSS JOIN {} USING ( namespace_id );'.format( hash_ids_table_name, mappings_table_name, tags_table_name, namespace_ids_table_name ) for ( mappings_table_name, tags_table_name ) in mapping_and_tag_table_names ]
                
            
        
        cancelled_hook = None
        
        if job_key is not None:
            
            cancelled_hook = job_key.IsCancelled
            
        
        nonzero_tag_hash_ids = set()
        
        for query in queries:
            
            cursor = self._Execute( query )
            
            nonzero_tag_hash_ids.update( self._STI( HydrusDB.ReadFromCancellableCursor( cursor, 10240, cancelled_hook ) ) )
            
            if job_key is not None and job_key.IsCancelled():
                
                return set()
                
            
        
        return nonzero_tag_hash_ids
        
    
    def _GetHashIdsThatHaveTagAsNumComplexLocation( self, tag_display_type: int, location_context: ClientLocation.LocationContext, tag_search_context: ClientSearch.TagSearchContext, namespace, num, operator, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        if not location_context.SearchesAnything():
            
            return set()
            
        
        ( file_service_keys, file_location_is_cross_referenced ) = location_context.GetCoveringCurrentFileServiceKeys()
        
        if not file_location_is_cross_referenced and hash_ids_table_name is not None:
            
            file_location_is_cross_referenced = True
            
        
        results = set()
        
        for file_service_key in file_service_keys:
            
            some_results = self._GetHashIdsThatHaveTagAsNumSimpleLocation( tag_display_type, file_service_key, tag_search_context, namespace, num, operator, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
            
            if len( results ) == 0:
                
                results = some_results
                
            else:
                
                results.update( some_results )
                
            
        
        if not file_location_is_cross_referenced:
            
            results = self.modules_files_storage.FilterHashIds( location_context, results )
            
        
        return results
        
    
    def _GetHashIdsThatHaveTagAsNumSimpleLocation( self, tag_display_type: int, file_service_key: bytes, tag_search_context: ClientSearch.TagSearchContext, namespace, num, operator, hash_ids = None, hash_ids_table_name = None, job_key = None ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_id = self.modules_services.GetServiceId( tag_search_context.service_key )
        
        if tag_service_id == self.modules_services.combined_tag_service_id:
            
            search_tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            search_tag_service_ids = ( tag_service_id, )
            
        
        possible_subtag_ids = set()
        
        for search_tag_service_id in search_tag_service_ids:
            
            some_possible_subtag_ids = self.modules_tag_search.GetTagAsNumSubtagIds( file_service_id, search_tag_service_id, operator, num )
            
            possible_subtag_ids.update( some_possible_subtag_ids )
            
        
        if namespace == '':
            
            return self._GetHashIdsFromSubtagIds( tag_display_type, file_service_key, tag_search_context, possible_subtag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
            
        else:
            
            namespace_id = self.modules_tags.GetNamespaceId( namespace )
            
            possible_namespace_ids = { namespace_id }
            
            return self._GetHashIdsFromNamespaceIdsSubtagIds( tag_display_type, file_service_key, tag_search_context, possible_namespace_ids, possible_subtag_ids, hash_ids = hash_ids, hash_ids_table_name = hash_ids_table_name, job_key = job_key )
            
        
    
    def _GetHashIdStatus( self, hash_id, prefix = '' ) -> ClientImportFiles.FileImportStatus:
        
        if prefix != '':
            
            prefix += ': '
            
        
        hash = self.modules_hashes_local_cache.GetHash( hash_id )
        
        ( is_deleted, timestamp, file_deletion_reason ) = self.modules_files_storage.GetDeletionStatus( self.modules_services.combined_local_file_service_id, hash_id )
        
        if is_deleted:
            
            if timestamp is None:
                
                note = 'Deleted from the client before delete times were tracked ({}).'.format( file_deletion_reason )
                
            else:
                
                note = 'Deleted from the client {} ({}), which was {} before this check.'.format( HydrusData.ConvertTimestampToPrettyTime( timestamp ), file_deletion_reason, HydrusData.BaseTimestampToPrettyTimeDelta( timestamp ) )
                
            
            return ClientImportFiles.FileImportStatus( CC.STATUS_DELETED, hash, note = prefix + note )
            
        
        result = self.modules_files_storage.GetCurrentTimestamp( self.modules_services.trash_service_id, hash_id )
        
        if result is not None:
            
            timestamp = result
            
            note = 'Currently in trash ({}). Sent there at {}, which was {} before this check.'.format( file_deletion_reason, HydrusData.ConvertTimestampToPrettyTime( timestamp ), HydrusData.BaseTimestampToPrettyTimeDelta( timestamp, just_now_threshold = 0 ) )
            
            return ClientImportFiles.FileImportStatus( CC.STATUS_DELETED, hash, note = prefix + note )
            
        
        result = self.modules_files_storage.GetCurrentTimestamp( self.modules_services.combined_local_file_service_id, hash_id )
        
        if result is not None:
            
            timestamp = result
            
            mime = self.modules_files_metadata_basic.GetMime( hash_id )
            
            note = 'Imported at {}, which was {} before this check.'.format( HydrusData.ConvertTimestampToPrettyTime( timestamp ), HydrusData.BaseTimestampToPrettyTimeDelta( timestamp, just_now_threshold = 0 ) )
            
            return ClientImportFiles.FileImportStatus( CC.STATUS_SUCCESSFUL_BUT_REDUNDANT, hash, mime = mime, note = prefix + note )
            
        
        return ClientImportFiles.FileImportStatus( CC.STATUS_UNKNOWN, hash )
        
    
    def _GetHashStatus( self, hash_type, hash, prefix = None ) -> ClientImportFiles.FileImportStatus:
        
        if prefix is None:
            
            prefix = hash_type + ' recognised'
            
        
        if hash_type == 'sha256':
            
            if not self.modules_hashes.HasHash( hash ):
                
                f = ClientImportFiles.FileImportStatus.STATICGetUnknownStatus()
                
                f.hash = hash
                
                return f
                
            else:
                
                hash_id = self.modules_hashes_local_cache.GetHashId( hash )
                
            
        else:
            
            try:
                
                hash_id = self.modules_hashes.GetHashIdFromExtraHash( hash_type, hash )
                
            except HydrusExceptions.DataMissing:
                
                return ClientImportFiles.FileImportStatus.STATICGetUnknownStatus()
                
            
        
        return self._GetHashIdStatus( hash_id, prefix = prefix )
        
    
    def _GetIdealClientFilesLocations( self ):
        
        locations_to_ideal_weights = {}
        
        for ( portable_location, weight ) in self._Execute( 'SELECT location, weight FROM ideal_client_files_locations;' ):
            
            abs_location = HydrusPaths.ConvertPortablePathToAbsPath( portable_location )
            
            locations_to_ideal_weights[ abs_location ] = weight
            
        
        result = self._Execute( 'SELECT location FROM ideal_thumbnail_override_location;' ).fetchone()
        
        if result is None:
            
            abs_ideal_thumbnail_override_location = None
            
        else:
            
            ( portable_ideal_thumbnail_override_location, ) = result
            
            abs_ideal_thumbnail_override_location = HydrusPaths.ConvertPortablePathToAbsPath( portable_ideal_thumbnail_override_location )
            
        
        return ( locations_to_ideal_weights, abs_ideal_thumbnail_override_location )
        
    
    def _GetMaintenanceDue( self, stop_time ):
        
        jobs_to_do = []
        
        # analyze
        
        names_to_analyze = self.modules_db_maintenance.GetTableNamesDueAnalysis()
        
        if len( names_to_analyze ) > 0:
            
            jobs_to_do.append( 'analyze ' + HydrusData.ToHumanInt( len( names_to_analyze ) ) + ' table_names' )
            
        
        similar_files_due = self.modules_similar_files.MaintenanceDue()
        
        if similar_files_due:
            
            jobs_to_do.append( 'similar files work' )
            
        
        return jobs_to_do
        
    
    def _GetMappingTables( self, tag_display_type, file_service_key: bytes, tag_search_context: ClientSearch.TagSearchContext ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_key = tag_search_context.service_key
        
        if tag_service_key == CC.COMBINED_TAG_SERVICE_KEY:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            tag_service_ids = [ self.modules_services.GetServiceId( tag_service_key ) ]
            
        
        current_tables = []
        pending_tables = []
        
        for tag_service_id in tag_service_ids:
            
            if file_service_id == self.modules_services.combined_file_service_id:
                
                ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
                
                current_tables.append( current_mappings_table_name )
                pending_tables.append( pending_mappings_table_name )
                
            else:
                
                if tag_display_type == ClientTags.TAG_DISPLAY_STORAGE:
                    
                    ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
                    
                    current_tables.append( cache_current_mappings_table_name )
                    pending_tables.append( cache_pending_mappings_table_name )
                    
                elif tag_display_type == ClientTags.TAG_DISPLAY_ACTUAL:
                    
                    ( cache_current_display_mappings_table_name, cache_pending_display_mappings_table_name ) = ClientDBMappingsCacheSpecificDisplay.GenerateSpecificDisplayMappingsCacheTableNames( file_service_id, tag_service_id )
                    
                    current_tables.append( cache_current_display_mappings_table_name )
                    pending_tables.append( cache_pending_display_mappings_table_name )
                    
                
            
        
        table_names = []
        
        if tag_search_context.include_current_tags:
            
            table_names.extend( current_tables )
            
        
        if tag_search_context.include_pending_tags:
            
            table_names.extend( pending_tables )
            
        
        return table_names
        
    
    def _GetMappingAndTagTables( self, tag_display_type, file_service_key: bytes, tag_search_context: ClientSearch.TagSearchContext ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_key = tag_search_context.service_key
        
        if tag_service_key == CC.COMBINED_TAG_SERVICE_KEY:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            tag_service_ids = [ self.modules_services.GetServiceId( tag_service_key ) ]
            
        
        current_tables = []
        pending_tables = []
        
        for tag_service_id in tag_service_ids:
            
            tags_table_name = self.modules_tag_search.GetTagsTableName( file_service_id, tag_service_id )
            
            if file_service_id == self.modules_services.combined_file_service_id:
                
                # yo this does not support tag_display_actual--big tricky problem
                
                ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
                
                current_tables.append( ( current_mappings_table_name, tags_table_name ) )
                pending_tables.append( ( pending_mappings_table_name, tags_table_name ) )
                
            else:
                
                if tag_display_type == ClientTags.TAG_DISPLAY_STORAGE:
                    
                    ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id )
                    
                    current_tables.append( ( cache_current_mappings_table_name, tags_table_name ) )
                    pending_tables.append( ( cache_pending_mappings_table_name, tags_table_name ) )
                    
                elif tag_display_type == ClientTags.TAG_DISPLAY_ACTUAL:
                    
                    ( cache_current_display_mappings_table_name, cache_pending_display_mappings_table_name ) = ClientDBMappingsCacheSpecificDisplay.GenerateSpecificDisplayMappingsCacheTableNames( file_service_id, tag_service_id )
                    
                    current_tables.append( ( cache_current_display_mappings_table_name, tags_table_name ) )
                    pending_tables.append( ( cache_pending_display_mappings_table_name, tags_table_name ) )
                    
                
            
        
        table_names = []
        
        if tag_search_context.include_current_tags:
            
            table_names.extend( current_tables )
            
        
        if tag_search_context.include_pending_tags:
            
            table_names.extend( pending_tables )
            
        
        return table_names
        
    
    def _GetMediaPredicates( self, tag_search_context: ClientSearch.TagSearchContext, tags_to_counts, inclusive, job_key = None ):
        
        display_tag_service_id = self.modules_services.GetServiceId( tag_search_context.display_service_key )
        
        max_current_count = None
        max_pending_count = None
        
        tag_ids_to_full_counts = {}
        
        showed_bad_tag_error = False
        
        for ( i, ( tag, ( current_count, pending_count ) ) ) in enumerate( tags_to_counts.items() ):
            
            try:
                
                tag_id = self.modules_tags.GetTagId( tag )
                
            except HydrusExceptions.TagSizeException:
                
                if not showed_bad_tag_error:
                    
                    showed_bad_tag_error = True
                    
                    HydrusData.ShowText( 'Hey, you seem to have an invalid tag in view right now! Please run the \'repair invalid tags\' routine under the \'database\' menu asap!' )
                    
                
                continue
                
            
            tag_ids_to_full_counts[ tag_id ] = ( current_count, max_current_count, pending_count, max_pending_count )
            
            if i % 100 == 0:
                
                if job_key is not None and job_key.IsCancelled():
                    
                    return []
                    
                
            
        
        if job_key is not None and job_key.IsCancelled():
            
            return []
            
        
        predicates = self._GeneratePredicatesFromTagIdsAndCounts( ClientTags.TAG_DISPLAY_ACTUAL, display_tag_service_id, tag_ids_to_full_counts, inclusive, job_key = job_key )
        
        return predicates
        
    
    def _GetMediaResults( self, hash_ids: typing.Iterable[ int ], sorted = False ):
        
        ( cached_media_results, missing_hash_ids ) = self._weakref_media_result_cache.GetMediaResultsAndMissing( hash_ids )
        
        if len( missing_hash_ids ) > 0:
            
            # get first detailed results
            
            missing_hash_ids_to_hashes = self.modules_hashes_local_cache.GetHashIdsToHashes( hash_ids = missing_hash_ids )
            
            with self._MakeTemporaryIntegerTable( missing_hash_ids, 'hash_id' ) as temp_table_name:
                
                # everything here is temp hashes to metadata
                
                hash_ids_to_info = { hash_id : ClientMediaManagers.FileInfoManager( hash_id, missing_hash_ids_to_hashes[ hash_id ], size, mime, width, height, duration, num_frames, has_audio, num_words ) for ( hash_id, size, mime, width, height, duration, num_frames, has_audio, num_words ) in self._Execute( 'SELECT * FROM {} CROSS JOIN files_info USING ( hash_id );'.format( temp_table_name ) ) }
                
                ( hash_ids_to_current_file_service_ids_and_timestamps,
                  hash_ids_to_deleted_file_service_ids_and_timestamps,
                  hash_ids_to_pending_file_service_ids,
                  hash_ids_to_petitioned_file_service_ids
                ) = self.modules_files_storage.GetHashIdsToServiceInfoDicts( temp_table_name )
                
                hash_ids_to_urls = HydrusData.BuildKeyToSetDict( self._Execute( 'SELECT hash_id, url FROM {} CROSS JOIN url_map USING ( hash_id ) CROSS JOIN urls USING ( url_id );'.format( temp_table_name ) ) )
                
                hash_ids_to_service_ids_and_filenames = HydrusData.BuildKeyToListDict( ( ( hash_id, ( service_id, filename ) ) for ( hash_id, service_id, filename ) in self._Execute( 'SELECT hash_id, service_id, filename FROM {} CROSS JOIN service_filenames USING ( hash_id );'.format( temp_table_name ) ) ) )
                
                hash_ids_to_local_ratings = HydrusData.BuildKeyToListDict( ( ( hash_id, ( service_id, rating ) ) for ( service_id, hash_id, rating ) in self._Execute( 'SELECT service_id, hash_id, rating FROM {} CROSS JOIN local_ratings USING ( hash_id );'.format( temp_table_name ) ) ) )
                
                hash_ids_to_names_and_notes = HydrusData.BuildKeyToListDict( ( ( hash_id, ( name, note ) ) for ( hash_id, name, note ) in self._Execute( 'SELECT file_notes.hash_id, label, note FROM {} CROSS JOIN file_notes USING ( hash_id ), labels, notes ON ( file_notes.name_id = labels.label_id AND file_notes.note_id = notes.note_id );'.format( temp_table_name ) ) ) )
                
                hash_ids_to_file_viewing_stats = HydrusData.BuildKeyToListDict( ( ( hash_id, ( canvas_type, last_viewed_timestamp, views, viewtime ) ) for ( hash_id, canvas_type, last_viewed_timestamp, views, viewtime ) in self._Execute( 'SELECT hash_id, canvas_type, last_viewed_timestamp, views, viewtime FROM {} CROSS JOIN file_viewing_stats USING ( hash_id );'.format( temp_table_name ) ) ) )
                
                hash_ids_to_file_viewing_stats_managers = { hash_id : ClientMediaManagers.FileViewingStatsManager( file_viewing_stats ) for ( hash_id, file_viewing_stats ) in hash_ids_to_file_viewing_stats.items() }
                
                hash_ids_to_file_modified_timestamps = dict( self._Execute( 'SELECT hash_id, file_modified_timestamp FROM {} CROSS JOIN file_modified_timestamps USING ( hash_id );'.format( temp_table_name ) ) )
                
                hash_ids_to_current_file_service_ids = { hash_id : [ file_service_id for ( file_service_id, timestamp ) in file_service_ids_and_timestamps ] for ( hash_id, file_service_ids_and_timestamps ) in hash_ids_to_current_file_service_ids_and_timestamps.items() }
                
                hash_ids_to_tags_managers = self._GetForceRefreshTagsManagersWithTableHashIds( missing_hash_ids, temp_table_name, hash_ids_to_current_file_service_ids = hash_ids_to_current_file_service_ids )
                
            
            # build it
            
            service_ids_to_service_keys = self.modules_services.GetServiceIdsToServiceKeys()
            
            missing_media_results = []
            
            for hash_id in missing_hash_ids:
                
                tags_manager = hash_ids_to_tags_managers[ hash_id ]
                
                #
                
                current_file_service_keys_to_timestamps = { service_ids_to_service_keys[ service_id ] : timestamp for ( service_id, timestamp ) in hash_ids_to_current_file_service_ids_and_timestamps[ hash_id ] }
                
                deleted_file_service_keys_to_timestamps = { service_ids_to_service_keys[ service_id ] : ( timestamp, original_timestamp ) for ( service_id, timestamp, original_timestamp ) in hash_ids_to_deleted_file_service_ids_and_timestamps[ hash_id ] }
                
                pending_file_service_keys = { service_ids_to_service_keys[ service_id ] for service_id in hash_ids_to_pending_file_service_ids[ hash_id ] }
                
                petitioned_file_service_keys = { service_ids_to_service_keys[ service_id ] for service_id in hash_ids_to_petitioned_file_service_ids[ hash_id ] }
                
                inbox = hash_id in self.modules_files_metadata_basic.inbox_hash_ids
                
                urls = hash_ids_to_urls[ hash_id ]
                
                service_ids_to_filenames = HydrusData.BuildKeyToListDict( hash_ids_to_service_ids_and_filenames[ hash_id ] )
                
                service_keys_to_filenames = { service_ids_to_service_keys[ service_id ] : filenames for ( service_id, filenames ) in list(service_ids_to_filenames.items()) }
                
                if hash_id in hash_ids_to_file_modified_timestamps:
                    
                    file_modified_timestamp = hash_ids_to_file_modified_timestamps[ hash_id ]
                    
                else:
                    
                    file_modified_timestamp = None
                    
                
                locations_manager = ClientMediaManagers.LocationsManager( current_file_service_keys_to_timestamps, deleted_file_service_keys_to_timestamps, pending_file_service_keys, petitioned_file_service_keys, inbox, urls, service_keys_to_filenames, file_modified_timestamp = file_modified_timestamp )
                
                #
                
                local_ratings = { service_ids_to_service_keys[ service_id ] : rating for ( service_id, rating ) in hash_ids_to_local_ratings[ hash_id ] }
                
                ratings_manager = ClientMediaManagers.RatingsManager( local_ratings )
                
                #
                
                if hash_id in hash_ids_to_names_and_notes:
                    
                    names_to_notes = dict( hash_ids_to_names_and_notes[ hash_id ] )
                    
                else:
                    
                    names_to_notes = dict()
                    
                
                notes_manager = ClientMediaManagers.NotesManager( names_to_notes )
                
                #
                
                if hash_id in hash_ids_to_file_viewing_stats_managers:
                    
                    file_viewing_stats_manager = hash_ids_to_file_viewing_stats_managers[ hash_id ]
                    
                else:
                    
                    file_viewing_stats_manager = ClientMediaManagers.FileViewingStatsManager.STATICGenerateEmptyManager()
                    
                
                #
                
                if hash_id in hash_ids_to_info:
                    
                    file_info_manager = hash_ids_to_info[ hash_id ]
                    
                else:
                    
                    hash = missing_hash_ids_to_hashes[ hash_id ]
                    
                    file_info_manager = ClientMediaManagers.FileInfoManager( hash_id, hash )
                    
                
                missing_media_results.append( ClientMediaResult.MediaResult( file_info_manager, tags_manager, locations_manager, ratings_manager, notes_manager, file_viewing_stats_manager ) )
                
            
            self._weakref_media_result_cache.AddMediaResults( missing_media_results )
            
            cached_media_results.extend( missing_media_results )
            
        
        media_results = cached_media_results
        
        if sorted:
            
            hash_ids_to_media_results = { media_result.GetHashId() : media_result for media_result in media_results }
            
            media_results = [ hash_ids_to_media_results[ hash_id ] for hash_id in hash_ids if hash_id in hash_ids_to_media_results ]
            
        
        return media_results
        
    
    def _GetMediaResultFromHash( self, hash ) -> ClientMediaResult.MediaResult:
        
        media_results = self._GetMediaResultsFromHashes( [ hash ] )
        
        return media_results[0]
        
    
    def _GetMediaResultsFromHashes( self, hashes: typing.Iterable[ bytes ], sorted: bytes = False ) -> typing.List[ ClientMediaResult.MediaResult ]:
        
        query_hash_ids = set( self.modules_hashes_local_cache.GetHashIds( hashes ) )
        
        media_results = self._GetMediaResults( query_hash_ids )
        
        if sorted:
            
            if len( hashes ) > len( query_hash_ids ):
                
                hashes = HydrusData.DedupeList( hashes )
                
            
            hashes_to_media_results = { media_result.GetHash() : media_result for media_result in media_results }
            
            media_results = [ hashes_to_media_results[ hash ] for hash in hashes if hash in hashes_to_media_results ]
            
        
        return media_results
        
    
    def _GetNumsPending( self ):
        
        services = self.modules_services.GetServices( ( HC.TAG_REPOSITORY, HC.FILE_REPOSITORY, HC.IPFS ) )
        
        pendings = {}
        
        for service in services:
            
            service_key = service.GetServiceKey()
            service_type = service.GetServiceType()
            
            service_id = self.modules_services.GetServiceId( service_key )
            
            if service_type in ( HC.FILE_REPOSITORY, HC.IPFS ):
                
                info_types = { HC.SERVICE_INFO_NUM_PENDING_FILES, HC.SERVICE_INFO_NUM_PETITIONED_FILES }
                
            elif service_type == HC.TAG_REPOSITORY:
                
                info_types = { HC.SERVICE_INFO_NUM_PENDING_MAPPINGS, HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS, HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS, HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS, HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS, HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS }
                
            
            pendings[ service_key ] = self._GetServiceInfoSpecific( service_id, service_type, info_types )
            
        
        return pendings
        
    
    def _GetOptions( self ):
        
        result = self._Execute( 'SELECT options FROM options;' ).fetchone()
        
        if result is None:
            
            options = ClientDefaults.GetClientDefaultOptions()
            
            self._Execute( 'INSERT INTO options ( options ) VALUES ( ? );', ( options, ) )
            
        else:
            
            ( options, ) = result
            
            default_options = ClientDefaults.GetClientDefaultOptions()
            
            for key in default_options:
                
                if key not in options: options[ key ] = default_options[ key ]
                
            
        
        return options
        
    
    def _GetPending( self, service_key, content_types ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        service = self.modules_services.GetService( service_id )
        
        service_type = service.GetServiceType()
        
        if service_type in HC.REPOSITORIES:
            
            account = service.GetAccount()
            
            client_to_server_update = HydrusNetwork.ClientToServerUpdate()
            
            if service_type == HC.TAG_REPOSITORY:
                
                if HC.CONTENT_TYPE_MAPPINGS in content_types:
                    
                    if account.HasPermission( HC.CONTENT_TYPE_MAPPINGS, HC.PERMISSION_ACTION_CREATE ):
                        
                        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( service_id )
                        
                        pending_dict = HydrusData.BuildKeyToListDict( self._Execute( 'SELECT tag_id, hash_id FROM ' + pending_mappings_table_name + ' ORDER BY tag_id LIMIT 100;' ) )
                        
                        pending_mapping_ids = list( pending_dict.items() )
                        
                        # dealing with a scary situation when (due to some bug) mappings are current and pending. they get uploaded, but the content update makes no changes, so we cycle infitely!
                        addable_pending_mapping_ids = self._FilterExistingUpdateMappings( service_id, pending_mapping_ids, HC.CONTENT_UPDATE_ADD )
                        
                        pending_mapping_weight = sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in pending_mapping_ids ) )
                        addable_pending_mapping_weight = sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in addable_pending_mapping_ids ) )
                        
                        if pending_mapping_weight != addable_pending_mapping_weight:
                            
                            message = 'Hey, while going through the pending tags to upload, it seemed some were simultaneously already in the \'current\' state. This looks like a bug.'
                            message += os.linesep * 2
                            message += 'Please run _database->check and repair->fix logically inconsistent mappings_. If everything seems good after that and you do not get this message again, you should be all fixed. If not, you may need to regenerate your mappings storage cache under the \'database\' menu. If that does not work, hydev would like to know about it!'
                            
                            HydrusData.ShowText( message )
                            
                            raise HydrusExceptions.VetoException( 'Logically inconsistent mappings detected!' )
                            
                        
                        for ( tag_id, hash_ids ) in pending_mapping_ids:
                            
                            tag = self.modules_tags_local_cache.GetTag( tag_id )
                            hashes = self.modules_hashes_local_cache.GetHashes( hash_ids )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_MAPPINGS, ( tag, hashes ) )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PEND, content )
                            
                        
                    
                    if account.HasPermission( HC.CONTENT_TYPE_MAPPINGS, HC.PERMISSION_ACTION_PETITION ):
                        
                        petitioned_dict = HydrusData.BuildKeyToListDict( [ ( ( tag_id, reason_id ), hash_id ) for ( tag_id, hash_id, reason_id ) in self._Execute( 'SELECT tag_id, hash_id, reason_id FROM ' + petitioned_mappings_table_name + ' ORDER BY reason_id LIMIT 100;' ) ] )
                        
                        petitioned_mapping_ids = list( petitioned_dict.items() )
                        
                        # dealing with a scary situation when (due to some bug) mappings are deleted and petitioned. they get uploaded, but the content update makes no changes, so we cycle infitely!
                        deletable_and_petitioned_mappings = self._FilterExistingUpdateMappings(
                            service_id,
                            [ ( tag_id, hash_ids ) for ( ( tag_id, reason_id ), hash_ids ) in petitioned_mapping_ids ],
                            HC.CONTENT_UPDATE_DELETE
                        )
                        
                        petitioned_mapping_weight = sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in petitioned_mapping_ids ) )
                        deletable_petitioned_mapping_weight = sum( ( len( hash_ids ) for ( tag_id, hash_ids ) in deletable_and_petitioned_mappings ) )
                        
                        if petitioned_mapping_weight != deletable_petitioned_mapping_weight:
                            
                            message = 'Hey, while going through the petitioned tags to upload, it seemed some were simultaneously already in the \'deleted\' state. This looks like a bug.'
                            message += os.linesep * 2
                            message += 'Please run _database->check and repair->fix logically inconsistent mappings_. If everything seems good after that and you do not get this message again, you should be all fixed. If not, you may need to regenerate your mappings storage cache under the \'database\' menu. If that does not work, hydev would like to know about it!'
                            
                            HydrusData.ShowText( message )
                            
                            raise HydrusExceptions.VetoException( 'Logically inconsistent mappings detected!' )
                            
                        
                        for ( ( tag_id, reason_id ), hash_ids ) in petitioned_mapping_ids:
                            
                            tag = self.modules_tags_local_cache.GetTag( tag_id )
                            hashes = self.modules_hashes_local_cache.GetHashes( hash_ids )
                            
                            reason = self.modules_texts.GetText( reason_id )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_MAPPINGS, ( tag, hashes ) )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PETITION, content, reason )
                            
                        
                    
                
                if HC.CONTENT_TYPE_TAG_PARENTS in content_types:
                    
                    if account.HasPermission( HC.CONTENT_TYPE_TAG_PARENTS, HC.PERMISSION_ACTION_PETITION ):
                        
                        pending = self._Execute( 'SELECT child_tag_id, parent_tag_id, reason_id FROM tag_parent_petitions WHERE service_id = ? AND status = ? ORDER BY reason_id LIMIT 1;', ( service_id, HC.CONTENT_STATUS_PENDING ) ).fetchall()
                        
                        for ( child_tag_id, parent_tag_id, reason_id ) in pending:
                            
                            child_tag = self.modules_tags_local_cache.GetTag( child_tag_id )
                            parent_tag = self.modules_tags_local_cache.GetTag( parent_tag_id )
                            
                            reason = self.modules_texts.GetText( reason_id )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_PARENTS, ( child_tag, parent_tag ) )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PEND, content, reason )
                            
                        
                        petitioned = self._Execute( 'SELECT child_tag_id, parent_tag_id, reason_id FROM tag_parent_petitions WHERE service_id = ? AND status = ? ORDER BY reason_id LIMIT 100;', ( service_id, HC.CONTENT_STATUS_PETITIONED ) ).fetchall()
                        
                        for ( child_tag_id, parent_tag_id, reason_id ) in petitioned:
                            
                            child_tag = self.modules_tags_local_cache.GetTag( child_tag_id )
                            parent_tag = self.modules_tags_local_cache.GetTag( parent_tag_id )
                            
                            reason = self.modules_texts.GetText( reason_id )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_PARENTS, ( child_tag, parent_tag ) )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PETITION, content, reason )
                            
                        
                    
                
                if HC.CONTENT_TYPE_TAG_SIBLINGS in content_types:
                    
                    if account.HasPermission( HC.CONTENT_TYPE_TAG_SIBLINGS, HC.PERMISSION_ACTION_PETITION ):
                        
                        pending = self._Execute( 'SELECT bad_tag_id, good_tag_id, reason_id FROM tag_sibling_petitions WHERE service_id = ? AND status = ? ORDER BY reason_id LIMIT 100;', ( service_id, HC.CONTENT_STATUS_PENDING ) ).fetchall()
                        
                        for ( bad_tag_id, good_tag_id, reason_id ) in pending:
                            
                            bad_tag = self.modules_tags_local_cache.GetTag( bad_tag_id )
                            good_tag = self.modules_tags_local_cache.GetTag( good_tag_id )
                            
                            reason = self.modules_texts.GetText( reason_id )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_SIBLINGS, ( bad_tag, good_tag ) )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PEND, content, reason )
                            
                        
                        petitioned = self._Execute( 'SELECT bad_tag_id, good_tag_id, reason_id FROM tag_sibling_petitions WHERE service_id = ? AND status = ? ORDER BY reason_id LIMIT 100;', ( service_id, HC.CONTENT_STATUS_PETITIONED ) ).fetchall()
                        
                        for ( bad_tag_id, good_tag_id, reason_id ) in petitioned:
                            
                            bad_tag = self.modules_tags_local_cache.GetTag( bad_tag_id )
                            good_tag = self.modules_tags_local_cache.GetTag( good_tag_id )
                            
                            reason = self.modules_texts.GetText( reason_id )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_TAG_SIBLINGS, ( bad_tag, good_tag ) )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PETITION, content, reason )
                            
                        
                    
                
            elif service_type == HC.FILE_REPOSITORY:
                
                if HC.CONTENT_TYPE_FILES in content_types:
                    
                    if account.HasPermission( HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_CREATE ):
                        
                        result = self.modules_files_storage.GetAPendingHashId( service_id )
                        
                        if result is not None:
                            
                            hash_id = result
                            
                            media_result = self._GetMediaResults( ( hash_id, ) )[ 0 ]
                            
                            return media_result
                            
                        
                    
                    if account.HasPermission( HC.CONTENT_TYPE_FILES, HC.PERMISSION_ACTION_PETITION ):
                        
                        petitioned_rows = self.modules_files_storage.GetSomePetitionedRows( service_id )
                        
                        for ( reason_id, hash_ids ) in petitioned_rows:
                            
                            hashes = self.modules_hashes_local_cache.GetHashes( hash_ids )
                            
                            reason = self.modules_texts.GetText( reason_id )
                            
                            content = HydrusNetwork.Content( HC.CONTENT_TYPE_FILES, hashes )
                            
                            client_to_server_update.AddContent( HC.CONTENT_UPDATE_PETITION, content, reason )
                            
                        
                    
                
            
            if client_to_server_update.HasContent():
                
                return client_to_server_update
                
            
        elif service_type == HC.IPFS:
            
            result = self.modules_files_storage.GetAPendingHashId( service_id )
            
            if result is not None:
                
                hash_id = result
                
                media_result = self._GetMediaResults( ( hash_id, ) )[ 0 ]
                
                return media_result
                
            
            while True:
                
                result = self.modules_files_storage.GetAPetitionedHashId( service_id )
                
                if result is None:
                    
                    break
                    
                else:
                    
                    hash_id = result
                    
                    hash = self.modules_hashes_local_cache.GetHash( hash_id )
                    
                    try:
                        
                        multihash = self._GetServiceFilename( service_id, hash_id )
                        
                    except HydrusExceptions.DataMissing:
                        
                        # somehow this file exists in ipfs (or at least is petitioned), but there is no multihash.
                        # this is probably due to a legacy sync issue
                        # so lets just process that now and continue
                        # in future we'll have ipfs service sync to repopulate missing filenames
                        
                        content_update = HydrusData.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_DELETE, ( hash, ) )
                        
                        service_keys_to_content_updates = { service_key : [ content_update ] }
                        
                        self._ProcessContentUpdates( service_keys_to_content_updates )
                        
                        continue
                        
                    
                    return ( hash, multihash )
                    
                
            
        
        return None
        
    
    def _GetPossibleAdditionalDBFilenames( self ):
        
        paths = HydrusDB.HydrusDB._GetPossibleAdditionalDBFilenames( self )
        
        paths.append( 'mpv.conf' )
        
        return paths
        
    
    def _GetRecentTags( self, service_key ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        # we could be clever and do LIMIT and ORDER BY in the delete, but not all compilations of SQLite have that turned on, so let's KISS
        
        tag_ids_to_timestamp = { tag_id : timestamp for ( tag_id, timestamp ) in self._Execute( 'SELECT tag_id, timestamp FROM recent_tags WHERE service_id = ?;', ( service_id, ) ) }
        
        def sort_key( key ):
            
            return tag_ids_to_timestamp[ key ]
            
        
        newest_first = list(tag_ids_to_timestamp.keys())
        
        newest_first.sort( key = sort_key, reverse = True )
        
        num_we_want = HG.client_controller.new_options.GetNoneableInteger( 'num_recent_tags' )
        
        if num_we_want == None:
            
            num_we_want = 20
            
        
        decayed = newest_first[ num_we_want : ]
        
        if len( decayed ) > 0:
            
            self._ExecuteMany( 'DELETE FROM recent_tags WHERE service_id = ? AND tag_id = ?;', ( ( service_id, tag_id ) for tag_id in decayed ) )
            
        
        sorted_recent_tag_ids = newest_first[ : num_we_want ]
        
        tag_ids_to_tags = self.modules_tags_local_cache.GetTagIdsToTags( tag_ids = sorted_recent_tag_ids )
        
        sorted_recent_tags = [ tag_ids_to_tags[ tag_id ] for tag_id in sorted_recent_tag_ids ]
        
        return sorted_recent_tags
        
    
    def _GetRelatedTags( self, service_key, skip_hash, search_tags, max_results, max_time_to_take ):
        
        stop_time_for_finding_files = HydrusData.GetNowPrecise() + ( max_time_to_take / 2 )
        stop_time_for_finding_tags = HydrusData.GetNowPrecise() + ( max_time_to_take / 2 )
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        skip_hash_id = self.modules_hashes_local_cache.GetHashId( skip_hash )
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( service_id )
        
        tag_ids = [ self.modules_tags.GetTagId( tag ) for tag in search_tags ]
        
        random.shuffle( tag_ids )
        
        hash_ids_counter = collections.Counter()
        
        with self._MakeTemporaryIntegerTable( tag_ids, 'tag_id' ) as temp_table_name:
            
            # temp tags to mappings
            cursor = self._Execute( 'SELECT hash_id FROM {} CROSS JOIN {} USING ( tag_id );'.format( temp_table_name, current_mappings_table_name ) )
            
            cancelled_hook = lambda: HydrusData.TimeHasPassedPrecise( stop_time_for_finding_files )
            
            for ( hash_id, ) in HydrusDB.ReadFromCancellableCursor( cursor, 128, cancelled_hook = cancelled_hook ):
                
                hash_ids_counter[ hash_id ] += 1
                
            
        
        if skip_hash_id in hash_ids_counter:
            
            del hash_ids_counter[ skip_hash_id ]
            
        
        #
        
        if len( hash_ids_counter ) == 0:
            
            return []
            
        
        # this stuff is often 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.....
        # the 1 stuff often produces large quantities of the same very popular tag, so your search for [ 'eva', 'female' ] will produce 'touhou' because so many 2hu images have 'female'
        # so we want to do a 'soft' intersect, only picking the files that have the greatest number of shared search_tags
        # this filters to only the '2' results, which gives us eva females and their hair colour and a few choice other popular tags for that particular domain
        
        [ ( gumpf, largest_count ) ] = hash_ids_counter.most_common( 1 )
        
        hash_ids = [ hash_id for ( hash_id, current_count ) in hash_ids_counter.items() if current_count > largest_count * 0.8 ]
        
        counter = collections.Counter()
        
        random.shuffle( hash_ids )
        
        for hash_id in hash_ids:
            
            for tag_id in self._STI( self._Execute( 'SELECT tag_id FROM ' + current_mappings_table_name + ' WHERE hash_id = ?;', ( hash_id, ) ) ):
                
                counter[ tag_id ] += 1
                
            
            if HydrusData.TimeHasPassedPrecise( stop_time_for_finding_tags ):
                
                break
                
            
        
        #
        
        for tag_id in tag_ids:
            
            if tag_id in counter:
                
                del counter[ tag_id ]
                
            
        
        results = counter.most_common( max_results )
        
        inclusive = True
        pending_count = 0
        
        tag_ids_to_full_counts = { tag_id : ( current_count, None, pending_count, None ) for ( tag_id, current_count ) in results }
        
        predicates = self._GeneratePredicatesFromTagIdsAndCounts( ClientTags.TAG_DISPLAY_STORAGE, service_id, tag_ids_to_full_counts, inclusive )
        
        return predicates
        
    
    def _GetRepositoryThumbnailHashesIDoNotHave( self, service_key ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( service_id, HC.CONTENT_STATUS_CURRENT )
        
        needed_hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} NATURAL JOIN files_info WHERE mime IN {} EXCEPT SELECT hash_id FROM remote_thumbnails WHERE service_id = ?;'.format( current_files_table_name, HydrusData.SplayListForDB( HC.MIMES_WITH_THUMBNAILS ) ), ( service_id, ) ) )
        
        needed_hashes = []
        
        client_files_manager = HG.client_controller.client_files_manager
        
        for hash_id in needed_hash_ids:
            
            hash = self.modules_hashes_local_cache.GetHash( hash_id )
            
            if client_files_manager.LocklessHasThumbnail( hash ):
                
                self._Execute( 'INSERT OR IGNORE INTO remote_thumbnails ( service_id, hash_id ) VALUES ( ?, ? );', ( service_id, hash_id ) )
                
            else:
                
                needed_hashes.append( hash )
                
                if len( needed_hashes ) == 10000:
                    
                    return needed_hashes
                    
                
            
        
        return needed_hashes
        
    
    def _GetServiceDirectoryHashes( self, service_key, dirname ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        directory_id = self.modules_texts.GetTextId( dirname )
        
        hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM service_directory_file_map WHERE service_id = ? AND directory_id = ?;', ( service_id, directory_id ) ) )
        
        hashes = self.modules_hashes_local_cache.GetHashes( hash_ids )
        
        return hashes
        
    
    def _GetServiceDirectoriesInfo( self, service_key ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        incomplete_info = self._Execute( 'SELECT directory_id, num_files, total_size, note FROM service_directories WHERE service_id = ?;', ( service_id, ) ).fetchall()
        
        info = [ ( self.modules_texts.GetText( directory_id ), num_files, total_size, note ) for ( directory_id, num_files, total_size, note ) in incomplete_info ]
        
        return info
        
    
    def _GetServiceFilename( self, service_id, hash_id ):
        
        result = self._Execute( 'SELECT filename FROM service_filenames WHERE service_id = ? AND hash_id = ?;', ( service_id, hash_id ) ).fetchone()
        
        if result is None:
            
            raise HydrusExceptions.DataMissing( 'Service filename not found!' )
            
        
        ( filename, ) = result
        
        return filename
        
    
    def _GetServiceFilenames( self, service_key, hashes ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
        
        result = sorted( ( filename for ( filename, ) in self._Execute( 'SELECT filename FROM service_filenames WHERE service_id = ? AND hash_id IN ' + HydrusData.SplayListForDB( hash_ids ) + ';', ( service_id, ) ) ) )
        
        return result
        
    
    def _GetServiceInfo( self, service_key ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        service = self.modules_services.GetService( service_id )
        
        service_type = service.GetServiceType()
        
        if service_type in ( HC.COMBINED_LOCAL_FILE, HC.LOCAL_FILE_DOMAIN, HC.FILE_REPOSITORY ):
            
            info_types = { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_VIEWABLE_FILES, HC.SERVICE_INFO_TOTAL_SIZE, HC.SERVICE_INFO_NUM_DELETED_FILES }
            
        elif service_type == HC.LOCAL_FILE_TRASH_DOMAIN:
            
            info_types = { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_VIEWABLE_FILES, HC.SERVICE_INFO_TOTAL_SIZE }
            
        elif service_type == HC.IPFS:
            
            info_types = { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_VIEWABLE_FILES, HC.SERVICE_INFO_TOTAL_SIZE }
            
        elif service_type == HC.LOCAL_TAG:
            
            info_types = { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_TAGS, HC.SERVICE_INFO_NUM_MAPPINGS }
            
        elif service_type == HC.TAG_REPOSITORY:
            
            info_types = { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_TAGS, HC.SERVICE_INFO_NUM_MAPPINGS, HC.SERVICE_INFO_NUM_DELETED_MAPPINGS }
            
        elif service_type in ( HC.LOCAL_RATING_LIKE, HC.LOCAL_RATING_NUMERICAL ):
            
            info_types = { HC.SERVICE_INFO_NUM_FILES }
            
        elif service_type == HC.LOCAL_BOORU:
            
            info_types = { HC.SERVICE_INFO_NUM_SHARES }
            
        else:
            
            info_types = set()
            
        
        service_info = self._GetServiceInfoSpecific( service_id, service_type, info_types )
        
        return service_info
        
    
    def _GetServiceInfoSpecific( self, service_id, service_type, info_types, calculate_missing = True ):
        
        info_types = set( info_types )
        
        results = { info_type : info for ( info_type, info ) in self._Execute( 'SELECT info_type, info FROM service_info WHERE service_id = ? AND info_type IN ' + HydrusData.SplayListForDB( info_types ) + ';', ( service_id, ) ) }
        
        if len( results ) != len( info_types ) and calculate_missing:
            
            info_types_hit = list( results.keys() )
            
            info_types_missed = info_types.difference( info_types_hit )
            
            for info_type in info_types_missed:
                
                info = None
                result = None
                
                save_it = True
                
                if service_type in HC.FILE_SERVICES:
                    
                    if info_type in ( HC.SERVICE_INFO_NUM_PENDING_FILES, HC.SERVICE_INFO_NUM_PETITIONED_FILES ):
                        
                        save_it = False
                        
                    
                    if info_type == HC.SERVICE_INFO_NUM_FILES:
                        
                        info = self.modules_files_storage.GetCurrentFilesCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_VIEWABLE_FILES:
                        
                        info = self.modules_files_storage.GetCurrentFilesCount( service_id, only_viewable = True )
                        
                    elif info_type == HC.SERVICE_INFO_TOTAL_SIZE:
                        
                        info = self.modules_files_storage.GetCurrentFilesTotalSize( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_DELETED_FILES:
                        
                        info = self.modules_files_storage.GetDeletedFilesCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PENDING_FILES:
                        
                        info = self.modules_files_storage.GetPendingFilesCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_FILES:
                        
                        info = self.modules_files_storage.GetPetitionedFilesCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_INBOX:
                        
                        info = self.modules_files_storage.GetCurrentFilesInboxCount( service_id )
                        
                    
                elif service_type in HC.REAL_TAG_SERVICES:
                    
                    if info_type in ( HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS, HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS, HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS, HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS ):
                        
                        save_it = False
                        
                    
                    if info_type == HC.SERVICE_INFO_NUM_FILES:
                        
                        info = self.modules_mappings_storage.GetCurrentFilesCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_TAGS:
                        
                        info = self.modules_tag_search.GetTagCount( self.modules_services.combined_file_service_id, service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_MAPPINGS:
                        
                        info = self.modules_mappings_counts.GetTotalCurrentCount( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PENDING_MAPPINGS:
                        
                        # since pending is nearly always far smaller rowcount than current, if I pull this from a/c table, it is a HUGE waste of time and not faster than counting the raw table rows!
                        
                        info = self.modules_mappings_storage.GetPendingMappingsCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_DELETED_MAPPINGS:
                        
                        # since pending is nearly always far smaller rowcount than current, if I pull this from a/c table, it is a HUGE waste of time and not faster than counting the raw table rows!
                        
                        info = self.modules_mappings_storage.GetDeletedMappingsCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS:
                        
                        # since pending is nearly always far smaller rowcount than current, if I pull this from a/c table, it is a HUGE waste of time and not faster than counting the raw table rows!
                        
                        info = self.modules_mappings_storage.GetPetitionedMappingsCount( service_id )
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS:
                        
                        ( info, ) = self._Execute( 'SELECT COUNT( * ) FROM tag_sibling_petitions WHERE service_id = ? AND status = ?;', ( service_id, HC.CONTENT_STATUS_PENDING ) ).fetchone()
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS:
                        
                        ( info, ) = self._Execute( 'SELECT COUNT( * ) FROM tag_sibling_petitions WHERE service_id = ? AND status = ?;', ( service_id, HC.CONTENT_STATUS_PETITIONED ) ).fetchone()
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS:
                        
                        ( info, ) = self._Execute( 'SELECT COUNT( * ) FROM tag_parent_petitions WHERE service_id = ? AND status = ?;', ( service_id, HC.CONTENT_STATUS_PENDING ) ).fetchone()
                        
                    elif info_type == HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS:
                        
                        ( info, ) = self._Execute( 'SELECT COUNT( * ) FROM tag_parent_petitions WHERE service_id = ? AND status = ?;', ( service_id, HC.CONTENT_STATUS_PETITIONED ) ).fetchone()
                        
                    
                elif service_type in ( HC.LOCAL_RATING_LIKE, HC.LOCAL_RATING_NUMERICAL ):
                    
                    if info_type == HC.SERVICE_INFO_NUM_FILES:
                        
                        ( info, ) = self._Execute( 'SELECT COUNT( * ) FROM local_ratings WHERE service_id = ?;', ( service_id, ) ).fetchone()
                        
                    
                elif service_type == HC.LOCAL_BOORU:
                    
                    if info_type == HC.SERVICE_INFO_NUM_SHARES:
                        
                        ( info, ) = self._Execute( 'SELECT COUNT( * ) FROM yaml_dumps WHERE dump_type = ?;', ( ClientDBSerialisable.YAML_DUMP_ID_LOCAL_BOORU, ) ).fetchone()
                        
                    
                
                if info is None:
                    
                    info = 0
                    
                
                if save_it:
                    
                    self._Execute( 'INSERT INTO service_info ( service_id, info_type, info ) VALUES ( ?, ?, ? );', ( service_id, info_type, info ) )
                    
                
                results[ info_type ] = info
                
            
        
        return results
        
    
    def _GetSiteId( self, name ):
        
        result = self._Execute( 'SELECT site_id FROM imageboard_sites WHERE name = ?;', ( name, ) ).fetchone()
        
        if result is None:
            
            self._Execute( 'INSERT INTO imageboard_sites ( name ) VALUES ( ? );', ( name, ) )
            
            site_id = self._GetLastRowId()
            
        else:
            
            ( site_id, ) = result
            
        
        return site_id
        
    
    def _GetTagIdsFromNamespaceIds( self, leaf: ClientDBServices.FileSearchContextLeaf, namespace_ids: typing.Collection[ int ], job_key = None ):
        
        if len( namespace_ids ) == 0:
            
            return set()
            
        
        final_result_tag_ids = set()
        
        with self._MakeTemporaryIntegerTable( namespace_ids, 'namespace_id' ) as temp_namespace_ids_table_name:
            
            tags_table_name = self.modules_tag_search.GetTagsTableName( leaf.file_service_id, leaf.tag_service_id )
            
            if len( namespace_ids ) == 1:
                
                ( namespace_id, ) = namespace_ids
                
                cursor = self._Execute( 'SELECT tag_id FROM {} WHERE namespace_id = ?;'.format( tags_table_name ), ( namespace_id, ) )
                
            else:
                
                # temp namespaces to tags
                cursor = self._Execute( 'SELECT tag_id FROM {} CROSS JOIN {} USING ( namespace_id );'.format( temp_namespace_ids_table_name, tags_table_name ) )
                
            
            cancelled_hook = None
            
            if job_key is not None:
                
                cancelled_hook = job_key.IsCancelled
                
            
            result_tag_ids = self._STS( HydrusDB.ReadFromCancellableCursor( cursor, 128, cancelled_hook = cancelled_hook ) )
            
            if job_key is not None:
                
                if job_key.IsCancelled():
                    
                    return set()
                    
                
            
            final_result_tag_ids.update( result_tag_ids )
            
        
        return final_result_tag_ids
        
    
    def _GetTagIdsFromNamespaceIdsSubtagIds( self, file_service_id: int, tag_service_id: int, namespace_ids: typing.Collection[ int ], subtag_ids: typing.Collection[ int ], job_key = None ):
        
        if len( namespace_ids ) == 0 or len( subtag_ids ) == 0:
            
            return set()
            
        
        with self._MakeTemporaryIntegerTable( subtag_ids, 'subtag_id' ) as temp_subtag_ids_table_name:
            
            with self._MakeTemporaryIntegerTable( namespace_ids, 'namespace_id' ) as temp_namespace_ids_table_name:
                
                return self._GetTagIdsFromNamespaceIdsSubtagIdsTables( file_service_id, tag_service_id, temp_namespace_ids_table_name, temp_subtag_ids_table_name, job_key = job_key )
                
            
        
    
    def _GetTagIdsFromNamespaceIdsSubtagIdsTables( self, file_service_id: int, tag_service_id: int, namespace_ids_table_name: str, subtag_ids_table_name: str, job_key = None ):
        
        final_result_tag_ids = set()
        
        if tag_service_id == self.modules_services.combined_tag_service_id:
            
            search_tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            search_tag_service_ids = ( tag_service_id, )
            
        
        for search_tag_service_id in search_tag_service_ids:
            
            tags_table_name = self.modules_tag_search.GetTagsTableName( file_service_id, search_tag_service_id )
            
            # temp subtags to tags to temp namespaces
            cursor = self._Execute( 'SELECT tag_id FROM {} CROSS JOIN {} USING ( subtag_id ) CROSS JOIN {} USING ( namespace_id );'.format( subtag_ids_table_name, tags_table_name, namespace_ids_table_name ) )
            
            cancelled_hook = None
            
            if job_key is not None:
                
                cancelled_hook = job_key.IsCancelled
                
            
            result_tag_ids = self._STS( HydrusDB.ReadFromCancellableCursor( cursor, 128, cancelled_hook = cancelled_hook ) )
            
            if job_key is not None:
                
                if job_key.IsCancelled():
                    
                    return set()
                    
                
            
            final_result_tag_ids.update( result_tag_ids )
            
        
        return final_result_tag_ids
        
    
    def _GetTagIdsFromSubtagIds( self, file_service_id: int, tag_service_id: int, subtag_ids: typing.Collection[ int ], job_key = None ):
        
        if len( subtag_ids ) == 0:
            
            return set()
            
        
        with self._MakeTemporaryIntegerTable( subtag_ids, 'subtag_id' ) as temp_subtag_ids_table_name:
            
            return self._GetTagIdsFromSubtagIdsTable( file_service_id, tag_service_id, temp_subtag_ids_table_name, job_key = job_key )
            
        
    
    def _GetTagIdsFromSubtagIdsTable( self, file_service_id: int, tag_service_id: int, subtag_ids_table_name: str, job_key = None ):
        
        final_result_tag_ids = set()
        
        if tag_service_id == self.modules_services.combined_tag_service_id:
            
            search_tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            search_tag_service_ids = ( tag_service_id, )
            
        
        for search_tag_service_id in search_tag_service_ids:
            
            tags_table_name = self.modules_tag_search.GetTagsTableName( file_service_id, search_tag_service_id )
            
            # temp subtags to tags
            cursor = self._Execute( 'SELECT tag_id FROM {} CROSS JOIN {} USING ( subtag_id );'.format( subtag_ids_table_name, tags_table_name ) )
            
            cancelled_hook = None
            
            if job_key is not None:
                
                cancelled_hook = job_key.IsCancelled
                
            
            result_tag_ids = self._STS( HydrusDB.ReadFromCancellableCursor( cursor, 128, cancelled_hook = cancelled_hook ) )
            
            if job_key is not None:
                
                if job_key.IsCancelled():
                    
                    return set()
                    
                
            
            final_result_tag_ids.update( result_tag_ids )
            
        
        return final_result_tag_ids
        
    
    def _GetTrashHashes( self, limit = None, minimum_age = None ):
        
        if limit is None:
            
            limit_phrase = ''
            
        else:
            
            limit_phrase = ' LIMIT ' + str( limit )
            
        
        if minimum_age is None:
            
            age_phrase = ' ORDER BY timestamp ASC' # when deleting until trash is small enough, let's delete oldest first
            
        else:
            
            timestamp_cutoff = HydrusData.GetNow() - minimum_age
            
            age_phrase = ' WHERE timestamp < ' + str( timestamp_cutoff )
            
        
        current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.trash_service_id, HC.CONTENT_STATUS_CURRENT )
        
        hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM {}{}{};'.format( current_files_table_name, age_phrase, limit_phrase ) ) )
        
        hash_ids = self._FilterForFileDeleteLock( self.modules_services.trash_service_id, hash_ids )
        
        if HG.db_report_mode:
            
            message = 'When asked for '
            
            if limit is None:
                
                message += 'all the'
                
            else:
                
                message += 'at most ' + HydrusData.ToHumanInt( limit )
                
            
            message += ' trash files,'
            
            if minimum_age is not None:
                
                message += ' with minimum age ' + ClientData.TimestampToPrettyTimeDelta( timestamp_cutoff, just_now_threshold = 0 ) + ','
                
            
            message += ' I found ' + HydrusData.ToHumanInt( len( hash_ids ) ) + '.'
            
            HydrusData.ShowText( message )
            
        
        return self.modules_hashes_local_cache.GetHashes( hash_ids )
        
    
    def _GetURLStatuses( self, url ) -> typing.List[ ClientImportFiles.FileImportStatus ]:
        
        search_urls = ClientNetworkingFunctions.GetSearchURLs( url )
        
        hash_ids = set()
        
        for search_url in search_urls:
            
            results = self._STS( self._Execute( 'SELECT hash_id FROM url_map NATURAL JOIN urls WHERE url = ?;', ( search_url, ) ) )
            
            hash_ids.update( results )
            
        
        try:
            
            results = [ self._GetHashIdStatus( hash_id, prefix = 'url recognised' ) for hash_id in hash_ids ]
            
        except:
            
            return []
            
        
        return results
        
    
    def _GetWithAndWithoutTagsForFilesFileCount( self, status, tag_service_id, with_these_tag_ids, without_these_tag_ids, hash_ids, hash_ids_table_name, file_service_ids_to_hash_ids ):
        
        # ok, given this selection of files, how many of them on current/pending have any of these tags but not any these, real fast?
        
        count = 0
        
        with self._MakeTemporaryIntegerTable( with_these_tag_ids, 'tag_id' ) as temp_with_these_tag_ids_table_name:
            
            with self._MakeTemporaryIntegerTable( without_these_tag_ids, 'tag_id' ) as temp_without_these_tag_ids_table_name:
                
                for ( file_service_id, batch_of_hash_ids ) in file_service_ids_to_hash_ids.items():
                    
                    if len( batch_of_hash_ids ) == len( hash_ids ):
                        
                        subcount = self._GetWithAndWithoutTagsForFilesFileCountFileService( status, file_service_id, tag_service_id, with_these_tag_ids, temp_with_these_tag_ids_table_name, without_these_tag_ids, temp_without_these_tag_ids_table_name, hash_ids, hash_ids_table_name )
                        
                    else:
                        
                        with self._MakeTemporaryIntegerTable( batch_of_hash_ids, 'hash_id' ) as temp_batch_hash_ids_table_name:
                            
                            subcount = self._GetWithAndWithoutTagsForFilesFileCountFileService( status, file_service_id, tag_service_id, with_these_tag_ids, temp_with_these_tag_ids_table_name, without_these_tag_ids, temp_without_these_tag_ids_table_name, batch_of_hash_ids, temp_batch_hash_ids_table_name )
                            
                        
                    
                    count += subcount
                    
                
            
        
        return count
        
    
    def _GetWithAndWithoutTagsForFilesFileCountFileService( self, status, file_service_id, tag_service_id, with_these_tag_ids, with_these_tag_ids_table_name, without_these_tag_ids, without_these_tag_ids_table_name, hash_ids, hash_ids_table_name ):
        
        # ପୁରୁଣା ଲୋକଙ୍କ ଶକ୍ତି ଦ୍ୱାରା, ଏହି କ୍ରସ୍ କାର୍ଯ୍ୟରେ ଯୋଗ ଦିଅନ୍ତୁ |
        
        # ok, given this selection of files, how many of them on current/pending have any of these tags but not any these, real fast?
        
        statuses_to_table_names = self.modules_mappings_storage.GetFastestStorageMappingTableNames( file_service_id, tag_service_id )
        
        ( current_with_tag_ids, current_with_tag_ids_weight, pending_with_tag_ids, pending_with_tag_ids_weight ) = self.modules_mappings_counts.GetCurrentPendingPositiveCountsAndWeights( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, with_these_tag_ids, tag_ids_table_name = with_these_tag_ids_table_name )
        ( current_without_tag_ids, current_without_tag_ids_weight, pending_without_tag_ids, pending_without_tag_ids_weight ) = self.modules_mappings_counts.GetCurrentPendingPositiveCountsAndWeights( ClientTags.TAG_DISPLAY_STORAGE, file_service_id, tag_service_id, without_these_tag_ids, tag_ids_table_name = without_these_tag_ids_table_name )
        
        mappings_table_name = statuses_to_table_names[ status ]
        
        if status == HC.CONTENT_STATUS_CURRENT:
            
            with_tag_ids = current_with_tag_ids
            with_tag_ids_weight = current_with_tag_ids_weight
            without_tag_ids = current_without_tag_ids
            without_tag_ids_weight = current_without_tag_ids_weight
            
        elif status == HC.CONTENT_STATUS_PENDING:
            
            with_tag_ids = pending_with_tag_ids
            with_tag_ids_weight = pending_with_tag_ids_weight
            without_tag_ids = pending_without_tag_ids
            without_tag_ids_weight = pending_without_tag_ids_weight
            
        
        if with_tag_ids_weight == 0:
            
            # nothing there, so nothing to do!
            
            return 0
            
        
        hash_ids_weight = len( hash_ids )
        
        # in order to reduce overhead, we go full meme and do a bunch of different situations
        
        with self._MakeTemporaryIntegerTable( [], 'tag_id' ) as temp_with_tag_ids_table_name:
            
            with self._MakeTemporaryIntegerTable( [], 'tag_id' ) as temp_without_tag_ids_table_name:
                
                if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( hash_ids_weight, with_tag_ids_weight ):
                    
                    select_with_weight = hash_ids_weight
                    
                else:
                    
                    select_with_weight = with_tag_ids_weight
                    
                
                if len( with_tag_ids ) == 1:
                    
                    ( with_tag_id, ) = with_tag_ids
                    
                    if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( hash_ids_weight, with_tag_ids_weight ):
                        
                        # temp files to mappings
                        select_with_hash_ids_on_storage = 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = {}'.format( hash_ids_table_name, mappings_table_name, with_tag_id )
                        
                    else:
                        
                        # mappings to temp files
                        select_with_hash_ids_on_storage = 'SELECT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) WHERE tag_id = {}'.format( mappings_table_name, hash_ids_table_name, with_tag_id )
                        
                    
                else:
                    
                    # distinct as with many tags hashes can appear twice (e.g. two siblings on the same file)
                    
                    self._ExecuteMany( 'INSERT INTO {} ( tag_id ) VALUES ( ? );'.format( temp_with_tag_ids_table_name ), ( ( with_tag_id, ) for with_tag_id in with_tag_ids ) )
                    
                    if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( hash_ids_weight, with_tag_ids_weight ):
                        
                        # temp files to mappings to temp tags
                        select_with_hash_ids_on_storage = 'SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( hash_id ) CROSS JOIN {} USING ( tag_id )'.format( hash_ids_table_name, mappings_table_name, temp_with_tag_ids_table_name )
                        
                    else:
                        
                        # temp tags to mappings to temp files
                        select_with_hash_ids_on_storage = 'SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( tag_id ) CROSS JOIN {} USING ( hash_id )'.format( temp_with_tag_ids_table_name, mappings_table_name, hash_ids_table_name )
                        
                    
                
                if without_tag_ids_weight == 0:
                    
                    table_phrase = '({})'.format( select_with_hash_ids_on_storage )
                    
                else:
                    
                    # WARNING, WARNING: Big Brain Query, potentially great/awful
                    # note that in the 'clever/file join' situation, the number of total mappings is many, but we are dealing with a few files
                    # in that situation, we want to say 'for every file in this list, check if it exists'. this is the 'NOT EXISTS' thing
                    # when we have lots of files, tag lookups are generally faster, so easier just to search by that tag in one go and check each file against that subquery result. this is 'hash_id NOT IN'
                    
                    if len( without_tag_ids ) == 1:
                        
                        ( without_tag_id, ) = without_tag_ids
                        
                        if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( select_with_weight, without_tag_ids_weight ):
                            
                            # (files to) mappings
                            hash_id_not_in_storage_without = 'NOT EXISTS ( SELECT 1 FROM {} as mt2 WHERE mt1.hash_id = mt2.hash_id and tag_id = {} )'.format( mappings_table_name, without_tag_id )
                            
                        else:
                            
                            hash_id_not_in_storage_without = 'hash_id NOT IN ( SELECT hash_id FROM {} WHERE tag_id = {} )'.format( mappings_table_name, without_tag_id )
                            
                        
                    else:
                        
                        self._ExecuteMany( 'INSERT INTO {} ( tag_id ) VALUES ( ? );'.format( temp_without_tag_ids_table_name ), ( ( without_tag_id, ) for without_tag_id in without_tag_ids ) )
                        
                        if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( select_with_weight, without_tag_ids_weight ):
                            
                            # (files to) mappings to temp tags
                            hash_id_not_in_storage_without = 'NOT EXISTS ( SELECT 1 FROM {} as mt2 CROSS JOIN {} USING ( tag_id ) WHERE mt1.hash_id = mt2.hash_id )'.format( mappings_table_name, temp_without_tag_ids_table_name )
                            
                        else:
                            
                            # temp tags to mappings to temp files
                            hash_id_not_in_storage_without = 'hash_id NOT IN ( SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( tag_id ) )'.format( temp_without_tag_ids_table_name, mappings_table_name )
                            
                        
                    
                    table_phrase = '({}) as mt1 WHERE {}'.format( select_with_hash_ids_on_storage, hash_id_not_in_storage_without )
                    
                
                query = 'SELECT COUNT ( * ) FROM {};'.format( table_phrase )
                
                ( count, ) = self._Execute( query ).fetchone()
                
                return count
                
            
        
    
    def _GetWithAndWithoutTagsFileCountCombined( self, tag_service_id, with_these_tag_ids, without_these_tag_ids ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        statuses_to_count = collections.Counter()
        
        ( current_with_tag_ids, current_with_tag_ids_weight, pending_with_tag_ids, pending_with_tag_ids_weight ) = self.modules_mappings_counts.GetCurrentPendingPositiveCountsAndWeights( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, with_these_tag_ids )
        ( current_without_tag_ids, current_without_tag_ids_weight, pending_without_tag_ids, pending_without_tag_ids_weight ) = self.modules_mappings_counts.GetCurrentPendingPositiveCountsAndWeights( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, without_these_tag_ids )
        
        jobs = []
        
        jobs.append( ( HC.CONTENT_STATUS_CURRENT, current_mappings_table_name, current_with_tag_ids, current_with_tag_ids_weight, current_without_tag_ids, current_without_tag_ids_weight ) )
        jobs.append( ( HC.CONTENT_STATUS_PENDING, pending_mappings_table_name, pending_with_tag_ids, pending_with_tag_ids_weight, pending_without_tag_ids, pending_without_tag_ids_weight ) )
        
        for ( status, mappings_table_name, with_tag_ids, with_tag_ids_weight, without_tag_ids, without_tag_ids_weight ) in jobs:
            
            if with_tag_ids_weight == 0:
                
                # nothing there, so nothing to do!
                
                continue
                
            
            if without_tag_ids_weight == 0 and len( with_tag_ids ) == 1:
                
                statuses_to_count[ status ] = with_tag_ids_weight
                
                continue
                
            
            # ultimately here, we are doing "delete all display mappings with hash_ids that have a storage mapping for a removee tag and no storage mappings for a keep tag
            # in order to reduce overhead, we go full meme and do a bunch of different situations
            
            with self._MakeTemporaryIntegerTable( [], 'tag_id' ) as temp_with_tag_ids_table_name:
                
                with self._MakeTemporaryIntegerTable( [], 'tag_id' ) as temp_without_tag_ids_table_name:
                    
                    if len( with_tag_ids ) == 1:
                        
                        ( with_tag_id, ) = with_tag_ids
                        
                        select_with_hash_ids_on_storage = 'SELECT hash_id FROM {} WHERE tag_id = {}'.format( mappings_table_name, with_tag_id )
                        
                    else:
                        
                        self._ExecuteMany( 'INSERT INTO {} ( tag_id ) VALUES ( ? );'.format( temp_with_tag_ids_table_name ), ( ( with_tag_id, ) for with_tag_id in with_tag_ids ) )
                        
                        # temp tags to mappings
                        select_with_hash_ids_on_storage = 'SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( tag_id )'.format( temp_with_tag_ids_table_name, mappings_table_name )
                        
                    
                    if without_tag_ids_weight == 0:
                        
                        table_phrase = '({})'.format( select_with_hash_ids_on_storage )
                        
                    else:
                        
                        # WARNING, WARNING: Big Brain Query, potentially great/awful
                        # note that in the 'clever/file join' situation, the number of total mappings is many, but we are deleting a few
                        # we want to precisely scan the status of the potential hashes to delete, not scan through them all to see what not to do
                        # therefore, we do NOT EXISTS, which just scans the parts, rather than NOT IN, which does the whole query and then checks against all results
                        
                        if len( without_tag_ids ) == 1:
                            
                            ( without_tag_id, ) = without_tag_ids
                            
                            if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( with_tag_ids_weight, without_tag_ids_weight ):
                                
                                hash_id_not_in_storage_without = 'NOT EXISTS ( SELECT 1 FROM {} as mt2 WHERE mt1.hash_id = mt2.hash_id and tag_id = {} )'.format( mappings_table_name, without_tag_id )
                                
                            else:
                                
                                hash_id_not_in_storage_without = 'hash_id NOT IN ( SELECT hash_id FROM {} WHERE tag_id = {} )'.format( mappings_table_name, without_tag_id )
                                
                            
                        else:
                            
                            self._ExecuteMany( 'INSERT INTO {} ( tag_id ) VALUES ( ? );'.format( temp_without_tag_ids_table_name ), ( ( without_tag_id, ) for without_tag_id in without_tag_ids ) )
                            
                            if ClientDBMappingsStorage.DoingAFileJoinTagSearchIsFaster( with_tag_ids_weight, without_tag_ids_weight ):
                                
                                # (files to) mappings to temp tags
                                hash_id_not_in_storage_without = 'NOT EXISTS ( SELECT 1 FROM {} as mt2 CROSS JOIN {} USING ( tag_id ) WHERE mt1.hash_id = mt2.hash_id )'.format( mappings_table_name, temp_without_tag_ids_table_name )
                                
                            else:
                                
                                # temp tags to mappings
                                hash_id_not_in_storage_without = 'hash_id NOT IN ( SELECT DISTINCT hash_id FROM {} CROSS JOIN {} USING ( tag_id ) )'.format( temp_without_tag_ids_table_name, mappings_table_name )
                                
                            
                        
                        table_phrase = '({}) as mt1 WHERE {}'.format( select_with_hash_ids_on_storage, hash_id_not_in_storage_without )
                        
                    
                    query = 'SELECT COUNT ( * ) FROM {};'.format( table_phrase )
                    
                    ( count, ) = self._Execute( query ).fetchone()
                    
                    statuses_to_count[ status ] = count
                    
                
            
        
        current_count = statuses_to_count[ HC.CONTENT_STATUS_CURRENT ]
        pending_count = statuses_to_count[ HC.CONTENT_STATUS_PENDING ]
        
        return ( current_count, pending_count )
        
    
    def _GroupHashIdsByTagCachedFileServiceId( self, hash_ids, hash_ids_table_name, hash_ids_to_current_file_service_ids = None ):
        
        # when we would love to do a fast cache lookup, it is useful to know if all the hash_ids are on one or two common file domains
        
        if hash_ids_to_current_file_service_ids is None:
            
            hash_ids_to_current_file_service_ids = self.modules_files_storage.GetHashIdsToCurrentServiceIds( hash_ids_table_name )
            
        
        cached_file_service_ids = set( self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES ) )
        
        file_service_ids_to_hash_ids = collections.defaultdict( set )
        
        for ( hash_id, file_service_ids ) in hash_ids_to_current_file_service_ids.items():
            
            for file_service_id in file_service_ids:
                
                if file_service_id in cached_file_service_ids:
                    
                    file_service_ids_to_hash_ids[ file_service_id ].add( hash_id )
                    
                
            
        
        # ok, we have our map, let's sort it out
        
        # sorting by most comprehensive service_id first
        file_service_ids_to_value = sorted( ( ( file_service_id, len( hash_ids ) ) for ( file_service_id, hash_ids ) in file_service_ids_to_hash_ids.items() ), key = lambda p: p[1], reverse = True )
        
        seen_hash_ids = set()
        
        # make our mapping non-overlapping
        for pair in file_service_ids_to_value:
            
            file_service_id = pair[0]
            
            this_services_hash_ids_set = file_service_ids_to_hash_ids[ file_service_id ]
            
            if len( seen_hash_ids ) > 0:
                
                this_services_hash_ids_set.difference_update( seen_hash_ids )
                
            
            if len( this_services_hash_ids_set ) == 0:
                
                del file_service_ids_to_hash_ids[ file_service_id ]
                
            else:
                
                seen_hash_ids.update( this_services_hash_ids_set )
                
            
        
        unmapped_hash_ids = set( hash_ids ).difference( seen_hash_ids )
        
        if len( unmapped_hash_ids ) > 0:
            
            file_service_ids_to_hash_ids[ self.modules_services.combined_file_service_id ] = unmapped_hash_ids
            
        
        return file_service_ids_to_hash_ids
        
    
    def _ImportFile( self, file_import_job: ClientImportFiles.FileImportJob ):
        
        if HG.file_import_report_mode:
            
            HydrusData.ShowText( 'File import job starting db job' )
            
        
        hash = file_import_job.GetHash()
        
        hash_id = self.modules_hashes_local_cache.GetHashId( hash )
        
        file_import_status = self._GetHashIdStatus( hash_id, prefix = 'file recognised by database' )
        
        if not file_import_status.AlreadyInDB():
            
            if HG.file_import_report_mode:
                
                HydrusData.ShowText( 'File import job adding new file' )
                
            
            ( size, mime, width, height, duration, num_frames, has_audio, num_words ) = file_import_job.GetFileInfo()
            
            if HG.file_import_report_mode:
                
                HydrusData.ShowText( 'File import job adding file info row' )
                
            
            self.modules_files_metadata_basic.AddFilesInfo( [ ( hash_id, size, mime, width, height, duration, num_frames, has_audio, num_words ) ], overwrite = True )
            
            #
            
            perceptual_hashes = file_import_job.GetPerceptualHashes()
            
            if perceptual_hashes is not None:
                
                if HG.file_import_report_mode:
                    
                    HydrusData.ShowText( 'File import job associating perceptual_hashes' )
                    
                
                self.modules_similar_files.AssociatePerceptualHashes( hash_id, perceptual_hashes )
                
            
            if HG.file_import_report_mode:
                
                HydrusData.ShowText( 'File import job adding file to local file service' )
                
            
            #
            
            ( md5, sha1, sha512 ) = file_import_job.GetExtraHashes()
            
            self.modules_hashes.SetExtraHashes( hash_id, md5, sha1, sha512 )
            
            #
            
            self.modules_files_metadata_basic.SetHasICCProfile( hash_id, file_import_job.HasICCProfile() )
            
            #
            
            pixel_hash = file_import_job.GetPixelHash()
            
            if pixel_hash is None:
                
                self.modules_similar_files.ClearPixelHash( hash_id )
                
            else:
                
                pixel_hash_id = self.modules_hashes.GetHashId( pixel_hash )
                
                self.modules_similar_files.SetPixelHash( hash_id, pixel_hash_id )
                
            
            #
            
            file_modified_timestamp = file_import_job.GetFileModifiedTimestamp()
            
            self._Execute( 'REPLACE INTO file_modified_timestamps ( hash_id, file_modified_timestamp ) VALUES ( ?, ? );', ( hash_id, file_modified_timestamp ) )
            
            #
            
            file_info_manager = ClientMediaManagers.FileInfoManager( hash_id, hash, size, mime, width, height, duration, num_frames, has_audio, num_words )
            
            now = HydrusData.GetNow()
            
            for destination_file_service_key in ( CC.LOCAL_FILE_SERVICE_KEY, ): # get this list from FIO, with fallback recovery
                
                destination_service_id = self.modules_services.GetServiceId( destination_file_service_key )
                
                self.modules_files_storage.ClearFileDeletionReason( ( hash_id, ) )
                
                self._AddFiles( destination_service_id, [ ( hash_id, now ) ] )
                
                content_update = HydrusData.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ADD, ( file_info_manager, now ) )
                
                self.pub_content_updates_after_commit( { destination_file_service_key : [ content_update ] } )
                
            
            #
            
            file_import_options = file_import_job.GetFileImportOptions()
            
            if file_import_options.AutomaticallyArchives():
                
                if HG.file_import_report_mode:
                    
                    HydrusData.ShowText( 'File import job archiving new file' )
                    
                
                self._ArchiveFiles( ( hash_id, ) )
                
                content_update = HydrusData.ContentUpdate( HC.CONTENT_TYPE_FILES, HC.CONTENT_UPDATE_ARCHIVE, ( hash, ) )
                
                self.pub_content_updates_after_commit( { CC.COMBINED_LOCAL_FILE_SERVICE_KEY : [ content_update ] } )
                
            else:
                
                if HG.file_import_report_mode:
                    
                    HydrusData.ShowText( 'File import job inboxing new file' )
                    
                
                self._InboxFiles( ( hash_id, ) )
                
            
            #
            
            if self._weakref_media_result_cache.HasFile( hash_id ):
                
                self._weakref_media_result_cache.DropMediaResult( hash_id, hash )
                
                self._controller.pub( 'new_file_info', set( ( hash, ) ) )
                
            
            #
            
            file_import_status = ClientImportFiles.FileImportStatus( CC.STATUS_SUCCESSFUL_AND_NEW, hash, mime = mime )
            
        
        if HG.file_import_report_mode:
            
            HydrusData.ShowText( 'File import job done at db level, final status: {}'.format( file_import_status.ToString() ) )
            
        
        return file_import_status
        
    
    def _ImportUpdate( self, update_network_bytes, update_hash, mime ):
        
        try:
            
            HydrusSerialisable.CreateFromNetworkBytes( update_network_bytes )
            
        except:
            
            HydrusData.ShowText( 'Was unable to parse an incoming update!' )
            
            raise
            
        
        hash_id = self.modules_hashes_local_cache.GetHashId( update_hash )
        
        size = len( update_network_bytes )
        
        width = None
        height = None
        duration = None
        num_frames = None
        has_audio = None
        num_words = None
        
        client_files_manager = self._controller.client_files_manager
        
        client_files_manager.LocklessAddFileFromBytes( update_hash, mime, update_network_bytes )
        
        self.modules_files_metadata_basic.AddFilesInfo( [ ( hash_id, size, mime, width, height, duration, num_frames, has_audio, num_words ) ], overwrite = True )
        
        now = HydrusData.GetNow()
        
        self._AddFiles( self.modules_services.local_update_service_id, [ ( hash_id, now ) ] )
        
    
    def _InboxFiles( self, hash_ids ):
        
        inboxed_hash_ids = self.modules_files_metadata_basic.InboxFiles( hash_ids )
        
        if len( inboxed_hash_ids ) > 0:
            
            service_ids_to_counts = self.modules_files_storage.GetServiceIdCounts( inboxed_hash_ids )
            
            if len( service_ids_to_counts ) > 0:
                
                self._ExecuteMany( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', [ ( count, service_id, HC.SERVICE_INFO_NUM_INBOX ) for ( service_id, count ) in service_ids_to_counts.items() ] )
                
            
        
    
    def _InitCaches( self ):
        
        # this occurs after db update, so is safe to reference things in there but also cannot be relied upon in db update
        
        HG.client_controller.frame_splash_status.SetText( 'preparing db caches' )
        
        HG.client_controller.frame_splash_status.SetSubtext( 'inbox' )
        
    
    def _InitExternalDatabases( self ):
        
        self._db_filenames[ 'external_caches' ] = 'client.caches.db'
        self._db_filenames[ 'external_mappings' ] = 'client.mappings.db'
        self._db_filenames[ 'external_master' ] = 'client.master.db'
        
    
    def _FilterInboxHashes( self, hashes: typing.Collection[ bytes ] ):
        
        hash_ids_to_hashes = self.modules_hashes_local_cache.GetHashIdsToHashes( hashes = hashes )
        
        inbox_hashes = { hash for ( hash_id, hash ) in hash_ids_to_hashes.items() if hash_id in self.modules_files_metadata_basic.inbox_hash_ids }
        
        return inbox_hashes
        
    
    def _IsAnOrphan( self, test_type, possible_hash ):
        
        if self.modules_hashes.HasHash( possible_hash ):
            
            hash = possible_hash
            
            hash_id = self.modules_hashes_local_cache.GetHashId( hash )
            
            if test_type == 'file':
                
                orphan_hash_ids = self.modules_files_storage.FilterOrphanFileHashIds( ( hash_id, ) )
                
                return len( orphan_hash_ids ) == 1
                
            elif test_type == 'thumbnail':
                
                orphan_hash_ids = self.modules_files_storage.FilterOrphanThumbnailHashIds( ( hash_id, ) )
                
                return len( orphan_hash_ids ) == 1
                
            
        else:
            
            return True
            
        
    
    def _LoadModules( self ):
        
        self.modules_db_maintenance = ClientDBMaintenance.ClientDBMaintenance( self._c, self._db_dir, self._db_filenames )
        
        self._modules.append( self.modules_db_maintenance )
        
        self.modules_services = ClientDBServices.ClientDBMasterServices( self._c )
        
        self._modules.append( self.modules_services )
        
        self.modules_hashes = ClientDBMaster.ClientDBMasterHashes( self._c )
        
        self._modules.append( self.modules_hashes )
        
        self.modules_tags = ClientDBMaster.ClientDBMasterTags( self._c )
        
        self._modules.append( self.modules_tags )
        
        self.modules_urls = ClientDBMaster.ClientDBMasterURLs( self._c )
        
        self._modules.append( self.modules_urls )
        
        self.modules_texts = ClientDBMaster.ClientDBMasterTexts( self._c )
        
        self._modules.append( self.modules_texts )
        
        self.modules_serialisable = ClientDBSerialisable.ClientDBSerialisable( self._c, self._db_dir, self._cursor_transaction_wrapper, self.modules_services )
        
        self._modules.append( self.modules_serialisable )
        
        #
        
        self.modules_files_metadata_basic = ClientDBFilesMetadataBasic.ClientDBFilesMetadataBasic( self._c )
        
        self._modules.append( self.modules_files_metadata_basic )
        
        #
        
        self.modules_files_storage = ClientDBFilesStorage.ClientDBFilesStorage( self._c, self._cursor_transaction_wrapper, self.modules_services, self.modules_hashes, self.modules_texts )
        
        self._modules.append( self.modules_files_storage )
        
        #
        
        self.modules_mappings_counts = ClientDBMappingsCounts.ClientDBMappingsCounts( self._c, self.modules_services )
        
        self._modules.append( self.modules_mappings_counts )
        
        #
        
        self.modules_tags_local_cache = ClientDBDefinitionsCache.ClientDBCacheLocalTags( self._c, self.modules_tags, self.modules_services, self.modules_mappings_counts )
        
        self._modules.append( self.modules_tags_local_cache )
        
        self.modules_hashes_local_cache = ClientDBDefinitionsCache.ClientDBCacheLocalHashes( self._c, self.modules_hashes, self.modules_services, self.modules_files_storage )
        
        self._modules.append( self.modules_hashes_local_cache )
        
        #
        
        self.modules_mappings_storage = ClientDBMappingsStorage.ClientDBMappingsStorage( self._c, self.modules_services )
        
        self._modules.append( self.modules_mappings_storage )
        
        #
        
        self.modules_tag_siblings = ClientDBTagSiblings.ClientDBTagSiblings( self._c, self.modules_services, self.modules_tags, self.modules_tags_local_cache )
        
        self._modules.append( self.modules_tag_siblings )
        
        self.modules_tag_parents = ClientDBTagParents.ClientDBTagParents( self._c, self.modules_services, self.modules_tags_local_cache, self.modules_tag_siblings )
        
        self._modules.append( self.modules_tag_parents )
        
        self.modules_tag_display = ClientDBTagDisplay.ClientDBTagDisplay( self._c, self._cursor_transaction_wrapper, self.modules_services, self.modules_tags, self.modules_tags_local_cache, self.modules_tag_siblings, self.modules_tag_parents )
        
        self._modules.append( self.modules_tag_display )
        
        # when you do the mappings caches, storage and display, consider carefully how you want them slotting in here
        # don't rush into it
        
        self.modules_tag_search = ClientDBTagSearch.ClientDBTagSearch( self._c, self.modules_services, self.modules_tags, self.modules_tag_display )
        
        self._modules.append( self.modules_tag_search )
        
        self.modules_mappings_counts_update = ClientDBMappingsCountsUpdate.ClientDBMappingsCountsUpdate( self._c, self.modules_services, self.modules_mappings_counts, self.modules_tags_local_cache, self.modules_tag_display, self.modules_tag_search )
        
        self._modules.append( self.modules_mappings_counts_update )
        
        #
        
        self.modules_mappings_cache_specific_display = ClientDBMappingsCacheSpecificDisplay.ClientDBMappingsCacheSpecificDisplay( self._c, self.modules_services, self.modules_mappings_counts, self.modules_mappings_counts_update, self.modules_mappings_storage, self.modules_tag_display )
        
        #
        
        self.modules_similar_files = ClientDBSimilarFiles.ClientDBSimilarFiles( self._c, self.modules_services, self.modules_files_storage )
        
        self._modules.append( self.modules_similar_files )
        
        self.modules_files_duplicates = ClientDBFilesDuplicates.ClientDBFilesDuplicates( self._c, self.modules_files_storage, self.modules_hashes_local_cache, self.modules_similar_files )
        
        self._modules.append( self.modules_files_duplicates )
        
        #
        
        self.modules_files_maintenance_queue = ClientDBFilesMaintenanceQueue.ClientDBFilesMaintenanceQueue( self._c, self.modules_hashes_local_cache )
        
        self._modules.append( self.modules_files_maintenance_queue )
        
        #
        
        self.modules_repositories = ClientDBRepositories.ClientDBRepositories( self._c, self._cursor_transaction_wrapper, self.modules_services, self.modules_files_storage, self.modules_files_metadata_basic, self.modules_hashes_local_cache, self.modules_tags_local_cache, self.modules_files_maintenance_queue )
        
        self._modules.append( self.modules_repositories )
        
        #
        
        self.modules_files_maintenance = ClientDBFilesMaintenance.ClientDBFilesMaintenance( self._c, self.modules_files_maintenance_queue, self.modules_hashes, self.modules_hashes_local_cache, self.modules_files_metadata_basic, self.modules_similar_files, self.modules_repositories, self._weakref_media_result_cache )
        
        self._modules.append( self.modules_files_maintenance )
        
    
    def _ManageDBError( self, job, e ):
        
        if isinstance( e, MemoryError ):
            
            HydrusData.ShowText( 'The client is running out of memory! Restart it ASAP!' )
            
        
        tb = traceback.format_exc()
        
        if 'malformed' in tb:
            
            HydrusData.ShowText( 'A database exception looked like it could be a very serious \'database image is malformed\' error! Unless you know otherwise, please shut down the client immediately and check the \'help my db is broke.txt\' under install_dir/db.' )
            
        
        if job.IsSynchronous():
            
            db_traceback = 'Database ' + tb
            
            first_line = str( type( e ).__name__ ) + ': ' + str( e )
            
            new_e = HydrusExceptions.DBException( e, first_line, db_traceback )
            
            job.PutResult( new_e )
            
        else:
            
            HydrusData.ShowException( e )
            
        
    
    def _MigrationClearJob( self, database_temp_job_name ):
        
        self._Execute( 'DROP TABLE {};'.format( database_temp_job_name ) )
        
    
    def _MigrationGetMappings( self, database_temp_job_name, file_service_key, tag_service_key, hash_type, tag_filter, content_statuses ):
        
        time_started_precise = HydrusData.GetNowPrecise()
        
        data = []
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        tag_service_id = self.modules_services.GetServiceId( tag_service_key )
        
        statuses_to_table_names = self.modules_mappings_storage.GetFastestStorageMappingTableNames( file_service_id, tag_service_id )
        
        select_queries = []
        
        for content_status in content_statuses:
            
            table_name = statuses_to_table_names[ content_status ]
            
            select_query = 'SELECT tag_id FROM {} WHERE hash_id = ?;'.format( table_name )
            
            select_queries.append( select_query )
            
        
        we_should_stop = False
        
        while not we_should_stop:
            
            result = self._Execute( 'SELECT hash_id FROM {};'.format( database_temp_job_name ) ).fetchone()
            
            if result is None:
                
                break
                
            
            ( hash_id, ) = result
            
            self._Execute( 'DELETE FROM {} WHERE hash_id = ?;'.format( database_temp_job_name ), ( hash_id, ) )
            
            if hash_type == 'sha256':
                
                desired_hash = self.modules_hashes_local_cache.GetHash( hash_id )
                
            else:
                
                try:
                    
                    desired_hash = self.modules_hashes.GetExtraHash( hash_type, hash_id )
                    
                except HydrusExceptions.DataMissing:
                    
                    continue
                    
                
            
            tags = set()
            
            for select_query in select_queries:
                
                tag_ids = self._STL( self._Execute( select_query, ( hash_id, ) ) )
                
                tag_ids_to_tags = self.modules_tags_local_cache.GetTagIdsToTags( tag_ids = tag_ids )
                
                tags.update( tag_ids_to_tags.values() )
                
            
            if not tag_filter.AllowsEverything():
                
                tags = tag_filter.Filter( tags )
                
            
            if len( tags ) > 0:
                
                data.append( ( desired_hash, tags ) )
                
            
            we_should_stop = len( data ) >= 256 or ( len( data ) > 0 and HydrusData.TimeHasPassedPrecise( time_started_precise + 1.0 ) )
            
        
        return data
        
    
    def _MigrationGetPairs( self, database_temp_job_name, left_tag_filter, right_tag_filter ):
        
        time_started_precise = HydrusData.GetNowPrecise()
        
        data = []
        
        we_should_stop = False
        
        while not we_should_stop:
            
            result = self._Execute( 'SELECT left_tag_id, right_tag_id FROM {};'.format( database_temp_job_name ) ).fetchone()
            
            if result is None:
                
                break
                
            
            ( left_tag_id, right_tag_id ) = result
            
            self._Execute( 'DELETE FROM {} WHERE left_tag_id = ? AND right_tag_id = ?;'.format( database_temp_job_name ), ( left_tag_id, right_tag_id ) )
            
            left_tag = self.modules_tags_local_cache.GetTag( left_tag_id )
            
            if not left_tag_filter.TagOK( left_tag ):
                
                continue
                
            
            right_tag = self.modules_tags_local_cache.GetTag( right_tag_id )
            
            if not right_tag_filter.TagOK( right_tag ):
                
                continue
                
            
            data.append( ( left_tag, right_tag ) )
            
            we_should_stop = len( data ) >= 256 or ( len( data ) > 0 and HydrusData.TimeHasPassedPrecise( time_started_precise + 1.0 ) )
            
        
        return data
        
    
    def _MigrationStartMappingsJob( self, database_temp_job_name, file_service_key, tag_service_key, hashes, content_statuses ):
        
        file_service_id = self.modules_services.GetServiceId( file_service_key )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS durable_temp.{} ( hash_id INTEGER PRIMARY KEY );'.format( database_temp_job_name ) )
        
        if hashes is not None:
            
            hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
            
            self._ExecuteMany( 'INSERT INTO {} ( hash_id ) VALUES ( ? );'.format( database_temp_job_name ), ( ( hash_id, ) for hash_id in hash_ids ) )
            
        else:
            
            tag_service_id = self.modules_services.GetServiceId( tag_service_key )
            
            statuses_to_table_names = {}
            
            use_hashes_table = False
            
            if file_service_id == self.modules_services.combined_file_service_id:
                
                # if our tag service is the biggest, and if it basically accounts for all the hashes we know about, it is much faster to just use the hashes table
                
                our_results = self._GetServiceInfo( tag_service_key )
                
                our_num_files = our_results[ HC.SERVICE_INFO_NUM_FILES ]
                
                other_services = [ service for service in self.modules_services.GetServices( HC.REAL_TAG_SERVICES ) if service.GetServiceKey() != tag_service_key ]
                
                other_num_files = []
                
                for other_service in other_services:
                    
                    other_results = self._GetServiceInfo( other_service.GetServiceKey() )
                    
                    other_num_files.append( other_results[ HC.SERVICE_INFO_NUM_FILES ] )
                    
                
                if len( other_num_files ) == 0:
                    
                    we_are_big = True
                    
                else:
                    
                    we_are_big = our_num_files >= 0.75 * max( other_num_files )
                    
                
                if we_are_big:
                    
                    local_files_results = self._GetServiceInfo( CC.COMBINED_LOCAL_FILE_SERVICE_KEY )
                    
                    local_files_num_files = local_files_results[ HC.SERVICE_INFO_NUM_FILES ]
                    
                    if local_files_num_files > our_num_files:
                        
                        # probably a small local tags service, ok to pull from current_mappings
                        
                        we_are_big = False
                        
                    
                
                if we_are_big:
                    
                    use_hashes_table = True
                    
                
            
            if use_hashes_table:
                
                # this obviously just pulls literally all known files
                # makes migration take longer if the tag service does not cover many of these files, but saves huge startup time since it is a simple list
                select_subqueries = [ 'SELECT hash_id FROM hashes' ]
                
            else:
                
                statuses_to_table_names = self.modules_mappings_storage.GetFastestStorageMappingTableNames( file_service_id, tag_service_id )
                
                select_subqueries = []
                
                for content_status in content_statuses:
                    
                    table_name = statuses_to_table_names[ content_status ]
                    
                    select_subquery = 'SELECT DISTINCT hash_id FROM {}'.format( table_name )
                    
                    select_subqueries.append( select_subquery )
                    
                
            
            for select_subquery in select_subqueries:
                
                self._Execute( 'INSERT OR IGNORE INTO {} ( hash_id ) {};'.format( database_temp_job_name, select_subquery ) )
                
            
        
    
    def _MigrationStartPairsJob( self, database_temp_job_name, tag_service_key, content_type, content_statuses ):
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS durable_temp.{} ( left_tag_id INTEGER, right_tag_id INTEGER, PRIMARY KEY ( left_tag_id, right_tag_id ) );'.format( database_temp_job_name ) )
        
        tag_service_id = self.modules_services.GetServiceId( tag_service_key )
        
        if content_type == HC.CONTENT_TYPE_TAG_PARENTS:
            
            source_table_names = [ 'tag_parents', 'tag_parent_petitions' ]
            left_column_name = 'child_tag_id'
            right_column_name = 'parent_tag_id'
            
        elif content_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
            
            source_table_names = [ 'tag_siblings', 'tag_sibling_petitions' ]
            left_column_name = 'bad_tag_id'
            right_column_name = 'good_tag_id'
            
        
        for source_table_name in source_table_names:
            
            self._Execute( 'INSERT OR IGNORE INTO {} ( left_tag_id, right_tag_id ) SELECT {}, {} FROM {} WHERE service_id = ? AND status IN {};'.format( database_temp_job_name, left_column_name, right_column_name, source_table_name, HydrusData.SplayListForDB( content_statuses ) ), ( tag_service_id, ) )
            
        
    
    def _PerceptualHashesResetSearchFromHashes( self, hashes ):
        
        hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
        
        self.modules_similar_files.ResetSearch( hash_ids )
        
    
    def _PerceptualHashesSearchForPotentialDuplicates( self, search_distance, maintenance_mode = HC.MAINTENANCE_FORCED, job_key = None, stop_time = None, work_time_float = None ):
        
        time_started_float = HydrusData.GetNowFloat()
        
        num_done = 0
        still_work_to_do = True
        
        group_of_hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM shape_search_cache WHERE searched_distance IS NULL or searched_distance < ?;', ( search_distance, ) ).fetchmany( 10 ) )
        
        while len( group_of_hash_ids ) > 0:
        
            text = 'searching potential duplicates: {}'.format( HydrusData.ToHumanInt( num_done ) )
            
            HG.client_controller.frame_splash_status.SetSubtext( text )
            
            for ( i, hash_id ) in enumerate( group_of_hash_ids ):
                
                if work_time_float is not None and HydrusData.TimeHasPassedFloat( time_started_float + work_time_float ):
                    
                    return ( still_work_to_do, num_done )
                    
                
                if job_key is not None:
                    
                    ( i_paused, should_stop ) = job_key.WaitIfNeeded()
                    
                    if should_stop:
                        
                        return ( still_work_to_do, num_done )
                        
                    
                
                should_stop = HG.client_controller.ShouldStopThisWork( maintenance_mode, stop_time = stop_time )
                
                if should_stop:
                    
                    return ( still_work_to_do, num_done )
                    
                
                media_id = self.modules_files_duplicates.DuplicatesGetMediaId( hash_id )
                
                potential_duplicate_media_ids_and_distances = [ ( self.modules_files_duplicates.DuplicatesGetMediaId( duplicate_hash_id ), distance ) for ( duplicate_hash_id, distance ) in self.modules_similar_files.Search( hash_id, search_distance ) if duplicate_hash_id != hash_id ]
                
                self.modules_files_duplicates.DuplicatesAddPotentialDuplicates( media_id, potential_duplicate_media_ids_and_distances )
                
                self._Execute( 'UPDATE shape_search_cache SET searched_distance = ? WHERE hash_id = ?;', ( search_distance, hash_id ) )
                
                num_done += 1
                
            
            group_of_hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM shape_search_cache WHERE searched_distance IS NULL or searched_distance < ?;', ( search_distance, ) ).fetchmany( 10 ) )
            
        
        still_work_to_do = False
        
        return ( still_work_to_do, num_done )
        
    
    def _ProcessContentUpdates( self, service_keys_to_content_updates, publish_content_updates = True ):
        
        notify_new_downloads = False
        notify_new_pending = False
        notify_new_parents = False
        notify_new_siblings = False
        
        valid_service_keys_to_content_updates = {}
        
        for ( service_key, content_updates ) in service_keys_to_content_updates.items():
            
            try:
                
                service_id = self.modules_services.GetServiceId( service_key )
                
            except HydrusExceptions.DataMissing:
                
                continue
                
            
            valid_service_keys_to_content_updates[ service_key ] = content_updates
            
            service = self.modules_services.GetService( service_id )
            
            service_type = service.GetServiceType()
            
            ultimate_mappings_ids = []
            ultimate_deleted_mappings_ids = []
            
            ultimate_pending_mappings_ids = []
            ultimate_pending_rescinded_mappings_ids = []
            
            ultimate_petitioned_mappings_ids = []
            ultimate_petitioned_rescinded_mappings_ids = []
            
            changed_sibling_tag_ids = set()
            changed_parent_tag_ids = set()
            
            for content_update in content_updates:
                
                ( data_type, action, row ) = content_update.ToTuple()
                
                if service_type in HC.FILE_SERVICES:
                    
                    if data_type == HC.CONTENT_TYPE_FILES:
                        
                        if action == HC.CONTENT_UPDATE_ADVANCED:
                            
                            ( sub_action, sub_row ) = row
                            
                            if sub_action == 'delete_deleted':
                                
                                hashes = sub_row
                                
                                if hashes is None:
                                    
                                    service_ids_to_nums_cleared = self.modules_files_storage.ClearLocalDeleteRecord()
                                    
                                else:
                                    
                                    hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                                    
                                    service_ids_to_nums_cleared = self.modules_files_storage.ClearLocalDeleteRecord( hash_ids )
                                    
                                
                                self._ExecuteMany( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', ( ( -num_cleared, clear_service_id, HC.SERVICE_INFO_NUM_DELETED_FILES ) for ( clear_service_id, num_cleared ) in service_ids_to_nums_cleared.items() ) )
                                
                            
                        elif action == HC.CONTENT_UPDATE_ADD:
                            
                            if service_type in HC.LOCAL_FILE_SERVICES or service_type == HC.FILE_REPOSITORY:
                                
                                ( file_info_manager, timestamp ) = row
                                
                                ( hash_id, hash, size, mime, width, height, duration, num_frames, has_audio, num_words ) = file_info_manager.ToTuple()
                                
                                self.modules_files_metadata_basic.AddFilesInfo( [ ( hash_id, size, mime, width, height, duration, num_frames, has_audio, num_words ) ] )
                                
                            elif service_type == HC.IPFS:
                                
                                ( file_info_manager, multihash ) = row
                                
                                hash_id = file_info_manager.hash_id
                                
                                self._SetServiceFilename( service_id, hash_id, multihash )
                                
                                timestamp = HydrusData.GetNow()
                                
                            
                            self._AddFiles( service_id, [ ( hash_id, timestamp ) ] )
                            
                        else:
                            
                            hashes = row
                            
                            hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                            
                            if action == HC.CONTENT_UPDATE_ARCHIVE:
                                
                                self._ArchiveFiles( hash_ids )
                                
                            elif action == HC.CONTENT_UPDATE_INBOX:
                                
                                self._InboxFiles( hash_ids )
                                
                            elif action == HC.CONTENT_UPDATE_DELETE:
                                
                                actual_delete_hash_ids = self._FilterForFileDeleteLock( service_id, hash_ids )
                                
                                if len( actual_delete_hash_ids ) < len( hash_ids ):
                                    
                                    hash_ids = actual_delete_hash_ids
                                    
                                    hashes = self.modules_hashes_local_cache.GetHashes( hash_ids )
                                    
                                    content_update.SetRow( hashes )
                                    
                                
                                if service_type in ( HC.LOCAL_FILE_DOMAIN, HC.COMBINED_LOCAL_FILE ):
                                    
                                    if content_update.HasReason():
                                        
                                        reason = content_update.GetReason()
                                        
                                        # at the moment, we only set a deletion reason when a file leaves a real file domain. not on second delete from trash, so if file in trash, no new delete reason will be set
                                        
                                        location_context = ClientLocation.LocationContext( current_service_keys = ( service_key, ) )
                                        
                                        reason_setting_hash_ids = self.modules_files_storage.FilterHashIds( location_context, hash_ids )
                                        
                                        self.modules_files_storage.SetFileDeletionReason( reason_setting_hash_ids, reason )
                                        
                                    
                                
                                if service_id == self.modules_services.trash_service_id:
                                    
                                    # shouldn't be called anymore, but just in case someone fidgets a trash delete with client api or something
                                    
                                    self._DeleteFiles( self.modules_services.combined_local_file_service_id, hash_ids )
                                    
                                else:
                                    
                                    self._DeleteFiles( service_id, hash_ids )
                                    
                                
                            elif action == HC.CONTENT_UPDATE_UNDELETE:
                                
                                self.modules_files_storage.ClearFileDeletionReason( hash_ids )
                                
                                self._UndeleteFiles( service_id, hash_ids )
                                
                            elif action == HC.CONTENT_UPDATE_PEND:
                                
                                invalid_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( service_id, hash_ids, HC.CONTENT_STATUS_CURRENT )
                                
                                valid_hash_ids = hash_ids.difference( invalid_hash_ids )
                                
                                self.modules_files_storage.PendFiles( service_id, valid_hash_ids )
                                
                                if service_key == CC.COMBINED_LOCAL_FILE_SERVICE_KEY:
                                    
                                    notify_new_downloads = True
                                    
                                else:
                                    
                                    notify_new_pending = True
                                    
                                
                            elif action == HC.CONTENT_UPDATE_PETITION:
                                
                                reason = content_update.GetReason()
                                
                                reason_id = self.modules_texts.GetTextId( reason )
                                
                                valid_hash_ids = self.modules_files_storage.FilterHashIdsToStatus( service_id, hash_ids, HC.CONTENT_STATUS_CURRENT )
                                
                                self.modules_files_storage.PetitionFiles( service_id, reason_id, valid_hash_ids )
                                
                                notify_new_pending = True
                                
                            elif action == HC.CONTENT_UPDATE_RESCIND_PEND:
                                
                                self.modules_files_storage.RescindPendFiles( service_id, hash_ids )
                                
                                if service_key == CC.COMBINED_LOCAL_FILE_SERVICE_KEY:
                                    
                                    notify_new_downloads = True
                                    
                                else:
                                    
                                    notify_new_pending = True
                                    
                                
                            elif action == HC.CONTENT_UPDATE_RESCIND_PETITION:
                                
                                self.modules_files_storage.RescindPetitionFiles( service_id, hash_ids )
                                
                                notify_new_pending = True
                                
                            
                        
                    elif data_type == HC.CONTENT_TYPE_DIRECTORIES:
                        
                        if action == HC.CONTENT_UPDATE_ADD:
                            
                            ( hashes, dirname, note ) = row
                            
                            hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                            
                            self._SetServiceDirectory( service_id, hash_ids, dirname, note )
                            
                        elif action == HC.CONTENT_UPDATE_DELETE:
                            
                            dirname = row
                            
                            self._DeleteServiceDirectory( service_id, dirname )
                            
                        
                    elif data_type == HC.CONTENT_TYPE_URLS:
                        
                        if action == HC.CONTENT_UPDATE_ADD:
                            
                            ( urls, hashes ) = row
                            
                            url_ids = { self.modules_urls.GetURLId( url ) for url in urls }
                            hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                            
                            self._ExecuteMany( 'INSERT OR IGNORE INTO url_map ( hash_id, url_id ) VALUES ( ?, ? );', itertools.product( hash_ids, url_ids ) )
                            
                        elif action == HC.CONTENT_UPDATE_DELETE:
                            
                            ( urls, hashes ) = row
                            
                            url_ids = { self.modules_urls.GetURLId( url ) for url in urls }
                            hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                            
                            self._ExecuteMany( 'DELETE FROM url_map WHERE hash_id = ? AND url_id = ?;', itertools.product( hash_ids, url_ids ) )
                            
                        
                    elif data_type == HC.CONTENT_TYPE_FILE_VIEWING_STATS:
                        
                        if action == HC.CONTENT_UPDATE_ADVANCED:
                            
                            action = row
                            
                            if action == 'clear':
                                
                                self._Execute( 'DELETE FROM file_viewing_stats;' )
                                
                            
                        elif action == HC.CONTENT_UPDATE_ADD:
                            
                            ( hash, canvas_type, view_timestamp, views_delta, viewtime_delta ) = row
                            
                            hash_id = self.modules_hashes_local_cache.GetHashId( hash )
                            
                            self._Execute( 'INSERT OR IGNORE INTO file_viewing_stats ( hash_id, canvas_type, last_viewed_timestamp, views, viewtime ) VALUES ( ?, ?, ?, ?, ? );', ( hash_id, canvas_type, 0, 0, 0 ) )
                            
                            self._Execute( 'UPDATE file_viewing_stats SET last_viewed_timestamp = ?, views = views + ?, viewtime = viewtime + ? WHERE hash_id = ? AND canvas_type = ?;', ( view_timestamp, views_delta, viewtime_delta, hash_id, canvas_type ) )
                            
                        elif action == HC.CONTENT_UPDATE_DELETE:
                            
                            hashes = row
                            
                            hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                            
                            self._ExecuteMany( 'DELETE FROM file_viewing_stats WHERE hash_id = ?;', ( ( hash_id, ) for hash_id in hash_ids ) )
                            
                        
                    
                elif service_type in HC.REAL_TAG_SERVICES:
                    
                    if data_type == HC.CONTENT_TYPE_MAPPINGS:
                        
                        ( tag, hashes ) = row
                        
                        try:
                            
                            tag_id = self.modules_tags.GetTagId( tag )
                            
                        except HydrusExceptions.TagSizeException:
                            
                            continue
                            
                        
                        hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                        
                        display_affected = action in ( HC.CONTENT_UPDATE_ADD, HC.CONTENT_UPDATE_DELETE, HC.CONTENT_UPDATE_PEND, HC.CONTENT_UPDATE_RESCIND_PEND )
                        
                        if display_affected and publish_content_updates and self.modules_tag_display.IsChained( ClientTags.TAG_DISPLAY_ACTUAL, service_id, tag_id ):
                            
                            self._regen_tags_managers_hash_ids.update( hash_ids )
                            
                        
                        if action == HC.CONTENT_UPDATE_ADD:
                            
                            if not HG.client_controller.tag_display_manager.TagOK( ClientTags.TAG_DISPLAY_STORAGE, service_key, tag ):
                                
                                continue
                                
                            
                            ultimate_mappings_ids.append( ( tag_id, hash_ids ) )
                            
                        elif action == HC.CONTENT_UPDATE_DELETE:
                            
                            ultimate_deleted_mappings_ids.append( ( tag_id, hash_ids ) )
                            
                        elif action == HC.CONTENT_UPDATE_PEND:
                            
                            if not HG.client_controller.tag_display_manager.TagOK( ClientTags.TAG_DISPLAY_STORAGE, service_key, tag ):
                                
                                continue
                                
                            
                            ultimate_pending_mappings_ids.append( ( tag_id, hash_ids ) )
                            
                        elif action == HC.CONTENT_UPDATE_RESCIND_PEND:
                            
                            ultimate_pending_rescinded_mappings_ids.append( ( tag_id, hash_ids ) )
                            
                        elif action == HC.CONTENT_UPDATE_PETITION:
                            
                            reason = content_update.GetReason()
                            
                            reason_id = self.modules_texts.GetTextId( reason )
                            
                            ultimate_petitioned_mappings_ids.append( ( tag_id, hash_ids, reason_id ) )
                            
                        elif action == HC.CONTENT_UPDATE_RESCIND_PETITION:
                            
                            ultimate_petitioned_rescinded_mappings_ids.append( ( tag_id, hash_ids ) )
                            
                        elif action == HC.CONTENT_UPDATE_CLEAR_DELETE_RECORD:
                            
                            ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( service_id )
                            
                            self._ExecuteMany( 'DELETE FROM {} WHERE tag_id = ? AND hash_id = ?;'.format( deleted_mappings_table_name ), ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                            
                            self._Execute( 'DELETE FROM service_info WHERE service_id = ? AND info_type = ?;', ( service_id, HC.SERVICE_INFO_NUM_DELETED_MAPPINGS ) )
                            
                            cache_file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
                            
                            for cache_file_service_id in cache_file_service_ids:
                                
                                ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( cache_file_service_id, service_id )
                                
                                self._ExecuteMany( 'DELETE FROM ' + cache_deleted_mappings_table_name + ' WHERE hash_id = ? AND tag_id = ?;', ( ( hash_id, tag_id ) for hash_id in hash_ids ) )
                                
                            
                        
                    elif data_type == HC.CONTENT_TYPE_TAG_PARENTS:
                        
                        if action in ( HC.CONTENT_UPDATE_ADD, HC.CONTENT_UPDATE_DELETE ):
                            
                            ( child_tag, parent_tag ) = row
                            
                            try:
                                
                                child_tag_id = self.modules_tags.GetTagId( child_tag )
                                
                                parent_tag_id = self.modules_tags.GetTagId( parent_tag )
                                
                            except HydrusExceptions.TagSizeException:
                                
                                continue
                                
                            
                            pairs = ( ( child_tag_id, parent_tag_id ), )
                            
                            if action == HC.CONTENT_UPDATE_ADD:
                                
                                self.modules_tag_parents.AddTagParents( service_id, pairs )
                                
                            elif action == HC.CONTENT_UPDATE_DELETE:
                                
                                self.modules_tag_parents.DeleteTagParents( service_id, pairs )
                                
                            
                            changed_parent_tag_ids.update( ( child_tag_id, parent_tag_id ) )
                            
                            if service_type == HC.TAG_REPOSITORY:
                                
                                notify_new_pending = True
                                
                            
                        elif action in ( HC.CONTENT_UPDATE_PEND, HC.CONTENT_UPDATE_PETITION ):
                            
                            ( child_tag, parent_tag ) = row
                            
                            try:
                                
                                child_tag_id = self.modules_tags.GetTagId( child_tag )
                                
                                parent_tag_id = self.modules_tags.GetTagId( parent_tag )
                                
                            except HydrusExceptions.TagSizeException:
                                
                                continue
                                
                            
                            reason = content_update.GetReason()
                            
                            reason_id = self.modules_texts.GetTextId( reason )
                            
                            triples = ( ( child_tag_id, parent_tag_id, reason_id ), )
                            
                            if action == HC.CONTENT_UPDATE_PEND:
                                
                                self.modules_tag_parents.PendTagParents( service_id, triples )
                                
                            elif action == HC.CONTENT_UPDATE_PETITION:
                                
                                self.modules_tag_parents.PetitionTagParents( service_id, triples )
                                
                            
                            changed_parent_tag_ids.update( ( child_tag_id, parent_tag_id ) )
                            
                            if service_type == HC.TAG_REPOSITORY:
                                
                                notify_new_pending = True
                                
                            
                        elif action in ( HC.CONTENT_UPDATE_RESCIND_PEND, HC.CONTENT_UPDATE_RESCIND_PETITION ):
                            
                            ( child_tag, parent_tag ) = row
                            
                            try:
                                
                                child_tag_id = self.modules_tags.GetTagId( child_tag )
                                
                                parent_tag_id = self.modules_tags.GetTagId( parent_tag )
                                
                            except HydrusExceptions.TagSizeException:
                                
                                continue
                                
                            
                            pairs = ( ( child_tag_id, parent_tag_id ), )
                            
                            if action == HC.CONTENT_UPDATE_RESCIND_PEND:
                                
                                self.modules_tag_parents.RescindPendingTagParents( service_id, pairs )
                                
                            elif action == HC.CONTENT_UPDATE_RESCIND_PETITION:
                                
                                self.modules_tag_parents.RescindPetitionedTagParents( service_id, pairs )
                                
                            
                            changed_parent_tag_ids.update( ( child_tag_id, parent_tag_id ) )
                            
                            if service_type == HC.TAG_REPOSITORY:
                                
                                notify_new_pending = True
                                
                            
                        
                        notify_new_parents = True
                        
                    elif data_type == HC.CONTENT_TYPE_TAG_SIBLINGS:
                        
                        if action in ( HC.CONTENT_UPDATE_ADD, HC.CONTENT_UPDATE_DELETE ):
                            
                            ( bad_tag, good_tag ) = row
                            
                            try:
                                
                                bad_tag_id = self.modules_tags.GetTagId( bad_tag )
                                
                                good_tag_id = self.modules_tags.GetTagId( good_tag )
                                
                            except HydrusExceptions.TagSizeException:
                                
                                continue
                                
                            
                            pairs = ( ( bad_tag_id, good_tag_id ), )
                            
                            if action == HC.CONTENT_UPDATE_ADD:
                                
                                self.modules_tag_siblings.AddTagSiblings( service_id, pairs )
                                
                            elif action == HC.CONTENT_UPDATE_DELETE:
                                
                                self.modules_tag_siblings.DeleteTagSiblings( service_id, pairs )
                                
                            
                            changed_sibling_tag_ids.update( ( bad_tag_id, good_tag_id ) )
                            
                            if service_type == HC.TAG_REPOSITORY:
                                
                                notify_new_pending = True
                                
                            
                        elif action in ( HC.CONTENT_UPDATE_PEND, HC.CONTENT_UPDATE_PETITION ):
                            
                            ( bad_tag, good_tag ) = row
                            
                            try:
                                
                                bad_tag_id = self.modules_tags.GetTagId( bad_tag )
                                
                                good_tag_id = self.modules_tags.GetTagId( good_tag )
                                
                            except HydrusExceptions.TagSizeException:
                                
                                continue
                                
                            
                            reason = content_update.GetReason()
                            
                            reason_id = self.modules_texts.GetTextId( reason )
                            
                            triples = ( ( bad_tag_id, good_tag_id, reason_id ), )
                            
                            if action == HC.CONTENT_UPDATE_PEND:
                                
                                self.modules_tag_siblings.PendTagSiblings( service_id, triples )
                                
                            elif action == HC.CONTENT_UPDATE_PETITION:
                                
                                self.modules_tag_siblings.PetitionTagSiblings( service_id, triples )
                                
                            
                            changed_sibling_tag_ids.update( ( bad_tag_id, good_tag_id ) )
                            
                            if service_type == HC.TAG_REPOSITORY:
                                
                                notify_new_pending = True
                                
                            
                        elif action in ( HC.CONTENT_UPDATE_RESCIND_PEND, HC.CONTENT_UPDATE_RESCIND_PETITION ):
                            
                            ( bad_tag, good_tag ) = row
                            
                            try:
                                
                                bad_tag_id = self.modules_tags.GetTagId( bad_tag )
                                
                                good_tag_id = self.modules_tags.GetTagId( good_tag )
                                
                            except HydrusExceptions.TagSizeException:
                                
                                continue
                                
                            
                            pairs = ( ( bad_tag_id, good_tag_id ), )
                            
                            if action == HC.CONTENT_UPDATE_RESCIND_PEND:
                                
                                self.modules_tag_siblings.RescindPendingTagSiblings( service_id, pairs )
                                
                            elif action == HC.CONTENT_UPDATE_RESCIND_PETITION:
                                
                                self.modules_tag_siblings.RescindPetitionedTagSiblings( service_id, pairs )
                                
                            
                            changed_sibling_tag_ids.update( ( bad_tag_id, good_tag_id ) )
                            
                            if service_type == HC.TAG_REPOSITORY:
                                
                                notify_new_pending = True
                                
                            
                        
                        notify_new_siblings = True
                        
                    
                elif service_type in HC.RATINGS_SERVICES:
                    
                    if action == HC.CONTENT_UPDATE_ADD:
                        
                        ( rating, hashes ) = row
                        
                        hash_ids = self.modules_hashes_local_cache.GetHashIds( hashes )
                        
                        splayed_hash_ids = HydrusData.SplayListForDB( hash_ids )
                        
                        if service_type in ( HC.LOCAL_RATING_LIKE, HC.LOCAL_RATING_NUMERICAL ):
                            
                            ratings_added = 0
                            
                            self._ExecuteMany( 'DELETE FROM local_ratings WHERE service_id = ? AND hash_id = ?;', ( ( service_id, hash_id ) for hash_id in hash_ids ) )
                            
                            ratings_added -= self._GetRowCount()
                            
                            if rating is not None:
                                
                                self._ExecuteMany( 'INSERT INTO local_ratings ( service_id, hash_id, rating ) VALUES ( ?, ?, ? );', [ ( service_id, hash_id, rating ) for hash_id in hash_ids ] )
                                
                                ratings_added += self._GetRowCount()
                                
                            
                            self._Execute( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', ( ratings_added, service_id, HC.SERVICE_INFO_NUM_FILES ) )
                            
                        
                    elif action == HC.CONTENT_UPDATE_ADVANCED:
                        
                        action = row
                        
                        if action == 'delete_for_deleted_files':
                            
                            deleted_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_DELETED )
                            
                            self._Execute( 'DELETE FROM local_ratings WHERE service_id = ? and hash_id IN ( SELECT hash_id FROM {} );'.format( deleted_files_table_name ), ( service_id, ) )
                            
                            ratings_deleted = self._GetRowCount()
                            
                            self._Execute( 'UPDATE service_info SET info = info - ? WHERE service_id = ? AND info_type = ?;', ( ratings_deleted, service_id, HC.SERVICE_INFO_NUM_FILES ) )
                            
                        elif action == 'delete_for_non_local_files':
                            
                            current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_CURRENT )
                            
                            self._Execute( 'DELETE FROM local_ratings WHERE local_ratings.service_id = ? and hash_id NOT IN ( SELECT hash_id FROM {} );'.format( current_files_table_name ), ( service_id, ) )
                            
                            ratings_deleted = self._GetRowCount()
                            
                            self._Execute( 'UPDATE service_info SET info = info - ? WHERE service_id = ? AND info_type = ?;', ( ratings_deleted, service_id, HC.SERVICE_INFO_NUM_FILES ) )
                            
                        elif action == 'delete_for_all_files':
                            
                            self._Execute( 'DELETE FROM local_ratings WHERE service_id = ?;', ( service_id, ) )
                            
                            self._Execute( 'UPDATE service_info SET info = ? WHERE service_id = ? AND info_type = ?;', ( 0, service_id, HC.SERVICE_INFO_NUM_FILES ) )
                            
                        
                    
                elif service_type == HC.LOCAL_NOTES:
                    
                    if action == HC.CONTENT_UPDATE_SET:
                        
                        ( hash, name, note ) = row
                        
                        hash_id = self.modules_hashes_local_cache.GetHashId( hash )
                        name_id = self.modules_texts.GetLabelId( name )
                        
                        self._Execute( 'DELETE FROM file_notes WHERE hash_id = ? AND name_id = ?;', ( hash_id, name_id ) )
                        
                        if len( note ) > 0:
                            
                            note_id = self.modules_texts.GetNoteId( note )
                            
                            self._Execute( 'INSERT OR IGNORE INTO file_notes ( hash_id, name_id, note_id ) VALUES ( ?, ?, ? );', ( hash_id, name_id, note_id ) )
                            
                        
                    elif action == HC.CONTENT_UPDATE_DELETE:
                        
                        ( hash, name ) = row
                        
                        hash_id = self.modules_hashes_local_cache.GetHashId( hash )
                        name_id = self.modules_texts.GetLabelId( name )
                        
                        self._Execute( 'DELETE FROM file_notes WHERE hash_id = ? AND name_id = ?;', ( hash_id, name_id ) )
                        
                    
                
            
            if len( ultimate_mappings_ids ) + len( ultimate_deleted_mappings_ids ) + len( ultimate_pending_mappings_ids ) + len( ultimate_pending_rescinded_mappings_ids ) + len( ultimate_petitioned_mappings_ids ) + len( ultimate_petitioned_rescinded_mappings_ids ) > 0:
                
                self._UpdateMappings( service_id, mappings_ids = ultimate_mappings_ids, deleted_mappings_ids = ultimate_deleted_mappings_ids, pending_mappings_ids = ultimate_pending_mappings_ids, pending_rescinded_mappings_ids = ultimate_pending_rescinded_mappings_ids, petitioned_mappings_ids = ultimate_petitioned_mappings_ids, petitioned_rescinded_mappings_ids = ultimate_petitioned_rescinded_mappings_ids )
                
                if service_type == HC.TAG_REPOSITORY:
                    
                    notify_new_pending = True
                    
                
            
            if len( changed_sibling_tag_ids ) > 0:
                
                self.modules_tag_display.NotifySiblingsChanged( service_id, changed_sibling_tag_ids )
                
            
            if len( changed_parent_tag_ids ) > 0:
                
                self.modules_tag_display.NotifyParentsChanged( service_id, changed_parent_tag_ids )
                
            
        
        if publish_content_updates:
            
            if notify_new_pending:
                
                self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
                
            if notify_new_downloads:
                
                self._cursor_transaction_wrapper.pub_after_job( 'notify_new_downloads' )
                
            if notify_new_siblings or notify_new_parents:
                
                self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
                
            
            self.pub_content_updates_after_commit( valid_service_keys_to_content_updates )
            
        
    
    def _ProcessRepositoryContent( self, service_key, content_hash, content_iterator_dict, content_types_to_process, job_key, work_time ):
        
        FILES_INITIAL_CHUNK_SIZE = 20
        MAPPINGS_INITIAL_CHUNK_SIZE = 50
        PAIR_ROWS_INITIAL_CHUNK_SIZE = 100
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        precise_time_to_stop = HydrusData.GetNowPrecise() + work_time
        
        num_rows_processed = 0
        
        if HC.CONTENT_TYPE_FILES in content_types_to_process:
            
            if 'new_files' in content_iterator_dict:
                
                has_audio = None # hack until we figure this out better
                
                i = content_iterator_dict[ 'new_files' ]
                
                for chunk in HydrusData.SplitIteratorIntoAutothrottledChunks( i, FILES_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                    
                    files_info_rows = []
                    files_rows = []
                    
                    for ( service_hash_id, size, mime, timestamp, width, height, duration, num_frames, num_words ) in chunk:
                        
                        hash_id = self.modules_repositories.NormaliseServiceHashId( service_id, service_hash_id )
                        
                        files_info_rows.append( ( hash_id, size, mime, width, height, duration, num_frames, has_audio, num_words ) )
                        
                        files_rows.append( ( hash_id, timestamp ) )
                        
                    
                    self.modules_files_metadata_basic.AddFilesInfo( files_info_rows )
                    
                    self._AddFiles( service_id, files_rows )
                    
                    num_rows_processed += len( files_rows )
                    
                    if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                        
                        return num_rows_processed
                        
                    
                
                del content_iterator_dict[ 'new_files' ]
                
            
            #
            
            if 'deleted_files' in content_iterator_dict:
                
                i = content_iterator_dict[ 'deleted_files' ]
                
                for chunk in HydrusData.SplitIteratorIntoAutothrottledChunks( i, FILES_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                    
                    service_hash_ids = chunk
                    
                    hash_ids = self.modules_repositories.NormaliseServiceHashIds( service_id, service_hash_ids )
                    
                    self._DeleteFiles( service_id, hash_ids )
                    
                    num_rows_processed += len( hash_ids )
                    
                    if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                        
                        return num_rows_processed
                        
                    
                
                del content_iterator_dict[ 'deleted_files' ]
                
            
        
        #
        
        if HC.CONTENT_TYPE_MAPPINGS in content_types_to_process:
            
            if 'new_mappings' in content_iterator_dict:
                
                i = content_iterator_dict[ 'new_mappings' ]
                
                for chunk in HydrusData.SplitMappingIteratorIntoAutothrottledChunks( i, MAPPINGS_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                    
                    mappings_ids = []
                    
                    num_rows = 0
                    
                    # yo, I can save time if I merge these ids so we only have one round of normalisation
                    
                    for ( service_tag_id, service_hash_ids ) in chunk:
                        
                        tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_tag_id )
                        hash_ids = self.modules_repositories.NormaliseServiceHashIds( service_id, service_hash_ids )
                        
                        mappings_ids.append( ( tag_id, hash_ids ) )
                        
                        num_rows += len( service_hash_ids )
                        
                    
                    self._UpdateMappings( service_id, mappings_ids = mappings_ids )
                    
                    num_rows_processed += num_rows
                    
                    if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                        
                        return num_rows_processed
                        
                    
                
                del content_iterator_dict[ 'new_mappings' ]
                
            
            #
            
            if 'deleted_mappings' in content_iterator_dict:
                
                i = content_iterator_dict[ 'deleted_mappings' ]
                
                for chunk in HydrusData.SplitMappingIteratorIntoAutothrottledChunks( i, MAPPINGS_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                    
                    deleted_mappings_ids = []
                    
                    num_rows = 0
                    
                    for ( service_tag_id, service_hash_ids ) in chunk:
                        
                        tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_tag_id )
                        hash_ids = self.modules_repositories.NormaliseServiceHashIds( service_id, service_hash_ids )
                        
                        deleted_mappings_ids.append( ( tag_id, hash_ids ) )
                        
                        num_rows += len( service_hash_ids )
                        
                    
                    self._UpdateMappings( service_id, deleted_mappings_ids = deleted_mappings_ids )
                    
                    num_rows_processed += num_rows
                    
                    if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                        
                        return num_rows_processed
                        
                    
                
                del content_iterator_dict[ 'deleted_mappings' ]
                
            
        
        #
        
        parents_or_siblings_changed = False
        
        try:
            
            if HC.CONTENT_TYPE_TAG_PARENTS in content_types_to_process:
                
                if 'new_parents' in content_iterator_dict:
                    
                    i = content_iterator_dict[ 'new_parents' ]
                    
                    for chunk in HydrusData.SplitIteratorIntoAutothrottledChunks( i, PAIR_ROWS_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                        
                        parent_ids = []
                        tag_ids = set()
                        
                        for ( service_child_tag_id, service_parent_tag_id ) in chunk:
                            
                            child_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_child_tag_id )
                            parent_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_parent_tag_id )
                            
                            tag_ids.add( child_tag_id )
                            tag_ids.add( parent_tag_id )
                            
                            parent_ids.append( ( child_tag_id, parent_tag_id ) )
                            
                        
                        self.modules_tag_parents.AddTagParents( service_id, parent_ids )
                        
                        self.modules_tag_display.NotifyParentsChanged( service_id, tag_ids )
                        
                        parents_or_siblings_changed = True
                        
                        num_rows_processed += len( parent_ids )
                        
                        if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                            
                            return num_rows_processed
                            
                        
                    
                    del content_iterator_dict[ 'new_parents' ]
                    
                
                #
                
                if 'deleted_parents' in content_iterator_dict:
                    
                    i = content_iterator_dict[ 'deleted_parents' ]
                    
                    for chunk in HydrusData.SplitIteratorIntoAutothrottledChunks( i, PAIR_ROWS_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                        
                        parent_ids = []
                        tag_ids = set()
                        
                        for ( service_child_tag_id, service_parent_tag_id ) in chunk:
                            
                            child_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_child_tag_id )
                            parent_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_parent_tag_id )
                            
                            tag_ids.add( child_tag_id )
                            tag_ids.add( parent_tag_id )
                            
                            parent_ids.append( ( child_tag_id, parent_tag_id ) )
                            
                        
                        self.modules_tag_parents.DeleteTagParents( service_id, parent_ids )
                        
                        self.modules_tag_display.NotifyParentsChanged( service_id, tag_ids )
                        
                        parents_or_siblings_changed = True
                        
                        num_rows = len( parent_ids )
                        
                        num_rows_processed += num_rows
                        
                        if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                            
                            return num_rows_processed
                            
                        
                    
                    del content_iterator_dict[ 'deleted_parents' ]
                    
                
            
            #
            
            if HC.CONTENT_TYPE_TAG_SIBLINGS in content_types_to_process:
                
                if 'new_siblings' in content_iterator_dict:
                    
                    i = content_iterator_dict[ 'new_siblings' ]
                    
                    for chunk in HydrusData.SplitIteratorIntoAutothrottledChunks( i, PAIR_ROWS_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                        
                        sibling_ids = []
                        tag_ids = set()
                        
                        for ( service_bad_tag_id, service_good_tag_id ) in chunk:
                            
                            bad_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_bad_tag_id )
                            good_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_good_tag_id )
                            
                            tag_ids.add( bad_tag_id )
                            tag_ids.add( good_tag_id )
                            
                            sibling_ids.append( ( bad_tag_id, good_tag_id ) )
                            
                        
                        self.modules_tag_siblings.AddTagSiblings( service_id, sibling_ids )
                        
                        self.modules_tag_display.NotifySiblingsChanged( service_id, tag_ids )
                        
                        parents_or_siblings_changed = True
                        
                        num_rows = len( sibling_ids )
                        
                        num_rows_processed += num_rows
                        
                        if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                            
                            return num_rows_processed
                            
                        
                    
                    del content_iterator_dict[ 'new_siblings' ]
                    
                
                #
                
                if 'deleted_siblings' in content_iterator_dict:
                    
                    i = content_iterator_dict[ 'deleted_siblings' ]
                    
                    for chunk in HydrusData.SplitIteratorIntoAutothrottledChunks( i, PAIR_ROWS_INITIAL_CHUNK_SIZE, precise_time_to_stop ):
                        
                        sibling_ids = []
                        tag_ids = set()
                        
                        for ( service_bad_tag_id, service_good_tag_id ) in chunk:
                            
                            bad_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_bad_tag_id )
                            good_tag_id = self.modules_repositories.NormaliseServiceTagId( service_id, service_good_tag_id )
                            
                            tag_ids.add( bad_tag_id )
                            tag_ids.add( good_tag_id )
                            
                            sibling_ids.append( ( bad_tag_id, good_tag_id ) )
                            
                        
                        self.modules_tag_siblings.DeleteTagSiblings( service_id, sibling_ids )
                        
                        self.modules_tag_display.NotifySiblingsChanged( service_id, tag_ids )
                        
                        parents_or_siblings_changed = True
                        
                        num_rows_processed += len( sibling_ids )
                        
                        if HydrusData.TimeHasPassedPrecise( precise_time_to_stop ) or job_key.IsCancelled():
                            
                            return num_rows_processed
                            
                        
                    
                    del content_iterator_dict[ 'deleted_siblings' ]
                    
                
            
        finally:
            
            if parents_or_siblings_changed:
                
                self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
                
            
        
        self.modules_repositories.SetUpdateProcessed( service_id, content_hash, content_types_to_process )
        
        return num_rows_processed
        
    
    def _PushRecentTags( self, service_key, tags ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        if tags is None:
            
            self._Execute( 'DELETE FROM recent_tags WHERE service_id = ?;', ( service_id, ) )
            
        else:
            
            now = HydrusData.GetNow()
            
            tag_ids = [ self.modules_tags.GetTagId( tag ) for tag in tags ]
            
            self._ExecuteMany( 'REPLACE INTO recent_tags ( service_id, tag_id, timestamp ) VALUES ( ?, ?, ? );', ( ( service_id, tag_id, now ) for tag_id in tag_ids ) )
            
        
    
    def _Read( self, action, *args, **kwargs ):
        
        if action == 'autocomplete_predicates': result = self._GetAutocompletePredicates( *args, **kwargs )
        elif action == 'boned_stats': result = self._GetBonedStats( *args, **kwargs )
        elif action == 'client_files_locations': result = self._GetClientFilesLocations( *args, **kwargs )
        elif action == 'deferred_physical_delete': result = self.modules_files_storage.GetDeferredPhysicalDelete( *args, **kwargs )
        elif action == 'duplicate_pairs_for_filtering': result = self._DuplicatesGetPotentialDuplicatePairsForFiltering( *args, **kwargs )
        elif action == 'file_duplicate_hashes': result = self.modules_files_duplicates.DuplicatesGetFileHashesByDuplicateType( *args, **kwargs )
        elif action == 'file_duplicate_info': result = self.modules_files_duplicates.DuplicatesGetFileDuplicateInfo( *args, **kwargs )
        elif action == 'file_hashes': result = self.modules_hashes.GetFileHashes( *args, **kwargs )
        elif action == 'file_maintenance_get_job': result = self.modules_files_maintenance_queue.GetJob( *args, **kwargs )
        elif action == 'file_maintenance_get_job_counts': result = self.modules_files_maintenance_queue.GetJobCounts( *args, **kwargs )
        elif action == 'file_query_ids': result = self._GetHashIdsFromQuery( *args, **kwargs )
        elif action == 'file_system_predicates': result = self._GetFileSystemPredicates( *args, **kwargs )
        elif action == 'filter_existing_tags': result = self._FilterExistingTags( *args, **kwargs )
        elif action == 'filter_hashes': result = self._FilterHashesByService( *args, **kwargs )
        elif action == 'force_refresh_tags_managers': result = self._GetForceRefreshTagsManagers( *args, **kwargs )
        elif action == 'gui_session': result = self.modules_serialisable.GetGUISession( *args, **kwargs )
        elif action == 'hash_ids_to_hashes': result = self.modules_hashes_local_cache.GetHashIdsToHashes( *args, **kwargs )
        elif action == 'hash_status': result = self._GetHashStatus( *args, **kwargs )
        elif action == 'have_hashed_serialised_objects': result = self.modules_serialisable.HaveHashedJSONDumps( *args, **kwargs )
        elif action == 'ideal_client_files_locations': result = self._GetIdealClientFilesLocations( *args, **kwargs )
        elif action == 'imageboards': result = self.modules_serialisable.GetYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_IMAGEBOARD, *args, **kwargs )
        elif action == 'inbox_hashes': result = self._FilterInboxHashes( *args, **kwargs )
        elif action == 'is_an_orphan': result = self._IsAnOrphan( *args, **kwargs )
        elif action == 'last_shutdown_work_time': result = self.modules_db_maintenance.GetLastShutdownWorkTime( *args, **kwargs )
        elif action == 'local_booru_share_keys': result = self.modules_serialisable.GetYAMLDumpNames( ClientDBSerialisable.YAML_DUMP_ID_LOCAL_BOORU )
        elif action == 'local_booru_share': result = self.modules_serialisable.GetYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_LOCAL_BOORU, *args, **kwargs )
        elif action == 'local_booru_shares': result = self.modules_serialisable.GetYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_LOCAL_BOORU )
        elif action == 'maintenance_due': result = self._GetMaintenanceDue( *args, **kwargs )
        elif action == 'media_predicates': result = self._GetMediaPredicates( *args, **kwargs )
        elif action == 'media_result': result = self._GetMediaResultFromHash( *args, **kwargs )
        elif action == 'media_results': result = self._GetMediaResultsFromHashes( *args, **kwargs )
        elif action == 'media_results_from_ids': result = self._GetMediaResults( *args, **kwargs )
        elif action == 'migration_get_mappings': result = self._MigrationGetMappings( *args, **kwargs )
        elif action == 'migration_get_pairs': result = self._MigrationGetPairs( *args, **kwargs )
        elif action == 'missing_repository_update_hashes': result = self.modules_repositories.GetRepositoryUpdateHashesIDoNotHave( *args, **kwargs )
        elif action == 'missing_thumbnail_hashes': result = self._GetRepositoryThumbnailHashesIDoNotHave( *args, **kwargs )
        elif action == 'num_deferred_file_deletes': result = self.modules_files_storage.GetDeferredPhysicalDeleteCounts()
        elif action == 'nums_pending': result = self._GetNumsPending( *args, **kwargs )
        elif action == 'trash_hashes': result = self._GetTrashHashes( *args, **kwargs )
        elif action == 'options': result = self._GetOptions( *args, **kwargs )
        elif action == 'pending': result = self._GetPending( *args, **kwargs )
        elif action == 'random_potential_duplicate_hashes': result = self._DuplicatesGetRandomPotentialDuplicateHashes( *args, **kwargs )
        elif action == 'recent_tags': result = self._GetRecentTags( *args, **kwargs )
        elif action == 'repository_progress': result = self.modules_repositories.GetRepositoryProgress( *args, **kwargs )
        elif action == 'repository_update_hashes_to_process': result = self.modules_repositories.GetRepositoryUpdateHashesICanProcess( *args, **kwargs )
        elif action == 'serialisable': result = self.modules_serialisable.GetJSONDump( *args, **kwargs )
        elif action == 'serialisable_simple': result = self.modules_serialisable.GetJSONSimple( *args, **kwargs )
        elif action == 'serialisable_named': result = self.modules_serialisable.GetJSONDumpNamed( *args, **kwargs )
        elif action == 'serialisable_names': result = self.modules_serialisable.GetJSONDumpNames( *args, **kwargs )
        elif action == 'serialisable_names_to_backup_timestamps': result = self.modules_serialisable.GetJSONDumpNamesToBackupTimestamps( *args, **kwargs )
        elif action == 'service_directory': result = self._GetServiceDirectoryHashes( *args, **kwargs )
        elif action == 'service_directories': result = self._GetServiceDirectoriesInfo( *args, **kwargs )
        elif action == 'service_filenames': result = self._GetServiceFilenames( *args, **kwargs )
        elif action == 'service_info': result = self._GetServiceInfo( *args, **kwargs )
        elif action == 'services': result = self.modules_services.GetServices( *args, **kwargs )
        elif action == 'similar_files_maintenance_status': result = self.modules_similar_files.GetMaintenanceStatus( *args, **kwargs )
        elif action == 'related_tags': result = self._GetRelatedTags( *args, **kwargs )
        elif action == 'tag_display_application': result = self.modules_tag_display.GetApplication( *args, **kwargs )
        elif action == 'tag_display_maintenance_status': result = self._CacheTagDisplayGetApplicationStatusNumbers( *args, **kwargs )
        elif action == 'tag_parents': result = self.modules_tag_parents.GetTagParents( *args, **kwargs )
        elif action == 'tag_siblings': result = self.modules_tag_siblings.GetTagSiblings( *args, **kwargs )
        elif action == 'tag_siblings_all_ideals': result = self.modules_tag_siblings.GetTagSiblingsIdeals( *args, **kwargs )
        elif action == 'tag_display_decorators': result = self.modules_tag_display.GetUIDecorators( *args, **kwargs )
        elif action == 'tag_siblings_and_parents_lookup': result = self.modules_tag_display.GetSiblingsAndParentsForTags( *args, **kwargs )
        elif action == 'tag_siblings_lookup': result = self.modules_tag_siblings.GetTagSiblingsForTags( *args, **kwargs )
        elif action == 'potential_duplicates_count': result = self._DuplicatesGetPotentialDuplicatesCount( *args, **kwargs )
        elif action == 'url_statuses': result = self._GetURLStatuses( *args, **kwargs )
        elif action == 'vacuum_data': result = self.modules_db_maintenance.GetVacuumData( *args, **kwargs )
        else: raise Exception( 'db received an unknown read command: ' + action )
        
        return result
        
    
    def _RecoverFromMissingDefinitions( self, content_type ):
        
        # this is not finished, but basics are there
        # remember this func uses a bunch of similar tech for the eventual orphan definition cleansing routine
        # we just have to extend modules functionality to cover all content tables and we are good to go
        
        if content_type == HC.CONTENT_TYPE_HASH:
            
            definition_column_name = 'hash_id'
            
        
        # eventually migrate this gubbins to cancellable async done in parts, which means generating, handling, and releasing the temp table name more cleverly
        
        # job presentation to UI
        
        all_tables_and_columns = []
        
        for module in self._modules:
            
            all_tables_and_columns.extend( module.GetTablesAndColumnsThatUseDefinitions( HC.CONTENT_TYPE_HASH ) )
            
        
        temp_all_useful_definition_ids_table_name = 'durable_temp.all_useful_definition_ids_{}'.format( os.urandom( 8 ).hex() )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS {} ( {} INTEGER PRIMARY KEY );'.format( temp_all_useful_definition_ids_table_name, definition_column_name ) )
        
        try:
            
            num_to_do = 0
            
            for ( table_name, column_name ) in all_tables_and_columns:
                
                query = 'INSERT OR IGNORE INTO {} ( {} ) SELECT DISTINCT {} FROM {};'.format(
                    temp_all_useful_definition_ids_table_name,
                    definition_column_name,
                    column_name,
                    table_name
                )
                
                self._Execute( query )
                
                num_to_do += self._GetRowCount()
                
            
            num_missing = 0
            num_recovered = 0
            
            batch_of_definition_ids = self._Execute( 'SELECT {} FROM {} LIMIT 1024;'.format( definition_column_name, temp_all_useful_definition_ids_table_name ) )
            
            while len( batch_of_definition_ids ) > 1024:
                
                for definition_id in batch_of_definition_ids:
                    
                    if not self.modules_hashes.HasHashId( definition_id ):
                        
                        if content_type == HC.CONTENT_TYPE_HASH and self.modules_hashes_local_cache.HasHashId( definition_id ):
                            
                            hash = self.modules_hashes_local_cache.GetHash( definition_id )
                            
                            self._Execute( 'INSERT OR IGNORE INTO hashes ( hash_id, hash ) VALUES ( ?, ? );', ( definition_id, sqlite3.Binary( hash ) ) )
                            
                            HydrusData.Print( '{} {} had no master definition, but I was able to recover from the local cache'.format( definition_column_name, definition_id ) )
                            
                            num_recovered += 1
                            
                        else:
                            
                            HydrusData.Print( '{} {} had no master definition, it has been purged from the database!'.format( definition_column_name, definition_id ) )
                            
                            for ( table_name, column_name ) in all_tables_and_columns:
                                
                                self._Execute( 'DELETE FROM {} WHERE {} = ?;'.format( table_name, column_name ), ( definition_id, ) )
                                
                            
                            # tell user they will want to run clear orphan files, reset service cache info, and may need to recalc some autocomplete counts depending on total missing definitions
                            # I should clear service info based on content_type
                            
                            num_missing += 1
                            
                        
                    
                
                batch_of_definition_ids = self._Execute( 'SELECT {} FROM {} LIMIT 1024;'.format( definition_column_name, temp_all_useful_definition_ids_table_name ) )
                
            
        finally:
            
            self._Execute( 'DROP TABLE {};'.format( temp_all_useful_definition_ids_table_name ) )
            
        
    
    def _RegenerateLocalHashCache( self ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating local hash cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            message = 'generating local hash cache'
            
            job_key.SetVariable( 'popup_text_1', message )
            self._controller.frame_splash_status.SetSubtext( message )
            
            self.modules_hashes_local_cache.Repopulate()
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
        
    
    def _RegenerateLocalTagCache( self ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating local tag cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            message = 'generating local tag cache'
            
            job_key.SetVariable( 'popup_text_1', message )
            self._controller.frame_splash_status.SetSubtext( message )
            
            self.modules_tags_local_cache.Repopulate()
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
            
        
    
    def _RegenerateTagCacheSearchableSubtagMaps( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerate tag fast search cache searchable subtag map' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            def status_hook( s ):
                
                job_key.SetVariable( 'popup_text_2', s )
                
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'repopulating specific cache {}_{}'.format( file_service_id, tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.RegenerateSearchableSubtagMap( file_service_id, tag_service_id, status_hook = status_hook )
                
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'repopulating combined cache {}'.format( tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.RegenerateSearchableSubtagMap( self.modules_services.combined_file_service_id, tag_service_id, status_hook = status_hook )
                
            
        finally:
            
            job_key.DeleteVariable( 'popup_text_2' )
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
        
    
    def _RegenerateTagCache( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating tag fast search cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            def status_hook( s ):
                
                job_key.SetVariable( 'popup_text_2', s )
                
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'generating specific cache {}_{}'.format( file_service_id, tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.Drop( file_service_id, tag_service_id )
                
                self.modules_tag_search.Generate( file_service_id, tag_service_id )
                
                self._CacheTagsPopulate( file_service_id, tag_service_id, status_hook = status_hook )
                
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'generating combined cache {}'.format( tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.Drop( self.modules_services.combined_file_service_id, tag_service_id )
                
                self.modules_tag_search.Generate( self.modules_services.combined_file_service_id, tag_service_id )
                
                self._CacheTagsPopulate( self.modules_services.combined_file_service_id, tag_service_id, status_hook = status_hook )
                
            
        finally:
            
            job_key.DeleteVariable( 'popup_text_2' )
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
        
    
    def _RegenerateTagDisplayMappingsCache( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating tag display mappings cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for tag_service_id in tag_service_ids:
                
                # first off, we want to clear all the current siblings and parents so they will be reprocessed later
                # we'll also have to catch up the tag definition cache to account for this
                
                tag_ids_in_dispute = set()
                
                tag_ids_in_dispute.update( self.modules_tag_siblings.GetAllTagIds( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id ) )
                tag_ids_in_dispute.update( self.modules_tag_parents.GetAllTagIds( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id ) )
                
                self.modules_tag_siblings.ClearActual( tag_service_id )
                self.modules_tag_parents.ClearActual( tag_service_id )
                
                if len( tag_ids_in_dispute ) > 0:
                    
                    self._CacheTagsSyncTags( tag_service_id, tag_ids_in_dispute )
                    
                
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'generating specific display cache {}_{}'.format( file_service_id, tag_service_id )
                
                def status_hook_1( s: str ):
                    
                    job_key.SetVariable( 'popup_text_2', s )
                    self._controller.frame_splash_status.SetSubtext( '{} - {}'.format( message, s ) )
                    
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                status_hook_1( 'dropping old data' )
                
                self.modules_mappings_cache_specific_display.Drop( file_service_id, tag_service_id )
                
                self.modules_mappings_cache_specific_display.Generate( file_service_id, tag_service_id, populate_from_storage = True, status_hook = status_hook_1 )
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'generating combined display cache {}'.format( tag_service_id )
                
                def status_hook_2( s: str ):
                    
                    job_key.SetVariable( 'popup_text_2', s )
                    self._controller.frame_splash_status.SetSubtext( '{} - {}'.format( message, s ) )
                    
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                status_hook_2( 'dropping old data' )
                
                self._CacheCombinedFilesDisplayMappingsDrop( tag_service_id )
                
                self._CacheCombinedFilesDisplayMappingsGenerate( tag_service_id, status_hook = status_hook_2 )
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
            
        
    
    def _RegenerateTagDisplayPendingMappingsCache( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating tag display pending mappings cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'regenerating specific display cache pending {}_{}'.format( file_service_id, tag_service_id )
                
                def status_hook_1( s: str ):
                    
                    job_key.SetVariable( 'popup_text_2', s )
                    self._controller.frame_splash_status.SetSubtext( '{} - {}'.format( message, s ) )
                    
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                self.modules_mappings_cache_specific_display.RegeneratePending( file_service_id, tag_service_id, status_hook = status_hook_1 )
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'regenerating combined display cache pending {}'.format( tag_service_id )
                
                def status_hook_2( s: str ):
                    
                    job_key.SetVariable( 'popup_text_2', s )
                    self._controller.frame_splash_status.SetSubtext( '{} - {}'.format( message, s ) )
                    
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                self._CacheCombinedFilesDisplayMappingsRegeneratePending( tag_service_id, status_hook = status_hook_2 )
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
            
        
    
    def _RegenerateTagMappingsCache( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating tag mappings cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            tag_cache_file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            for tag_service_id in tag_service_ids:
                
                self.modules_tag_siblings.ClearActual( tag_service_id )
                self.modules_tag_parents.ClearActual( tag_service_id )
                
            
            time.sleep( 0.01 )
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'generating specific cache {}_{}'.format( file_service_id, tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                if file_service_id in tag_cache_file_service_ids:
                    
                    self.modules_tag_search.Drop( file_service_id, tag_service_id )
                    self.modules_tag_search.Generate( file_service_id, tag_service_id )
                    
                
                self._CacheSpecificMappingsDrop( file_service_id, tag_service_id )
                
                self._CacheSpecificMappingsGenerate( file_service_id, tag_service_id )
                
                self._cursor_transaction_wrapper.CommitAndBegin()
                
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'generating combined cache {}'.format( tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.Drop( self.modules_services.combined_file_service_id, tag_service_id )
                self.modules_tag_search.Generate( self.modules_services.combined_file_service_id, tag_service_id )
                
                self._CacheCombinedFilesMappingsDrop( tag_service_id )
                
                self._CacheCombinedFilesMappingsGenerate( tag_service_id )
                
                self._cursor_transaction_wrapper.CommitAndBegin()
                
            
            if tag_service_key is None:
                
                message = 'generating local tag cache'
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                self.modules_tags_local_cache.Repopulate()
                
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
            
        
    
    def _RegenerateTagParentsCache( self, only_these_service_ids = None ):
        
        if only_these_service_ids is None:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            tag_service_ids = only_these_service_ids
            
        
        # as siblings may have changed, parents may have as well
        self.modules_tag_parents.Regen( tag_service_ids )
        
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_tag_display_application' )
        
    
    def _RegenerateTagPendingMappingsCache( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'regenerating tag pending mappings cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'regenerating specific cache pending {}_{}'.format( file_service_id, tag_service_id )
                
                def status_hook_1( s: str ):
                    
                    job_key.SetVariable( 'popup_text_2', s )
                    self._controller.frame_splash_status.SetSubtext( '{} - {}'.format( message, s ) )
                    
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                self._CacheSpecificMappingsRegeneratePending( file_service_id, tag_service_id, status_hook = status_hook_1 )
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'regenerating combined cache pending {}'.format( tag_service_id )
                
                def status_hook_2( s: str ):
                    
                    job_key.SetVariable( 'popup_text_2', s )
                    self._controller.frame_splash_status.SetSubtext( '{} - {}'.format( message, s ) )
                    
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                self._CacheCombinedFilesMappingsRegeneratePending( tag_service_id, status_hook = status_hook_2 )
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
            
        
    
    def _RelocateClientFiles( self, prefix, source, dest ):
        
        if not os.path.exists( dest ):
            
            raise Exception( 'Was commanded to move prefix "{}" from "{}" to "{}", but that destination does not exist!'.format( prefix, source, dest ) )
            
        
        full_source = os.path.join( source, prefix )
        full_dest = os.path.join( dest, prefix )
        
        if os.path.exists( full_source ):
            
            HydrusPaths.MergeTree( full_source, full_dest )
            
        elif not os.path.exists( full_dest ):
            
            HydrusPaths.MakeSureDirectoryExists( full_dest )
            
        
        portable_dest = HydrusPaths.ConvertAbsPathToPortablePath( dest )
        
        self._Execute( 'UPDATE client_files_locations SET location = ? WHERE prefix = ?;', ( portable_dest, prefix ) )
        
        if os.path.exists( full_source ):
            
            try: HydrusPaths.RecyclePath( full_source )
            except: pass
            
        
    
    def _RepairClientFiles( self, correct_rows ):
        
        for ( prefix, correct_location ) in correct_rows:
            
            full_abs_correct_location = os.path.join( correct_location, prefix )
            
            HydrusPaths.MakeSureDirectoryExists( full_abs_correct_location )
            
            portable_correct_location = HydrusPaths.ConvertAbsPathToPortablePath( correct_location )
            
            self._Execute( 'UPDATE client_files_locations SET location = ? WHERE prefix = ?;', ( portable_correct_location, prefix ) )
            
        
    
    def _RepairDB( self, version ):
        
        # migrate most of this gubbins to the new modules system, and HydrusDB tbh!
        
        self._controller.frame_splash_status.SetText( 'checking database' )
        
        HydrusDB.HydrusDB._RepairDB( self, version )
        
        self._weakref_media_result_cache = ClientMediaResultCache.MediaResultCache()
        
        tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
        file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
        
        # caches
        
        existing_cache_tables = self._STS( self._Execute( 'SELECT name FROM external_caches.sqlite_master WHERE type = ?;', ( 'table', ) ) )
        
        mappings_cache_tables = set()
        
        for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
            
            if version >= 465:
                
                mappings_cache_tables.update( ( name.split( '.' )[1] for name in ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( file_service_id, tag_service_id ) ) )
                mappings_cache_tables.update( ( name.split( '.' )[1] for name in ClientDBMappingsCacheSpecificDisplay.GenerateSpecificDisplayMappingsCacheTableNames( file_service_id, tag_service_id ) ) )
                
            
        
        we_did_a_full_regen = False
        
        missing_main_tables = sorted( mappings_cache_tables.difference( existing_cache_tables ) )
        
        if len( missing_main_tables ) > 0:
            
            HydrusData.DebugPrint( 'The missing mapping cache tables were:' )
            HydrusData.DebugPrint( os.linesep.join( missing_main_tables ) )
            
            message = 'On boot, {} mapping caches tables were missing! This could be due to the entire \'caches\' database file being missing or due to some other problem. All of this data can be regenerated.'.format( len( missing_main_tables ) )
            message += os.linesep * 2
            message += 'If you wish, click ok on this message and the client will recreate and repopulate these tables with the correct data. This may take a few minutes. But if you want to solve this problem otherwise, kill the hydrus process now.'
            message += os.linesep * 2
            message += 'If you do not already know what caused this, it was likely a hard drive fault--either due to a recent abrupt power cut or actual hardware failure. Check \'help my db is broke.txt\' in the install_dir/db directory as soon as you can.'
            
            BlockingSafeShowMessage( message )
            
            self._RegenerateTagMappingsCache()
            
            we_did_a_full_regen = True
            
        
        if not we_did_a_full_regen:
            
            # autocomplete
            
            ( missing_storage_tag_count_service_pairs, missing_display_tag_count_service_pairs ) = self.modules_mappings_counts.GetMissingTagCountServicePairs()
            
            # unfortunately, for now, due to display maintenance being tag service wide, I can't regen individual lads here
            # maybe in future I can iterate all sibs/parents and just do it here and now with addimplication
            
            missing_storage_tag_count_tag_service_ids = { tag_service_id for ( file_service_id, tag_service_id ) in missing_storage_tag_count_service_pairs }
            missing_display_tag_count_tag_service_ids = { tag_service_id for ( file_service_id, tag_service_id ) in missing_display_tag_count_service_pairs }
            
            # a storage regen will cover a display regen
            
            missing_display_tag_count_tag_service_ids = missing_display_tag_count_tag_service_ids.difference( missing_storage_tag_count_tag_service_ids )
            
            if len( missing_display_tag_count_tag_service_ids ) > 0:
                
                missing_display_tag_count_tag_service_ids = sorted( missing_display_tag_count_tag_service_ids )
                
                message = 'On boot, some important tag count tables for the display context were missing! You should have already had a notice about this. You may have had other problems earlier, but this particular problem is completely recoverable and results in no lost data. The relevant tables have been recreated and will now be repopulated. The services about to be worked on are:'
                message += os.linesep * 2
                message += os.linesep.join( ( str( t ) for t in missing_display_tag_count_tag_service_ids ) )
                message += os.linesep * 2
                message += 'If you want to go ahead, click ok on this message and the client will fill these tables with the correct data. It may take some time. If you want to solve this problem otherwise, kill the hydrus process now.'
                
                BlockingSafeShowMessage( message )
                
                for tag_service_id in missing_display_tag_count_tag_service_ids:
                    
                    tag_service_key = self.modules_services.GetService( tag_service_id ).GetServiceKey()
                    
                    self._RegenerateTagDisplayMappingsCache( tag_service_key = tag_service_key )
                    
                    self.modules_db_maintenance.TouchAnalyzeNewTables()
                    
                    self._cursor_transaction_wrapper.CommitAndBegin()
                    
                
            
            if len( missing_storage_tag_count_tag_service_ids ) > 0:
                
                missing_storage_tag_count_tag_service_ids = sorted( missing_storage_tag_count_tag_service_ids )
                
                message = 'On boot, some important tag count tables for the storage context were missing! You should have already had a notice about this. You may have had other problems earlier, but this particular problem is completely recoverable and results in no lost data. The relevant tables have been recreated and will now be repopulated. The services about to be worked on are:'
                message += os.linesep * 2
                message += os.linesep.join( ( str( t ) for t in missing_storage_tag_count_tag_service_ids ) )
                message += os.linesep * 2
                message += 'If you want to go ahead, click ok on this message and the client will fill these tables with the correct data. It may take some time. If you want to solve this problem otherwise, kill the hydrus process now.'
                
                BlockingSafeShowMessage( message )
                
                for tag_service_id in missing_storage_tag_count_tag_service_ids:
                    
                    tag_service_key = self.modules_services.GetService( tag_service_id ).GetServiceKey()
                    
                    self._RegenerateTagMappingsCache( tag_service_key = tag_service_key )
                    
                    self.modules_db_maintenance.TouchAnalyzeNewTables()
                    
                    self._cursor_transaction_wrapper.CommitAndBegin()
                    
                
            
            # tag search, this requires autocomplete and siblings/parents in place
            
            missing_tag_search_service_pairs = self.modules_tag_search.GetMissingTagSearchServicePairs()
            
            if len( missing_tag_search_service_pairs ) > 0:
                
                missing_tag_search_service_pairs = sorted( missing_tag_search_service_pairs )
                
                message = 'On boot, some important tag search tables were missing! You should have already had a notice about this. You may have had other problems earlier, but this particular problem is completely recoverable and results in no lost data. The relevant tables have been recreated and will now be repopulated. The service pairs about to be worked on are:'
                message += os.linesep * 2
                message += os.linesep.join( ( str( t ) for t in missing_tag_search_service_pairs ) )
                message += os.linesep * 2
                message += 'If you want to go ahead, click ok on this message and the client will fill these tables with the correct data. It may take some time. If you want to solve this problem otherwise, kill the hydrus process now.'
                
                BlockingSafeShowMessage( message )
                
                for ( file_service_id, tag_service_id ) in missing_tag_search_service_pairs:
                    
                    self.modules_tag_search.Drop( file_service_id, tag_service_id )
                    self.modules_tag_search.Generate( file_service_id, tag_service_id )
                    self._CacheTagsPopulate( file_service_id, tag_service_id )
                    
                    self.modules_db_maintenance.TouchAnalyzeNewTables()
                    
                    self._cursor_transaction_wrapper.CommitAndBegin()
                    
                
            
        
        #
        
        new_options = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_CLIENT_OPTIONS )
        
        if new_options is None:
            
            message = 'On boot, your main options object was missing!'
            message += os.linesep * 2
            message += 'If you wish, click ok on this message and the client will re-add fresh options with default values. But if you want to solve this problem otherwise, kill the hydrus process now.'
            message += os.linesep * 2
            message += 'If you do not already know what caused this, it was likely a hard drive fault--either due to a recent abrupt power cut or actual hardware failure. Check \'help my db is broke.txt\' in the install_dir/db directory as soon as you can.'
            
            BlockingSafeShowMessage( message )
            
            new_options = ClientOptions.ClientOptions()
            
            new_options.SetSimpleDownloaderFormulae( ClientDefaults.GetDefaultSimpleDownloaderFormulae() )
            
            self.modules_serialisable.SetJSONDump( new_options )
            
        
        # an explicit empty string so we don't linger on 'checking database' if the next stage lags a bit on its own update. no need to give anyone heart attacks
        self._controller.frame_splash_status.SetText( '' )
        
    
    def _RepairInvalidTags( self, job_key: typing.Optional[ ClientThreading.JobKey ] = None ):
        
        invalid_tag_ids_and_tags = set()
        
        BLOCK_SIZE = 1000
        
        select_statement = 'SELECT tag_id FROM tags;'
        
        bad_tag_count = 0
        
        for ( group_of_tag_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, select_statement, BLOCK_SIZE ):
            
            if job_key is not None:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'Scanning tags: {} - Bad Found: {}'.format( HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do ), HydrusData.ToHumanInt( bad_tag_count ) )
                
                job_key.SetVariable( 'popup_text_1', message )
                
            
            for tag_id in group_of_tag_ids:
                
                tag = self.modules_tags_local_cache.GetTag( tag_id )
                
                try:
                    
                    cleaned_tag = HydrusTags.CleanTag( tag )
                    
                    HydrusTags.CheckTagNotEmpty( cleaned_tag )
                    
                except:
                    
                    cleaned_tag = 'unrecoverable invalid tag'
                    
                
                if tag != cleaned_tag:
                    
                    invalid_tag_ids_and_tags.add( ( tag_id, tag, cleaned_tag ) )
                    
                    bad_tag_count += 1
                    
                
            
        
        file_service_ids = list( self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES ) )
        file_service_ids.append( self.modules_services.combined_file_service_id )
        
        tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
        
        for ( i, ( tag_id, tag, cleaned_tag ) ) in enumerate( invalid_tag_ids_and_tags ):
            
            if job_key is not None:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'Fixing bad tags: {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, bad_tag_count ) )
                
                job_key.SetVariable( 'popup_text_1', message )
                
            
            # now find an entirely new namespace_id, subtag_id pair for this tag
            
            existing_tags = set()
            
            potential_new_cleaned_tag = cleaned_tag
            
            while self.modules_tags.TagExists( potential_new_cleaned_tag ):
                
                existing_tags.add( potential_new_cleaned_tag )
                
                potential_new_cleaned_tag = HydrusData.GetNonDupeName( cleaned_tag, existing_tags )
                
            
            cleaned_tag = potential_new_cleaned_tag
            
            ( namespace, subtag ) = HydrusTags.SplitTag( cleaned_tag )
            
            namespace_id = self.modules_tags.GetNamespaceId( namespace )
            subtag_id = self.modules_tags.GetSubtagId( subtag )
            
            self.modules_tags.UpdateTagId( tag_id, namespace_id, subtag_id )
            self.modules_tags_local_cache.UpdateTagInCache( tag_id, cleaned_tag )
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if self.modules_tag_search.HasTag( file_service_id, tag_service_id, tag_id ):
                    
                    self.modules_tag_search.DeleteTags( file_service_id, tag_service_id, ( tag_id, ) )
                    self.modules_tag_search.AddTags( file_service_id, tag_service_id, ( tag_id, ) )
                    
                
            
            try:
                
                HydrusData.Print( 'Invalid tag fixing: {} replaced with {}'.format( repr( tag ), repr( cleaned_tag ) ) )
                
            except:
                
                HydrusData.Print( 'Invalid tag fixing: Could not even print the bad tag to the log! It is now known as {}'.format( repr( cleaned_tag ) ) )
                
            
        
        if job_key is not None:
            
            if not job_key.IsCancelled():
                
                if bad_tag_count == 0:
                    
                    message = 'Invalid tag scanning: No bad tags found!'
                    
                else:
                    
                    message = 'Invalid tag scanning: {} bad tags found and fixed! They have been written to the log.'.format( HydrusData.ToHumanInt( bad_tag_count ) )
                    
                    self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
                    
                
                HydrusData.Print( message )
                
                job_key.SetVariable( 'popup_text_1', message )
                
            
            job_key.Finish()
            
        
    
    def _RepopulateMappingsFromCache( self, tag_service_key = None, job_key = None ):
        
        BLOCK_SIZE = 10000
        
        num_rows_recovered = 0
        
        if tag_service_key is None:
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
        else:
            
            tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
            
        
        for tag_service_id in tag_service_ids:
            
            service = self.modules_services.GetService( tag_service_id )
            
            name = service.GetName()
            
            ( cache_current_mappings_table_name, cache_deleted_mappings_table_name, cache_pending_mappings_table_name ) = ClientDBMappingsStorage.GenerateSpecificMappingsCacheTableNames( self.modules_services.combined_local_file_service_id, tag_service_id )
            
            ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
            
            current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_local_file_service_id, HC.CONTENT_STATUS_CURRENT )
            
            select_statement = 'SELECT hash_id FROM {};'.format( current_files_table_name )
            
            for ( group_of_hash_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, select_statement, BLOCK_SIZE ):
                
                if job_key is not None:
                    
                    message = 'Doing "{}"\u2026: {}'.format( name, HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do ) )
                    message += os.linesep * 2
                    message += 'Total rows recovered: {}'.format( HydrusData.ToHumanInt( num_rows_recovered ) )
                    
                    job_key.SetVariable( 'popup_text_1', message )
                    
                    if job_key.IsCancelled():
                        
                        return
                        
                    
                
                with self._MakeTemporaryIntegerTable( group_of_hash_ids, 'hash_id' ) as temp_table_name:
                    
                    # temp hashes to mappings
                    insert_template = 'INSERT OR IGNORE INTO {} ( tag_id, hash_id ) SELECT tag_id, hash_id FROM {} CROSS JOIN {} USING ( hash_id );'
                    
                    self._Execute( insert_template.format( current_mappings_table_name, temp_table_name, cache_current_mappings_table_name ) )
                    
                    num_rows_recovered += self._GetRowCount()
                    
                    self._Execute( insert_template.format( deleted_mappings_table_name, temp_table_name, cache_deleted_mappings_table_name ) )
                    
                    num_rows_recovered += self._GetRowCount()
                    
                    self._Execute( insert_template.format( pending_mappings_table_name, temp_table_name, cache_pending_mappings_table_name ) )
                    
                    num_rows_recovered += self._GetRowCount()
                    
                
            
        
        if job_key is not None:
            
            job_key.SetVariable( 'popup_text_1', 'Done! Rows recovered: {}'.format( HydrusData.ToHumanInt( num_rows_recovered ) ) )
            
            job_key.Finish()
            
        
    
    def _RepopulateTagCacheMissingSubtags( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'repopulate tag fast search cache subtags' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            def status_hook( s ):
                
                job_key.SetVariable( 'popup_text_2', s )
                
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'repopulating specific cache {}_{}'.format( file_service_id, tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.RepopulateMissingSubtags( file_service_id, tag_service_id )
                
            
            for tag_service_id in tag_service_ids:
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                message = 'repopulating combined cache {}'.format( tag_service_id )
                
                job_key.SetVariable( 'popup_text_1', message )
                self._controller.frame_splash_status.SetSubtext( message )
                
                time.sleep( 0.01 )
                
                self.modules_tag_search.RepopulateMissingSubtags( self.modules_services.combined_file_service_id, tag_service_id )
                
            
        finally:
            
            job_key.DeleteVariable( 'popup_text_2' )
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
        
    
    def _RepopulateTagDisplayMappingsCache( self, tag_service_key = None ):
        
        job_key = ClientThreading.JobKey( cancellable = True )
        
        try:
            
            job_key.SetStatusTitle( 'repopulating tag display mappings cache' )
            
            self._controller.pub( 'modal_message', job_key )
            
            if tag_service_key is None:
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
            else:
                
                tag_service_ids = ( self.modules_services.GetServiceId( tag_service_key ), )
                
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
            
            for ( i, file_service_id ) in enumerate( file_service_ids ):
                
                if job_key.IsCancelled():
                    
                    break
                    
                
                table_name = ClientDBFilesStorage.GenerateFilesTableName( file_service_id, HC.CONTENT_STATUS_CURRENT )
                
                for ( group_of_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, 'SELECT hash_id FROM {};'.format( table_name ), 1024 ):
                    
                    message = 'repopulating {} {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, len( file_service_ids ) ), HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do ) )
                    
                    job_key.SetVariable( 'popup_text_1', message )
                    self._controller.frame_splash_status.SetSubtext( message )
                    
                    with self._MakeTemporaryIntegerTable( group_of_ids, 'hash_id' ) as temp_hash_id_table_name:
                        
                        for tag_service_id in tag_service_ids:
                            
                            self._CacheSpecificMappingsAddFiles( file_service_id, tag_service_id, group_of_ids, temp_hash_id_table_name )
                            self.modules_mappings_cache_specific_display.AddFiles( file_service_id, tag_service_id, group_of_ids, temp_hash_id_table_name )
                            
                        
                    
                
            
            job_key.SetVariable( 'popup_text_2', '' )
            self._controller.frame_splash_status.SetSubtext( '' )
            
        finally:
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 5 )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_force_refresh_tags_data' )
            
        
    
    def _ReportOverupdatedDB( self, version ):
        
        message = 'This client\'s database is version {}, but the software is version {}! This situation only sometimes works, and when it does not, it can break things! If you are not sure what is going on, or if you accidentally installed an older version of the software to a newer database, force-kill this client in Task Manager right now. Otherwise, ok this dialog box to continue.'.format( HydrusData.ToHumanInt( version ), HydrusData.ToHumanInt( HC.SOFTWARE_VERSION ) )
        
        BlockingSafeShowMessage( message )
        
    
    def _ReportUnderupdatedDB( self, version ):
        
        message = 'This client\'s database is version {}, but the software is significantly later, {}! Trying to update many versions in one go can be dangerous due to bitrot. I suggest you try at most to only do 10 versions at once. If you want to try a big jump anyway, you should make sure you have a backup beforehand so you can roll back to it in case the update makes your db unbootable. If you would rather try smaller updates, or you do not have a backup, force-kill this client in Task Manager right now. Otherwise, ok this dialog box to continue.'.format( HydrusData.ToHumanInt( version ), HydrusData.ToHumanInt( HC.SOFTWARE_VERSION ) )
        
        BlockingSafeShowMessage( message )
        
    
    def _ResetRepository( self, service ):
        
        ( service_key, service_type, name, dictionary ) = service.ToTuple()
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        prefix = 'resetting ' + name
        
        job_key = ClientThreading.JobKey()
        
        try:
            
            job_key.SetVariable( 'popup_text_1', prefix + ': deleting service' )
            
            self._controller.pub( 'modal_message', job_key )
            
            self._DeleteService( service_id )
            
            job_key.SetVariable( 'popup_text_1', prefix + ': recreating service' )
            
            self._AddService( service_key, service_type, name, dictionary )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_account_sync_due' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_data' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_gui' )
            
            job_key.SetVariable( 'popup_text_1', prefix + ': done!' )
            
        finally:
            
            job_key.Finish()
            
        
    
    def _ResetRepositoryProcessing( self, service_key: bytes, content_types ):
        
        service_id = self.modules_services.GetServiceId( service_key )
        
        service = self.modules_services.GetService( service_id )
        
        service_type = service.GetServiceType()
        
        prefix = 'resetting content'
        
        job_key = ClientThreading.JobKey()
        
        try:
            
            service_info_types_to_delete = []
            
            job_key.SetVariable( 'popup_text_1', '{}: calculating'.format( prefix ) )
            
            self._controller.pub( 'modal_message', job_key )
            
            # note that siblings/parents do not do a cachetags clear-regen because they only actually delete ideal, not actual
            
            if HC.CONTENT_TYPE_FILES in content_types:
                
                service_info_types_to_delete.extend( { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_VIEWABLE_FILES, HC.SERVICE_INFO_TOTAL_SIZE, HC.SERVICE_INFO_NUM_DELETED_FILES } )
                
                self._Execute( 'DELETE FROM remote_thumbnails WHERE service_id = ?;', ( service_id, ) )
                
                if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
                    
                    self.modules_files_storage.ClearFilesTables( service_id, keep_pending = True )
                    
                
                if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES:
                    
                    tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                    
                    for tag_service_id in tag_service_ids:
                        
                        self._CacheSpecificMappingsClear( service_id, tag_service_id, keep_pending = True )
                        
                        if service_type in HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES:
                            
                            # not clear since siblings and parents can contribute
                            self.modules_tag_search.Drop( service_id, tag_service_id )
                            self.modules_tag_search.Generate( service_id, tag_service_id )
                            self._CacheTagsPopulate( service_id, tag_service_id )
                            
                        
                    
                
            
            if HC.CONTENT_TYPE_MAPPINGS in content_types:
                
                service_info_types_to_delete.extend( { HC.SERVICE_INFO_NUM_FILES, HC.SERVICE_INFO_NUM_TAGS, HC.SERVICE_INFO_NUM_MAPPINGS, HC.SERVICE_INFO_NUM_DELETED_MAPPINGS } )
                
                if service_type in HC.REAL_TAG_SERVICES:
                    
                    self.modules_mappings_storage.ClearMappingsTables( service_id )
                    
                    self._CacheCombinedFilesMappingsClear( service_id, keep_pending = True )
                    
                    self.modules_tag_search.Drop( self.modules_services.combined_file_service_id, service_id )
                    self.modules_tag_search.Generate( self.modules_services.combined_file_service_id, service_id )
                    self._CacheTagsPopulate( self.modules_services.combined_file_service_id, service_id )
                    
                    file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
                    tag_cache_file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
                    
                    for file_service_id in file_service_ids:
                        
                        self._CacheSpecificMappingsClear( file_service_id, service_id, keep_pending = True )
                        
                        if file_service_id in tag_cache_file_service_ids:
                            
                            # not clear since siblings and parents can contribute
                            self.modules_tag_search.Drop( file_service_id, service_id )
                            self.modules_tag_search.Generate( file_service_id, service_id )
                            self._CacheTagsPopulate( file_service_id, service_id )
                            
                        
                    
                
            
            if HC.CONTENT_TYPE_TAG_PARENTS in content_types:
                
                self._Execute( 'DELETE FROM tag_parents WHERE service_id = ?;', ( service_id, ) )
                self._Execute( 'DELETE FROM tag_parent_petitions WHERE service_id = ? AND status = ?;', ( service_id, HC.CONTENT_STATUS_PETITIONED ) )
                
                ( cache_ideal_tag_parents_lookup_table_name, cache_actual_tag_parents_lookup_table_name ) = ClientDBTagParents.GenerateTagParentsLookupCacheTableNames( service_id )
                
                # do not delete from actual!
                self._Execute( 'DELETE FROM {};'.format( cache_ideal_tag_parents_lookup_table_name ) )
                
            
            if HC.CONTENT_TYPE_TAG_SIBLINGS in content_types:
                
                self._Execute( 'DELETE FROM tag_siblings WHERE service_id = ?;', ( service_id, ) )
                self._Execute( 'DELETE FROM tag_sibling_petitions WHERE service_id = ? AND status = ?;', ( service_id, HC.CONTENT_STATUS_PETITIONED ) )
                
                ( cache_ideal_tag_siblings_lookup_table_name, cache_actual_tag_siblings_lookup_table_name ) = ClientDBTagSiblings.GenerateTagSiblingsLookupCacheTableNames( service_id )
                
                self._Execute( 'DELETE FROM {};'.format( cache_ideal_tag_siblings_lookup_table_name ) )
                
            
            #
            
            job_key.SetVariable( 'popup_text_1', '{}: recalculating'.format( prefix ) )
            
            if HC.CONTENT_TYPE_TAG_PARENTS in content_types or HC.CONTENT_TYPE_TAG_SIBLINGS in content_types:
                
                interested_service_ids = set( self.modules_tag_display.GetInterestedServiceIds( service_id ) )
                
                if len( interested_service_ids ) > 0:
                    
                    self.modules_tag_display.RegenerateTagSiblingsAndParentsCache( only_these_service_ids = interested_service_ids )
                    
                
            
            self._ExecuteMany( 'DELETE FROM service_info WHERE service_id = ? AND info_type = ?;', ( ( service_id, info_type ) for info_type in service_info_types_to_delete ) )
            
            self.modules_repositories.ReprocessRepository( service_key, content_types )
            
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_data' )
            self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_gui' )
            
            job_key.SetVariable( 'popup_text_1', prefix + ': done!' )
            
        finally:
            
            job_key.Finish()
            
        
    
    def _SaveDirtyServices( self, dirty_services ):
        
        # if allowed to save objects
        
        self._SaveServices( dirty_services )
        
    
    def _SaveServices( self, services ):
        
        for service in services:
            
            self.modules_services.UpdateService( service )
            
        
    
    def _SaveOptions( self, options ):
        
        try:
            
            self._Execute( 'UPDATE options SET options = ?;', ( options, ) )
            
        except:
            
            HydrusData.Print( 'Failed options save dump:' )
            HydrusData.Print( options )
            
            raise
            
        
        self._cursor_transaction_wrapper.pub_after_job( 'reset_thumbnail_cache' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_options' )
        
    
    def _SetIdealClientFilesLocations( self, locations_to_ideal_weights, ideal_thumbnail_override_location ):
        
        if len( locations_to_ideal_weights ) == 0:
            
            raise Exception( 'No locations passed in ideal locations list!' )
            
        
        self._Execute( 'DELETE FROM ideal_client_files_locations;' )
        
        for ( abs_location, weight ) in locations_to_ideal_weights.items():
            
            portable_location = HydrusPaths.ConvertAbsPathToPortablePath( abs_location )
            
            self._Execute( 'INSERT INTO ideal_client_files_locations ( location, weight ) VALUES ( ?, ? );', ( portable_location, weight ) )
            
        
        self._Execute( 'DELETE FROM ideal_thumbnail_override_location;' )
        
        if ideal_thumbnail_override_location is not None:
            
            portable_ideal_thumbnail_override_location = HydrusPaths.ConvertAbsPathToPortablePath( ideal_thumbnail_override_location )
            
            self._Execute( 'INSERT INTO ideal_thumbnail_override_location ( location ) VALUES ( ? );', ( portable_ideal_thumbnail_override_location, ) )
            
        
    
    def _SetPassword( self, password ):
        
        if password is not None:
            
            password_bytes = bytes( password, 'utf-8' )
            
            password = hashlib.sha256( password_bytes ).digest()
            
        
        self._controller.options[ 'password' ] = password
        
        self._SaveOptions( self._controller.options )
        
    
    def _SetServiceFilename( self, service_id, hash_id, filename ):
        
        self._Execute( 'REPLACE INTO service_filenames ( service_id, hash_id, filename ) VALUES ( ?, ?, ? );', ( service_id, hash_id, filename ) )
        
    
    def _SetServiceDirectory( self, service_id, hash_ids, dirname, note ):
        
        directory_id = self.modules_texts.GetTextId( dirname )
        
        self._Execute( 'DELETE FROM service_directories WHERE service_id = ? AND directory_id = ?;', ( service_id, directory_id ) )
        self._Execute( 'DELETE FROM service_directory_file_map WHERE service_id = ? AND directory_id = ?;', ( service_id, directory_id ) )
        
        num_files = len( hash_ids )
        
        result = self._Execute( 'SELECT SUM( size ) FROM files_info WHERE hash_id IN ' + HydrusData.SplayListForDB( hash_ids ) + ';' ).fetchone()
        
        if result is None:
            
            total_size = 0
            
        else:
            
            ( total_size, ) = result
            
        
        self._Execute( 'INSERT INTO service_directories ( service_id, directory_id, num_files, total_size, note ) VALUES ( ?, ?, ?, ?, ? );', ( service_id, directory_id, num_files, total_size, note ) )
        self._ExecuteMany( 'INSERT INTO service_directory_file_map ( service_id, directory_id, hash_id ) VALUES ( ?, ?, ? );', ( ( service_id, directory_id, hash_id ) for hash_id in hash_ids ) )
        
    
    def _TryToSortHashIds( self, location_context: ClientLocation.LocationContext, hash_ids, sort_by: ClientMedia.MediaSort ):
        
        did_sort = False
        
        ( sort_metadata, sort_data ) = sort_by.sort_type
        sort_order = sort_by.sort_order
        
        query = None
        
        if sort_metadata == 'system':
            
            simple_sorts = []
            
            simple_sorts.append( CC.SORT_FILES_BY_IMPORT_TIME )
            simple_sorts.append( CC.SORT_FILES_BY_FILESIZE )
            simple_sorts.append( CC.SORT_FILES_BY_DURATION )
            simple_sorts.append( CC.SORT_FILES_BY_FRAMERATE )
            simple_sorts.append( CC.SORT_FILES_BY_NUM_FRAMES )
            simple_sorts.append( CC.SORT_FILES_BY_WIDTH )
            simple_sorts.append( CC.SORT_FILES_BY_HEIGHT )
            simple_sorts.append( CC.SORT_FILES_BY_RATIO )
            simple_sorts.append( CC.SORT_FILES_BY_NUM_PIXELS )
            simple_sorts.append( CC.SORT_FILES_BY_MEDIA_VIEWS )
            simple_sorts.append( CC.SORT_FILES_BY_MEDIA_VIEWTIME )
            simple_sorts.append( CC.SORT_FILES_BY_APPROX_BITRATE )
            simple_sorts.append( CC.SORT_FILES_BY_FILE_MODIFIED_TIMESTAMP )
            simple_sorts.append( CC.SORT_FILES_BY_LAST_VIEWED_TIME )
            
            if sort_data in simple_sorts:
                
                if sort_data == CC.SORT_FILES_BY_IMPORT_TIME:
                    
                    if location_context.IsOneDomain():
                        
                        file_service_key = list( location_context.current_service_keys )[0]
                        
                    else:
                        
                        file_service_key = CC.COMBINED_LOCAL_FILE_SERVICE_KEY
                        
                    
                    file_service_id = self.modules_services.GetServiceId( file_service_key )
                    
                    current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( file_service_id, HC.CONTENT_STATUS_CURRENT )
                    
                    query = 'SELECT hash_id, timestamp FROM {} CROSS JOIN {} USING ( hash_id );'.format( '{}', current_files_table_name )
                    
                elif sort_data == CC.SORT_FILES_BY_FILESIZE:
                    
                    query = 'SELECT hash_id, size FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_DURATION:
                    
                    query = 'SELECT hash_id, duration FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_FRAMERATE:
                    
                    query = 'SELECT hash_id, num_frames, duration FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_NUM_FRAMES:
                    
                    query = 'SELECT hash_id, num_frames FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_WIDTH:
                    
                    query = 'SELECT hash_id, width FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_HEIGHT:
                    
                    query = 'SELECT hash_id, height FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_RATIO:
                    
                    query = 'SELECT hash_id, width, height FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_NUM_PIXELS:
                    
                    query = 'SELECT hash_id, width, height FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_MEDIA_VIEWS:
                    
                    query = 'SELECT hash_id, views FROM {} CROSS JOIN file_viewing_stats USING ( hash_id ) WHERE canvas_type = {};'.format( '{}', CC.CANVAS_MEDIA_VIEWER )
                    
                elif sort_data == CC.SORT_FILES_BY_MEDIA_VIEWTIME:
                    
                    query = 'SELECT hash_id, viewtime FROM {} CROSS JOIN file_viewing_stats USING ( hash_id ) WHERE canvas_type = {};'.format( '{}', CC.CANVAS_MEDIA_VIEWER )
                    
                elif sort_data == CC.SORT_FILES_BY_APPROX_BITRATE:
                    
                    query = 'SELECT hash_id, duration, num_frames, size, width, height FROM {} CROSS JOIN files_info USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_FILE_MODIFIED_TIMESTAMP:
                    
                    query = 'SELECT hash_id, file_modified_timestamp FROM {} CROSS JOIN file_modified_timestamps USING ( hash_id );'
                    
                elif sort_data == CC.SORT_FILES_BY_LAST_VIEWED_TIME:
                    
                    query = 'SELECT hash_id, last_viewed_timestamp FROM {} CROSS JOIN file_viewing_stats USING ( hash_id ) WHERE canvas_type = {};'.format( '{}', CC.CANVAS_MEDIA_VIEWER )
                    
                
                if sort_data == CC.SORT_FILES_BY_RATIO:
                    
                    def key( row ):
                        
                        width = row[1]
                        height = row[2]
                        
                        if width is None or height is None:
                            
                            return -1
                            
                        else:
                            
                            return width / height
                            
                        
                    
                elif sort_data == CC.SORT_FILES_BY_FRAMERATE:
                    
                    def key( row ):
                        
                        num_frames = row[1]
                        duration = row[2]
                        
                        if num_frames is None or duration is None or num_frames == 0 or duration == 0:
                            
                            return -1
                            
                        else:
                            
                            return num_frames / duration
                            
                        
                    
                elif sort_data == CC.SORT_FILES_BY_NUM_PIXELS:
                    
                    def key( row ):
                        
                        width = row[1]
                        height = row[2]
                        
                        if width is None or height is None or width == 0 or height == 0:
                            
                            return -1
                            
                        else:
                            
                            return width * height
                            
                        
                    
                elif sort_data == CC.SORT_FILES_BY_APPROX_BITRATE:
                    
                    def key( row ):
                        
                        duration = row[1]
                        num_frames = row[2]
                        size = row[3]
                        width = row[4]
                        height = row[5]
                        
                        if duration is None or duration == 0:
                            
                            if size is None or size == 0:
                                
                                duration_bitrate = -1
                                frame_bitrate = -1
                                
                            else:
                                
                                duration_bitrate = 0
                                
                                if width is None or height is None:
                                    
                                    frame_bitrate = 0
                                    
                                else:
                                    
                                    num_pixels = width * height
                                    
                                    if size is None or size == 0 or num_pixels == 0:
                                        
                                        frame_bitrate = -1
                                        
                                    else:
                                        
                                        frame_bitrate = size / num_pixels
                                        
                                    
                                
                            
                        else:
                            
                            if size is None or size == 0:
                                
                                duration_bitrate = -1
                                frame_bitrate = -1
                                
                            else:
                                
                                duration_bitrate = size / duration
                                
                                if num_frames is None or num_frames == 0:
                                    
                                    frame_bitrate = 0
                                    
                                else:
                                    
                                    frame_bitrate = duration_bitrate / num_frames
                                    
                                
                            
                        
                        return ( duration_bitrate, frame_bitrate )
                        
                    
                else:
                    
                    key = lambda row: -1 if row[1] is None else row[1]
                    
                
                reverse = sort_order == CC.SORT_DESC
                
            elif sort_data == CC.SORT_FILES_BY_RANDOM:
                
                hash_ids = list( hash_ids )
                
                random.shuffle( hash_ids )
                
                did_sort = True
                
            
        
        if query is not None:
            
            with self._MakeTemporaryIntegerTable( hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
                
                hash_ids_and_other_data = sorted( self._Execute( query.format( temp_hash_ids_table_name ) ), key = key, reverse = reverse )
                
            
            original_hash_ids = set( hash_ids )
            
            hash_ids = [ row[0] for row in hash_ids_and_other_data ]
            
            # some stuff like media views won't have rows
            missing_hash_ids = original_hash_ids.difference( hash_ids )
            
            hash_ids.extend( missing_hash_ids )
            
            did_sort = True
            
        
        return ( did_sort, hash_ids )
        
    
    def _UndeleteFiles( self, service_id, hash_ids ):
        
        rows = self.modules_files_storage.GetUndeleteRows( service_id, hash_ids )
        
        self._AddFiles( service_id, rows )
        
    
    def _UnloadModules( self ):
        
        del self.modules_hashes
        del self.modules_tags
        del self.modules_urls
        del self.modules_texts
        
        self._modules = []
        
    
    def _UpdateDB( self, version ):
        
        self._controller.frame_splash_status.SetText( 'updating db to v' + str( version + 1 ) )
        
        if version == 419:
            
            self._controller.frame_splash_status.SetSubtext( 'creating a couple of indices' )
            
            self._CreateIndex( 'tag_parents', [ 'service_id', 'parent_tag_id' ] )
            self._CreateIndex( 'tag_parent_petitions', [ 'service_id', 'parent_tag_id' ] )
            self._CreateIndex( 'tag_siblings', [ 'service_id', 'good_tag_id' ] )
            self._CreateIndex( 'tag_sibling_petitions', [ 'service_id', 'good_tag_id' ] )
            
            self.modules_db_maintenance.AnalyzeTable( 'tag_parents' )
            self.modules_db_maintenance.AnalyzeTable( 'tag_parent_petitions' )
            self.modules_db_maintenance.AnalyzeTable( 'tag_siblings' )
            self.modules_db_maintenance.AnalyzeTable( 'tag_sibling_petitions' )
            
            self._controller.frame_splash_status.SetSubtext( 'regenerating ideal siblings and parents' )
            
            try:
                
                self.modules_tag_display.RegenerateTagSiblingsAndParentsCache()
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to regen sibling lookups failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 423:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultParsers( ( 'e621 file page parser', ) )
                
                domain_manager.OverwriteDefaultURLClasses( ( 'nitter media timeline', 'nitter timeline' ) )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some parsers failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            #
            
            result_master = self._Execute( 'SELECT 1 FROM external_master.sqlite_master WHERE name = ?;', ( 'subtags_fts4', ) ).fetchone()
            result_caches = self._Execute( 'SELECT 1 FROM external_caches.sqlite_master WHERE name = ?;', ( 'subtags_fts4', ) ).fetchone()
            
            if result_master is not None or result_caches is not None:
                
                try:
                    
                    self._controller.frame_splash_status.SetText( 'dropping old cache - subtags fts4' )
                    
                    self._Execute( 'DROP TABLE IF EXISTS subtags_fts4;' )
                    
                    self._controller.frame_splash_status.SetText( 'dropping old cache - subtags searchable map' )
                    
                    self._Execute( 'DROP TABLE IF EXISTS subtags_searchable_map;' )
                    
                    self._controller.frame_splash_status.SetText( 'dropping old cache - integer subtags' )
                    
                    self._Execute( 'DROP TABLE IF EXISTS integer_subtags;' )
                    
                    self.modules_services.combined_file_service_id = self.modules_services.GetServiceId( CC.COMBINED_FILE_SERVICE_KEY )
                    
                    file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
                    tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                    
                    for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                        
                        self._controller.frame_splash_status.SetText( 'creating new specific cache - {} {}'.format( file_service_id, tag_service_id ) )
                        
                        self.modules_tag_search.Drop( file_service_id, tag_service_id )
                        
                        self.modules_tag_search.Generate( file_service_id, tag_service_id )
                        
                        self._CacheTagsPopulate( file_service_id, tag_service_id )
                        
                    
                    for tag_service_id in tag_service_ids:
                        
                        self._controller.frame_splash_status.SetText( 'creating new combined files cache - {}'.format( tag_service_id ) )
                        
                        self.modules_tag_search.Drop( self.modules_services.combined_file_service_id, tag_service_id )
                        
                        self.modules_tag_search.Generate( self.modules_services.combined_file_service_id, tag_service_id )
                        
                        self._CacheTagsPopulate( self.modules_services.combined_file_service_id, tag_service_id )
                        
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    raise Exception( 'The v424 cache update failed to work! The error has been printed to the log. Please rollback to 423 and let hydev know the details.' )
                    
                
            
        
        if version == 424:
            
            session_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER )
            
            if session_manager is None:
                
                try:
                    
                    legacy_session_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER_LEGACY )
                    
                    if legacy_session_manager is None:
                        
                        session_manager = ClientNetworkingSessions.NetworkSessionManager()
                        
                        session_manager.SetDirty()
                        
                        message = 'Hey, when updating your session manager to the new object, it seems the original was missing. I have created an empty new one, but it will have no cookies, so you will have to re-login as needed.'
                        
                        self.pub_initial_message( message )
                        
                    else:
                        
                        session_manager = ClientNetworkingSessionsLegacy.ConvertLegacyToNewSessions( legacy_session_manager )
                        
                        self.modules_serialisable.DeleteJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_SESSION_MANAGER_LEGACY )
                        
                    
                    self.modules_serialisable.SetJSONDump( session_manager )
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    raise Exception( 'The v425 session update failed to work! The error has been printed to the log. Please rollback to 424 and let hydev know the details.' )
                    
                
            
            bandwidth_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER )
            
            if bandwidth_manager is None:
                
                try:
                    
                    legacy_bandwidth_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER_LEGACY )
                    
                    if legacy_bandwidth_manager is None:
                        
                        bandwidth_manager = ClientNetworkingBandwidth.NetworkBandwidthManager()
                        
                        ClientDefaults.SetDefaultBandwidthManagerRules( bandwidth_manager )
                        
                        bandwidth_manager.SetDirty()
                        
                        message = 'Hey, when updating your bandwidth manager to the new object, it seems the original was missing. I have created an empty new one, but it will have no bandwidth record or saved rules.'
                        
                        self.pub_initial_message( message )
                        
                    else:
                        
                        bandwidth_manager = ClientNetworkingBandwidthLegacy.ConvertLegacyToNewBandwidth( legacy_bandwidth_manager )
                        
                        self.modules_serialisable.DeleteJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_BANDWIDTH_MANAGER_LEGACY )
                        
                    
                    self.modules_serialisable.SetJSONDump( bandwidth_manager )
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    raise Exception( 'The v425 bandwidth update failed to work! The error has been printed to the log. Please rollback to 424 and let hydev know the details.' )
                    
                
            
        
        if version == 425:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultParsers( ( 'gelbooru 0.2.x gallery page parser', 'e621 file page parser', 'gelbooru 0.2.5 file page parser' ) )
                
                domain_manager.OverwriteDefaultURLClasses( ( 'gelbooru gallery pool page', ) )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some parsers failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            message = 'You updated from an older version, so some automatic maintenance could not be run. Please run _database->regenerate->tag text search cache (subtags repopulation)_ for all services when you have some time.'
            
            self.pub_initial_message( message )
            
        
        if version == 426:
            
            try:
                
                self._RegenerateTagDisplayPendingMappingsCache()
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'The v427 pending tags regen routine failed! This is not super important, but hydev would be interested in seeing the error that was printed to the log.'
                
                self.pub_initial_message( message )
                
            
            try:
                
                from hydrus.client.gui import ClientGUIShortcuts
                
                shortcut_sets = ClientDefaults.GetDefaultShortcuts()
                
                try:
                    
                    tags_autocomplete = [ shortcut_set for shortcut_set in shortcut_sets if shortcut_set.GetName() == 'tags_autocomplete' ][0]
                    
                except Exception as e:
                    
                    tags_autocomplete = ClientGUIShortcuts.ShortcutSet( 'tags_autocomplete' )
                    
                
                main_gui = self.modules_serialisable.GetJSONDumpNamed( HydrusSerialisable.SERIALISABLE_TYPE_SHORTCUT_SET, dump_name = 'main_gui' )
                
                shortcuts = main_gui.GetShortcuts( CAC.SIMPLE_SYNCHRONISED_WAIT_SWITCH )
                
                for shortcut in shortcuts:
                    
                    tags_autocomplete.SetCommand( shortcut, CAC.ApplicationCommand.STATICCreateSimpleCommand( CAC.SIMPLE_SYNCHRONISED_WAIT_SWITCH ) )
                    
                    main_gui.DeleteShortcut( shortcut )
                    
                
                self.modules_serialisable.SetJSONDump( main_gui )
                self.modules_serialisable.SetJSONDump( tags_autocomplete )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'The v427 shortcut migrate failed! This is not super important, but hydev would be interested in seeing the error that was printed to the log. Check your \'main gui\' shortcuts if you want to set the migrated commands like \'force autocomplete search\'. I will now try to save an empty tag autocomplete shortcut set.'
                
                self.pub_initial_message( message )
                
                tags_autocomplete = ClientGUIShortcuts.ShortcutSet( 'tags_autocomplete' )
                
                self.modules_serialisable.SetJSONDump( tags_autocomplete )
                
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.DissolveParserLink( 'gelbooru gallery favorites page', 'gelbooru 0.2.5 file page parser' )
                domain_manager.DissolveParserLink( 'gelbooru gallery page', 'gelbooru 0.2.5 file page parser' )
                domain_manager.DissolveParserLink( 'gelbooru gallery pool page', 'gelbooru 0.2.5 file page parser' )
                domain_manager.DissolveParserLink( 'gelbooru file page', 'gelbooru 0.2.x gallery page parser' )
                
                #
                
                domain_manager.OverwriteDefaultParsers( ( 'gelbooru 0.2.5 file page parser', ) )
                
                #
                
                domain_manager.OverwriteDefaultURLClasses( ( '420chan thread new format', ) )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some parsers failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 427:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultGUGs( [
                    'nitter (.eu mirror) media lookup',
                    'nitter (.eu mirror) retweets lookup',
                    'nitter (nixnet mirror) media lookup',
                    'nitter (nixnet mirror) retweets lookup'
                ] )
                
                #
                
                domain_manager.OverwriteDefaultURLClasses( [
                    'nitter (.eu mirror) media timeline',
                    'nitter (.eu mirror) timeline',
                    'nitter (.eu mirror) tweet media',
                    'nitter (.eu mirror) tweet',
                    'nitter (nixnet mirror) media timeline',
                    'nitter (nixnet mirror) timeline',
                    'nitter (nixnet mirror) tweet media',
                    'nitter (nixnet mirror) tweet'
                ] )
                
                #
                
                domain_manager.OverwriteDefaultParsers( [
                    'nitter media parser',
                    'nitter retweet parser',
                    'nitter tweet parser',
                    'nitter tweet parser (video from koto.reisen)'
                ] )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update nitter mirrors failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 428:
            
            try:
                
                self.modules_hashes_local_cache.CreateInitialTables()
                self.modules_hashes_local_cache.CreateInitialIndices()
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                raise Exception( 'Could not create the new local hashes cache! The error has been printed to the log, please let hydev know!' )
                
            
            # took out local hash regen here due to later file service splitting, which regens local hash cache anyway
            
        
        if version == 429:
            
            try:
                
                tag_service_ids = set( self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES ) )
                
                file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
                file_service_ids.add( self.modules_services.combined_file_service_id )
                
                for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                    
                    subtags_searchable_map_table_name = self.modules_tag_search.GetSubtagsSearchableMapTableName( file_service_id, tag_service_id )
                    
                    self._Execute( 'CREATE TABLE IF NOT EXISTS {} ( subtag_id INTEGER PRIMARY KEY, searchable_subtag_id INTEGER );'.format( subtags_searchable_map_table_name ) )
                    self._CreateIndex( subtags_searchable_map_table_name, [ 'searchable_subtag_id' ] )
                    
                
                self._RegenerateTagCacheSearchableSubtagMaps()
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                raise Exception( 'The v430 subtag searchable map generation routine failed! The error has been printed to the log, please let hydev know!' )
                
            
        
        if version == 430:
            
            try:
                
                # due to a bug in over-eager deletion from the tag definition cache, we'll need to resync chained tag ids
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
                for tag_service_id in tag_service_ids:
                    
                    message = 'fixing up some desynchronised tag definitions: {}'.format( tag_service_id )
                    
                    self._controller.frame_splash_status.SetSubtext( message )
                    
                    ( cache_ideal_tag_siblings_lookup_table_name, cache_actual_tag_siblings_lookup_table_name ) = ClientDBTagSiblings.GenerateTagSiblingsLookupCacheTableNames( tag_service_id )
                    ( cache_ideal_tag_parents_lookup_table_name, cache_actual_tag_parents_lookup_table_name ) = ClientDBTagParents.GenerateTagParentsLookupCacheTableNames( tag_service_id )
                    
                    tag_ids_in_dispute = set()
                    
                    tag_ids_in_dispute.update( self._STS( self._Execute( 'SELECT DISTINCT bad_tag_id FROM {};'.format( cache_actual_tag_siblings_lookup_table_name ) ) ) )
                    tag_ids_in_dispute.update( self._STS( self._Execute( 'SELECT ideal_tag_id FROM {};'.format( cache_actual_tag_siblings_lookup_table_name ) ) ) )
                    tag_ids_in_dispute.update( self._STS( self._Execute( 'SELECT DISTINCT child_tag_id FROM {};'.format( cache_actual_tag_parents_lookup_table_name ) ) ) )
                    tag_ids_in_dispute.update( self._STS( self._Execute( 'SELECT DISTINCT ancestor_tag_id FROM {};'.format( cache_actual_tag_parents_lookup_table_name ) ) ) )
                    
                    if len( tag_ids_in_dispute ) > 0:
                        
                        self._CacheTagsSyncTags( tag_service_id, tag_ids_in_dispute )
                        
                    
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to resync some tag definitions failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultParsers( [
                    '8chan.moe thread api parser',
                    'e621 file page parser'
                ] )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some parsers failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 431:
            
            try:
                
                new_options = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_CLIENT_OPTIONS )
                
                old_options = self._GetOptions()
                
                SORT_BY_LEXICOGRAPHIC_ASC = 8
                SORT_BY_LEXICOGRAPHIC_DESC = 9
                SORT_BY_INCIDENCE_ASC = 10
                SORT_BY_INCIDENCE_DESC = 11
                SORT_BY_LEXICOGRAPHIC_NAMESPACE_ASC = 12
                SORT_BY_LEXICOGRAPHIC_NAMESPACE_DESC = 13
                SORT_BY_INCIDENCE_NAMESPACE_ASC = 14
                SORT_BY_INCIDENCE_NAMESPACE_DESC = 15
                SORT_BY_LEXICOGRAPHIC_IGNORE_NAMESPACE_ASC = 16
                SORT_BY_LEXICOGRAPHIC_IGNORE_NAMESPACE_DESC = 17
                
                old_default_tag_sort = old_options[ 'default_tag_sort' ]
                
                from hydrus.client.metadata import ClientTagSorting
                
                sort_type = ClientTagSorting.SORT_BY_HUMAN_TAG
                
                if old_default_tag_sort in ( SORT_BY_LEXICOGRAPHIC_ASC, SORT_BY_LEXICOGRAPHIC_DESC, SORT_BY_LEXICOGRAPHIC_NAMESPACE_ASC, SORT_BY_LEXICOGRAPHIC_NAMESPACE_ASC ):
                    
                    sort_type = ClientTagSorting.SORT_BY_HUMAN_TAG
                    
                elif old_default_tag_sort in ( SORT_BY_LEXICOGRAPHIC_IGNORE_NAMESPACE_ASC, SORT_BY_LEXICOGRAPHIC_IGNORE_NAMESPACE_DESC ):
                    
                    sort_type = ClientTagSorting.SORT_BY_HUMAN_SUBTAG
                    
                elif old_default_tag_sort in ( SORT_BY_INCIDENCE_ASC, SORT_BY_INCIDENCE_DESC, SORT_BY_INCIDENCE_NAMESPACE_ASC, SORT_BY_INCIDENCE_NAMESPACE_DESC ):
                    
                    sort_type = ClientTagSorting.SORT_BY_COUNT
                    
                
                if old_default_tag_sort in ( SORT_BY_INCIDENCE_ASC, SORT_BY_INCIDENCE_NAMESPACE_ASC, SORT_BY_LEXICOGRAPHIC_ASC, SORT_BY_LEXICOGRAPHIC_IGNORE_NAMESPACE_ASC, SORT_BY_LEXICOGRAPHIC_NAMESPACE_ASC ):
                    
                    sort_order = CC.SORT_ASC
                    
                else:
                    
                    sort_order = CC.SORT_DESC
                    
                
                use_siblings = True
                
                if old_default_tag_sort in ( SORT_BY_INCIDENCE_NAMESPACE_ASC, SORT_BY_INCIDENCE_NAMESPACE_DESC, SORT_BY_LEXICOGRAPHIC_NAMESPACE_ASC, SORT_BY_LEXICOGRAPHIC_NAMESPACE_DESC ):
                    
                    group_by = ClientTagSorting.GROUP_BY_NAMESPACE
                    
                else:
                    
                    group_by = ClientTagSorting.GROUP_BY_NOTHING
                    
                
                tag_sort = ClientTagSorting.TagSort(
                    sort_type = sort_type,
                    sort_order = sort_order,
                    use_siblings = use_siblings,
                    group_by = group_by
                )
                
                new_options.SetDefaultTagSort( tag_sort )
                
                self.modules_serialisable.SetJSONDump( new_options )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to convert your old default tag sort to the new format failed! Please set it again in the options.'
                
                self.pub_initial_message( message )
                
            
        
        if version == 432:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultGUGs( [
                    'twitter syndication profile lookup (limited) (with replies)',
                    'twitter syndication profile lookup (limited)'
                ] )
                
                #
                
                domain_manager.OverwriteDefaultURLClasses( [
                    'twitter syndication api profile',
                    'twitter syndication api tweet',
                    'twitter tweet'
                ] )
                
                #
                
                domain_manager.OverwriteDefaultParsers( [
                    'twitter syndication api profile parser',
                    'twitter syndication api tweet parser'
                ] )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to add the twitter downloader failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 435:
            
            try:
                
                self._RegenerateTagPendingMappingsCache()
                
                types_to_delete = (
                    HC.SERVICE_INFO_NUM_PENDING_MAPPINGS,
                    HC.SERVICE_INFO_NUM_PENDING_TAG_SIBLINGS,
                    HC.SERVICE_INFO_NUM_PENDING_TAG_PARENTS,
                    HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS,
                    HC.SERVICE_INFO_NUM_PETITIONED_TAG_SIBLINGS,
                    HC.SERVICE_INFO_NUM_PETITIONED_TAG_PARENTS,
                    HC.SERVICE_INFO_NUM_PENDING_FILES,
                    HC.SERVICE_INFO_NUM_PETITIONED_FILES
                )
                
                self._DeleteServiceInfo( types_to_delete = types_to_delete )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to regenerate the pending tag cache failed! This is not a big deal, but you might still have a bad pending count for your pending menu. Error information has been written to the log. Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 436:
            
            result = self._Execute( 'SELECT sql FROM sqlite_master WHERE name = ?;', ( 'deleted_files', ) ).fetchone()
            
            if result is None:
                
                raise Exception( 'No deleted_files table!!!' )
                
            
            ( s, ) = result
            
            if 'timestamp' not in s:
                
                self._Execute( 'ALTER TABLE deleted_files ADD COLUMN timestamp INTEGER;' )
                self._Execute( 'ALTER TABLE deleted_files ADD COLUMN original_timestamp INTEGER;' )
                
                self._Execute( 'UPDATE deleted_files SET timestamp = ?, original_timestamp = ?;', ( None, None ) )
                
                my_files_service_id = self.modules_services.GetServiceId( CC.LOCAL_FILE_SERVICE_KEY )
                
                self._Execute( 'INSERT OR IGNORE INTO deleted_files ( service_id, hash_id, timestamp, original_timestamp ) SELECT ?, hash_id, timestamp, original_timestamp FROM deleted_files WHERE service_id = ?;', ( my_files_service_id, self.modules_services.combined_local_file_service_id ) )
                self._Execute( 'INSERT OR IGNORE INTO deleted_files ( service_id, hash_id, timestamp, original_timestamp ) SELECT ?, hash_id, ?, timestamp FROM current_files WHERE service_id = ?;', ( my_files_service_id, None, self.modules_services.trash_service_id ) )
                
                self._CreateIndex( 'deleted_files', [ 'timestamp' ] )
                self._CreateIndex( 'deleted_files', [ 'original_timestamp' ] )
                
                self._Execute( 'DELETE FROM service_info WHERE info_type = ?;', ( HC.SERVICE_INFO_NUM_DELETED_FILES, ) )
                
                self.modules_db_maintenance.AnalyzeTable( 'deleted_files' )
                
            
        
        if version == 438:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultURLClasses( ( 'imgur single media file url', ) )
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some url classes failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 440:
            
            try:
                
                old_options = self._GetOptions()
                
                if 'sort_by' in old_options:
                    
                    old_sort_by = old_options[ 'sort_by' ]
                    
                    new_options = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_CLIENT_OPTIONS )
                    
                    default_namespace_sorts = [ ClientMedia.MediaSort( sort_type = ( 'namespaces', ( namespaces, ClientTags.TAG_DISPLAY_ACTUAL ) ) ) for ( gumpf, namespaces ) in old_sort_by ]
                    
                    new_options.SetDefaultNamespaceSorts( default_namespace_sorts )
                    
                    self.modules_serialisable.SetJSONDump( new_options )
                    
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to migrate the old default namespace sorts failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultURLClasses( ( 'pixiv artist page (new format)', ) )
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some url classes failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 441:
            
            result = self._Execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( 'json_dumps_hashed', ) ).fetchone()
            
            if result is None:
                
                self._controller.frame_splash_status.SetSubtext( 'doing pre-update free space check' )
                
                legacy_dump_type = HydrusSerialisable.SERIALISABLE_TYPE_GUI_SESSION_LEGACY
                
                result = self._Execute( 'SELECT SUM( LENGTH( dump ) ) FROM json_dumps_named WHERE dump_type = ?;', ( legacy_dump_type, ) ).fetchone()
                
                if result is None or result[0] is None:
                    
                    raise Exception( 'Hey, for the v442 update step, I am supposed to be converting your sessions to a new object, but it did not seem like there were any! I am not sure what is going on, so the update will now be abandoned. Please roll back to v441 and let hydev know!' )
                    
                
                ( space_needed, ) = result
                
                space_needed /= 2 # most sessions will have backups and shared pages will save space in the end
                
                try:
                    
                    HydrusDBBase.CheckHasSpaceForDBTransaction( self._db_dir, space_needed )
                    
                except Exception as e:
                    
                    message = 'Hey, for the v442 update step, I am supposed to be converting your sessions to a new object, but there was a problem. It looks like you have very large sessions, and I do not think you have enough free disk space to perform the conversion safely. If you OK this dialog, it will be attempted anyway, but be warned: you may run out of space mid-update and then have serious problems. I recommend you kill the hydrus process NOW and then free up some space before trying again. Please check the full error:'
                    message += os.linesep * 2
                    message += str( e )
                    
                    BlockingSafeShowMessage( message )
                    
                
                one_worked_ok = False
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS json_dumps_hashed ( hash BLOB_BYTES PRIMARY KEY, dump_type INTEGER, version INTEGER, dump BLOB_BYTES );' )
                
                names_and_timestamps = self._Execute( 'SELECT dump_name, timestamp FROM json_dumps_named WHERE dump_type = ?;', ( legacy_dump_type, ) ).fetchall()
                
                from hydrus.client.gui.pages import ClientGUISessionLegacy
                
                import json
                
                for ( i, ( name, timestamp ) ) in enumerate( names_and_timestamps ):
                    
                    self._controller.frame_splash_status.SetSubtext( 'converting "{}" "{}"\u2026'.format( name, HydrusData.ConvertTimestampToPrettyTime( timestamp ) ) )
                    
                    ( dump_version, dump ) = self._Execute( 'SELECT version, dump FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', ( legacy_dump_type, name, timestamp ) ).fetchone()
                    
                    try:
                        
                        if isinstance( dump, bytes ):
                            
                            dump = str( dump, 'utf-8' )
                            
                        
                        serialisable_info = json.loads( dump )
                        
                        legacy_session = HydrusSerialisable.CreateFromSerialisableTuple( ( legacy_dump_type, name, dump_version, serialisable_info ) )
                        
                    except Exception as e:
                        
                        HydrusData.PrintException( e, do_wait = False )
                        
                        try:
                            
                            timestamp_string = time.strftime( '%Y-%m-%d %H-%M-%S' )
                            
                            filename = '({}, {}) at {}.json'.format( name, timestamp, timestamp_string )
                            
                            path = os.path.join( self._db_dir, filename )
                            
                            with open( path, 'wb' ) as f:
                                
                                if isinstance( dump, str ):
                                    
                                    dump = bytes( dump, 'utf-8', errors = 'replace' )
                                    
                                
                                f.write( dump )
                                
                            
                        except Exception as e:
                            
                            pass
                            
                        
                        message = 'When updating sessions, "{}" at "{}" was non-loadable/convertable! I tried to save a backup of the object to your database directory.'.format( name, HydrusData.ConvertTimestampToPrettyTime( timestamp ) )
                        
                        HydrusData.Print( message )
                        
                        self.pub_initial_message( message )
                        
                        continue
                        
                    
                    session = ClientGUISessionLegacy.ConvertLegacyToNew( legacy_session )
                    
                    self.modules_serialisable.SetJSONDump( session, force_timestamp = timestamp )
                    
                    self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ? AND timestamp = ?;', ( legacy_dump_type, name, timestamp ) )
                    
                    one_worked_ok = True
                    
                
                if not one_worked_ok:
                    
                    raise Exception( 'When trying to update your sessions to the new format, none of them converted correctly! Rather than send you into an empty and potentially non-functional client, the update is now being abandoned. Please roll back to v441 and let hydev know!' )
                    
                
                self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ?;', ( legacy_dump_type, ) )
                
                self._controller.frame_splash_status.SetSubtext( 'session converting finished' )
                
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultParsers( ( 'yande.re post page parser', 'moebooru file page parser' ) )
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some url classes failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 442:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteParserLink( 'yande.re file page', 'yande.re post page parser' )
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some url classes failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 446:
            
            result = self._Execute( 'SELECT 1 FROM json_dumps_named WHERE dump_type = ? AND dump_name = ?;', ( 32, 'gelbooru md5' ) ).fetchone()
            
            if result is not None:
                
                try:
                    
                    self._Execute( 'DELETE FROM json_dumps_named WHERE dump_type = ? AND dump_name = ?;', ( 32, 'gelbooru md5' ) )
                    
                    script_info = ( 32, 'gelbooru md5', 2, HydrusData.GetNow(), '''["http://gelbooru.com/index.php", 0, 1, [55, 1, [[[4, "hex"]], "some hash bytes"]], "md5", {"s": "list", "page": "post"}, [[30, 6, ["we got sent back to main gallery page -- title test", 8, [27, 7, [[26, 1, [[62, 2, [0, "head", {}, 0, null, false, [51, 1, [3, "", null, null, "example string"]]]], [62, 2, [0, "title", {}, 0, null, false, [51, 1, [3, "", null, null, "example string"]]]]]], 1, "", [84, 1, [26, 1, []]]]], [true, [51, 1, [2, "Image List", null, null, "Image List"]]]]], [30, 6, ["", 0, [27, 7, [[26, 1, [[62, 2, [0, "li", {"class": "tag-type-general"}, null, null, false, [51, 1, [3, "", null, null, "example string"]]]], [62, 2, [0, "a", {}, 1, null, false, [51, 1, [3, "", null, null, "example string"]]]]]], 1, "", [84, 1, [26, 1, []]]]], ""]], [30, 6, ["", 0, [27, 7, [[26, 1, [[62, 2, [0, "li", {"class": "tag-type-copyright"}, null, null, false, [51, 1, [3, "", null, null, "example string"]]]], [62, 2, [0, "a", {}, 1, null, false, [51, 1, [3, "", null, null, "example string"]]]]]], 1, "", [84, 1, [26, 1, []]]]], "series"]], [30, 6, ["", 0, [27, 7, [[26, 1, [[62, 2, [0, "li", {"class": "tag-type-artist"}, null, null, false, [51, 1, [3, "", null, null, "example string"]]]], [62, 2, [0, "a", {}, 1, null, false, [51, 1, [3, "", null, null, "example string"]]]]]], 1, "", [84, 1, [26, 1, []]]]], "creator"]], [30, 6, ["", 0, [27, 7, [[26, 1, [[62, 2, [0, "li", {"class": "tag-type-character"}, null, null, false, [51, 1, [3, "", null, null, "example string"]]]], [62, 2, [0, "a", {}, 1, null, false, [51, 1, [3, "", null, null, "example string"]]]]]], 1, "", [84, 1, [26, 1, []]]]], "character"]], [30, 6, ["we got sent back to main gallery page -- page links exist", 8, [27, 7, [[26, 1, [[62, 2, [0, "div", {"id": "paginator"}, null, null, false, [51, 1, [3, "", null, null, "example string"]]]], [62, 2, [0, "a", {}, null, null, false, [51, 1, [3, "", null, null, "example string"]]]]]], 2, "class", [84, 1, [26, 1, []]]]], [true, [51, 1, [3, "", null, null, "pagination"]]]]]]]''' )
                    
                    self._Execute( 'REPLACE INTO json_dumps_named VALUES ( ?, ?, ?, ?, ? );', script_info )
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    message = 'Trying to update gelbooru file lookup script failed! Please let hydrus dev know!'
                    
                    self.pub_initial_message( message )
                    
                
            
            #
            
            result = self._Execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( 'current_files', ) ).fetchone()
            
            if result is not None:
                
                try:
                    
                    service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
                    
                    for ( i, service_id ) in enumerate( service_ids ):
                        
                        self._controller.frame_splash_status.SetSubtext( 'reorganising file storage {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, len( service_ids ) ) ) )
                        
                        self.modules_files_storage.GenerateFilesTables( service_id )
                        
                        ( current_files_table_name, deleted_files_table_name, pending_files_table_name, petitioned_files_table_name ) = ClientDBFilesStorage.GenerateFilesTableNames( service_id )
                        
                        self._Execute( 'INSERT INTO {} ( hash_id, timestamp ) SELECT hash_id, timestamp FROM current_files WHERE service_id = ?;'.format( current_files_table_name ), ( service_id, ) )
                        self._Execute( 'INSERT INTO {} ( hash_id, timestamp, original_timestamp ) SELECT hash_id, timestamp, original_timestamp FROM deleted_files WHERE service_id = ?;'.format( deleted_files_table_name ), ( service_id, ) )
                        self._Execute( 'INSERT INTO {} ( hash_id ) SELECT hash_id FROM file_transfers WHERE service_id = ?;'.format( pending_files_table_name ), ( service_id, ) )
                        self._Execute( 'INSERT INTO {} ( hash_id, reason_id ) SELECT hash_id, reason_id FROM file_petitions WHERE service_id = ?;'.format( petitioned_files_table_name ), ( service_id, ) )
                        
                        self.modules_db_maintenance.TouchAnalyzeNewTables()
                        
                    
                    self._Execute( 'DROP TABLE current_files;' )
                    self._Execute( 'DROP TABLE deleted_files;' )
                    self._Execute( 'DROP TABLE file_transfers;' )
                    self._Execute( 'DROP TABLE file_petitions;' )
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    raise Exception( 'Unfortunately, hydrus was unable to update your file storage to the new system! The error has been written to your log, please roll back to v446 and let hydev know!' )
                    
                
            
            #
            
            self.modules_hashes_local_cache.Repopulate()
            
        
        if version == 447:
            
            try:
                
                self._controller.frame_splash_status.SetSubtext( 'scheduling PSD files for thumbnail regen' )
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime = ?;'.format( table_join ), ( HC.APPLICATION_PSD, ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FORCE_THUMBNAIL )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to schedule PSD files for thumbnail generation failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 448:
            
            self._controller.frame_splash_status.SetSubtext( 'updating repository update storage' )
            
            for service_id in self.modules_services.GetServiceIds( HC.REPOSITORIES ):
                
                service_type = self.modules_services.GetService( service_id ).GetServiceType()
                
                ( repository_updates_table_name, repository_unregistered_updates_table_name, repository_updates_processed_table_name ) = ClientDBRepositories.GenerateRepositoryUpdatesTableNames( service_id )
                
                result = self._Execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( repository_unregistered_updates_table_name, ) ).fetchone()
                
                if result is not None:
                    
                    continue
                    
                
                all_data = self._Execute( 'SELECT update_index, hash_id, processed FROM {};'.format( repository_updates_table_name ) ).fetchall()
                
                self._Execute( 'DROP TABLE {};'.format( repository_updates_table_name ) )
                
                #
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS {} ( update_index INTEGER, hash_id INTEGER, PRIMARY KEY ( update_index, hash_id ) );'.format( repository_updates_table_name ) )
                self._CreateIndex( repository_updates_table_name, [ 'hash_id' ] )   
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS {} ( hash_id INTEGER PRIMARY KEY );'.format( repository_unregistered_updates_table_name ) )
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS {} ( hash_id INTEGER, content_type INTEGER, processed INTEGER_BOOLEAN, PRIMARY KEY ( hash_id, content_type ) );'.format( repository_updates_processed_table_name ) )
                self._CreateIndex( repository_updates_processed_table_name, [ 'content_type' ] )
                
                #
                
                for ( update_index, hash_id, processed ) in all_data:
                    
                    self._Execute( 'INSERT OR IGNORE INTO {} ( update_index, hash_id ) VALUES ( ?, ? );'.format( repository_updates_table_name ), ( update_index, hash_id ) )
                    
                    try:
                        
                        mime = self.modules_files_metadata_basic.GetMime( hash_id )
                        
                    except HydrusExceptions.DataMissing:
                        
                        self._Execute( 'INSERT OR IGNORE INTO {} ( hash_id ) VALUES ( ? );'.format( repository_unregistered_updates_table_name ), ( hash_id, ) )
                        
                        continue
                        
                    
                    if mime == HC.APPLICATION_HYDRUS_UPDATE_DEFINITIONS:
                        
                        content_types = ( HC.CONTENT_TYPE_DEFINITIONS, )
                        
                    else:
                        
                        if service_type == HC.FILE_REPOSITORY:
                            
                            content_types = ( HC.CONTENT_TYPE_FILES, )
                            
                        else:
                            
                            content_types = ( HC.CONTENT_TYPE_MAPPINGS, HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_TYPE_TAG_SIBLINGS )
                            
                        
                    
                    self._ExecuteMany( 'INSERT OR IGNORE INTO {} ( hash_id, content_type, processed ) VALUES ( ?, ?, ? );'.format( repository_updates_processed_table_name ), ( ( hash_id, content_type, processed ) for content_type in content_types ) )
                    
                
            
            self.modules_repositories.DoOutstandingUpdateRegistration()
            
            self._controller.frame_splash_status.SetSubtext( 'resetting siblings and parents' )
            
            for service in self.modules_services.GetServices( ( HC.TAG_REPOSITORY, ) ):
                
                service_key = service.GetServiceKey()
                
                self._ResetRepositoryProcessing( service_key, ( HC.CONTENT_TYPE_TAG_PARENTS, HC.CONTENT_TYPE_TAG_SIBLINGS ) )
                
            
        
        if version == 450:
            
            result = self._c.execute( 'SELECT 1 FROM external_caches.sqlite_master WHERE name = ?;', ( 'shape_perceptual_hashes', ) ).fetchone()
            
            if result is not None:
                
                self._controller.frame_splash_status.SetSubtext( 'moving some similar file data around' )
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS external_master.shape_perceptual_hashes ( phash_id INTEGER PRIMARY KEY, phash BLOB_BYTES UNIQUE );' )
                self._Execute( 'CREATE TABLE IF NOT EXISTS external_master.shape_perceptual_hash_map ( phash_id INTEGER, hash_id INTEGER, PRIMARY KEY ( phash_id, hash_id ) );' )
                self._Execute( 'CREATE TABLE IF NOT EXISTS shape_search_cache ( hash_id INTEGER PRIMARY KEY, searched_distance INTEGER );' )
                
                self._Execute( 'INSERT OR IGNORE INTO external_master.shape_perceptual_hashes SELECT phash_id, phash FROM external_caches.shape_perceptual_hashes;' )
                self._Execute( 'INSERT OR IGNORE INTO external_master.shape_perceptual_hash_map SELECT phash_id, hash_id FROM external_caches.shape_perceptual_hash_map;' )
                self._Execute( 'INSERT OR IGNORE INTO main.shape_search_cache SELECT hash_id, searched_distance FROM external_caches.shape_search_cache;' )
                
                self._Execute( 'DROP TABLE external_caches.shape_perceptual_hashes;' )
                self._Execute( 'DROP TABLE external_caches.shape_perceptual_hash_map;' )
                self._Execute( 'DROP TABLE external_caches.shape_search_cache;' )
                
                self._CreateIndex( 'external_master.shape_perceptual_hash_map', [ 'hash_id' ] )
                
                self.modules_db_maintenance.TouchAnalyzeNewTables()
                
            
        
        if version == 451:
            
            self.modules_services.combined_file_service_id = self.modules_services.GetServiceId( CC.COMBINED_FILE_SERVICE_KEY )
            
            file_service_ids = list( self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES ) )
            file_service_ids.append( self.modules_services.combined_file_service_id )
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                if file_service_id == self.modules_services.combined_file_service_id:
                    
                    self._controller.frame_splash_status.SetText( 'working on combined tags cache - {}'.format( tag_service_id ) )
                    
                else:
                    
                    self._controller.frame_splash_status.SetText( 'working on specific tags cache - {} {}'.format( file_service_id, tag_service_id ) )
                    
                
                tags_table_name = self.modules_tag_search.GetTagsTableName( file_service_id, tag_service_id )
                integer_subtags_table_name = self.modules_tag_search.GetIntegerSubtagsTableName( file_service_id, tag_service_id )
                
                query = 'SELECT subtag_id FROM {};'.format( tags_table_name )
                
                BLOCK_SIZE = 10000
                
                for ( group_of_subtag_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, query, BLOCK_SIZE ):
                    
                    message = HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do )
                    
                    self._controller.frame_splash_status.SetSubtext( message )
                    
                    with self._MakeTemporaryIntegerTable( group_of_subtag_ids, 'subtag_id' ) as temp_subtag_ids_table_name:
                        
                        # temp subtag_ids to subtags
                        subtag_ids_and_subtags = self._Execute( 'SELECT subtag_id, subtag FROM {} CROSS JOIN subtags USING ( subtag_id );'.format( temp_subtag_ids_table_name ) ).fetchall()
                        
                        for ( subtag_id, subtag ) in subtag_ids_and_subtags:
                            
                            if subtag.isdecimal():
                                
                                try:
                                    
                                    integer_subtag = int( subtag )
                                    
                                    if ClientDBTagSearch.CanCacheInteger( integer_subtag ):
                                        
                                        self._Execute( 'INSERT OR IGNORE INTO {} ( subtag_id, integer_subtag ) VALUES ( ?, ? );'.format( integer_subtags_table_name ), ( subtag_id, integer_subtag ) )
                                        
                                    
                                except ValueError:
                                    
                                    pass
                                    
                                
                            
                        
                    
                
            
        
        if version == 452:
            
            file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_TAG_LOOKUP_CACHES )
            
            tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
            
            for ( file_service_id, tag_service_id ) in itertools.product( file_service_ids, tag_service_ids ):
                
                suffix = '{}_{}'.format( file_service_id, tag_service_id )
                
                cache_files_table_name = 'external_caches.specific_files_cache_{}'.format( suffix )
                
                result = self._Execute( 'SELECT 1 FROM external_caches.sqlite_master WHERE name = ?;', ( cache_files_table_name.split( '.', 1 )[1], ) ).fetchone()
                
                if result is None:
                    
                    continue
                    
                
                self._controller.frame_splash_status.SetText( 'filling holes in specific tags cache - {} {}'.format( file_service_id, tag_service_id ) )
                
                # it turns out cache_files_table_name was not being populated on service creation/reset, so files imported before a tag service was created were not being stored in specific mapping cache data!
                # furthermore, there was confusion whether cache_files_table_name was for mappings (files that have tags) on the tag service or just files on the file service.
                # since we now store current files for each file service on a separate table, and the clever mappings intepretation seems expensive and not actually so useful, we are moving to our nice table instead in various joins/filters/etc...
                
                current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( file_service_id, HC.CONTENT_STATUS_CURRENT )
                
                query = 'SELECT hash_id FROM {} EXCEPT SELECT hash_id FROM {};'.format( current_files_table_name, cache_files_table_name )
                
                BLOCK_SIZE = 10000
                
                for ( group_of_hash_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, query, BLOCK_SIZE ):
                    
                    message = HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do )
                    
                    self._controller.frame_splash_status.SetSubtext( message )
                    
                    with self._MakeTemporaryIntegerTable( group_of_hash_ids, 'hash_id' ) as temp_hash_ids_table_name:
                        
                        self._CacheSpecificMappingsAddFiles( file_service_id, tag_service_id, group_of_hash_ids, temp_hash_ids_table_name )
                        self.modules_mappings_cache_specific_display.AddFiles( file_service_id, tag_service_id, group_of_hash_ids, temp_hash_ids_table_name )
                        
                    
                
                self._Execute( 'DROP TABLE {};'.format( cache_files_table_name ) )
                
            
        
        if version == 459:
            
            try:
                
                self._controller.frame_splash_status.SetSubtext( 'scheduling clip and apng files for regen' )
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime = ?;'.format( table_join ), ( HC.APPLICATION_CLIP, ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_METADATA )
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FORCE_THUMBNAIL )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime = ?;'.format( table_join ), ( HC.IMAGE_APNG, ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_METADATA )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to schedule clip and apng files for maintenance failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 460:
            
            try:
                
                self._controller.frame_splash_status.SetSubtext( 'scheduling clip files for regen' )
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime = ?;'.format( table_join ), ( HC.APPLICATION_CLIP, ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_METADATA )
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_REFIT_THUMBNAIL )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE duration > ? AND size < ? AND width >= ? AND height >= ?;'.format( table_join ), ( 3600 * 1000, 64 * 1048576, 480, 360 ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_METADATA )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to schedule clip files for maintenance failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 461:
            
            try:
                
                num_rating_services = len( self.modules_services.GetServiceIds( HC.RATINGS_SERVICES ) )
                
                if num_rating_services == 0:
                    
                    def ask_what_to_do_ratings_service():
                        
                        message = 'New clients now start with a simple like/dislike rating service. You are not new, but you have no rating services--would you like to get this default now and try ratings out?'
                        
                        from hydrus.client.gui import ClientGUIDialogsQuick
                        
                        result = ClientGUIDialogsQuick.GetYesNo( None, message, title = 'Get rating service?' )
                        
                        return result == QW.QDialog.Accepted
                        
                    
                    add_favourites = self._controller.CallBlockingToQt( None, ask_what_to_do_ratings_service )
                    
                    if add_favourites:
                        
                        ( service_key, service_type, name ) = ( CC.DEFAULT_FAVOURITES_RATING_SERVICE_KEY, HC.LOCAL_RATING_LIKE, 'favourites' )
                        
                        dictionary = ClientServices.GenerateDefaultServiceDictionary( service_type )
                        
                        from hydrus.client.metadata import ClientRatings
                        
                        dictionary[ 'shape' ] = ClientRatings.STAR
                        
                        like_colours = {}
                        
                        like_colours[ ClientRatings.LIKE ] = ( ( 0, 0, 0 ), ( 240, 240, 65 ) )
                        like_colours[ ClientRatings.DISLIKE ] = ( ( 0, 0, 0 ), ( 200, 80, 120 ) )
                        like_colours[ ClientRatings.NULL ] = ( ( 0, 0, 0 ), ( 191, 191, 191 ) )
                        like_colours[ ClientRatings.MIXED ] = ( ( 0, 0, 0 ), ( 95, 95, 95 ) )
                        
                        dictionary[ 'colours' ] = list( like_colours.items() )
                        
                        self._AddService( service_key, service_type, name, dictionary )
                        
                    
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to add a default favourites service failed. Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            #
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultParsers( ( 'pixiv artist gallery page api parser new urls' ) )
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some downloader objects failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 462:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultGUGs( ( 'deviant art tag search', ) )
                
                domain_manager.OverwriteDefaultParsers( ( 'deviant gallery page api parser (new cursor)', ) )
                
                domain_manager.OverwriteDefaultURLClasses( ( 'deviant art tag gallery page api (cursor navigation)', ) )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some parsers failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            try:
                
                self._controller.frame_splash_status.SetSubtext( 'scheduling ogg files for regen' )
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime = ?;'.format( table_join ), ( HC.AUDIO_OGG, ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_METADATA )
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_REFIT_THUMBNAIL )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to schedule ogg files for maintenance failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 463:
            
            result = self._Execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( 'deferred_physical_file_deletes', ) ).fetchone()
            
            if result is None:
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS deferred_physical_file_deletes ( hash_id INTEGER PRIMARY KEY );' )
                self._Execute( 'CREATE TABLE IF NOT EXISTS deferred_physical_thumbnail_deletes ( hash_id INTEGER PRIMARY KEY );' )
                
            
        
        if version == 464:
            
            try:
                
                domain_manager = self.modules_serialisable.GetJSONDump( HydrusSerialisable.SERIALISABLE_TYPE_NETWORK_DOMAIN_MANAGER )
                
                domain_manager.Initialise()
                
                #
                
                domain_manager.OverwriteDefaultParsers( ( 'gelbooru 0.2.x gallery page parser', ) )
                
                #
                
                domain_manager.TryToLinkURLClassesAndParsers()
                
                #
                
                self.modules_serialisable.SetJSONDump( domain_manager )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to update some parsers failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
            #
            
            result = self.modules_services.GetServiceIds( ( HC.COMBINED_DELETED_FILE, ) )
            
            if len( result ) == 0:
                
                self._controller.frame_splash_status.SetText( 'creating new tag search data' )
                
                dictionary = ClientServices.GenerateDefaultServiceDictionary( HC.COMBINED_DELETED_FILE )
                
                self._AddService( CC.COMBINED_DELETED_FILE_SERVICE_KEY, HC.COMBINED_DELETED_FILE, 'all deleted files', dictionary )
                
                #
                
                # populate combined deleted files current files table
                
                self.modules_files_storage.DropFilesTables( self.modules_services.combined_deleted_file_service_id )
                self.modules_files_storage.GenerateFilesTables( self.modules_services.combined_deleted_file_service_id )
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
                for tag_service_id in tag_service_ids:
                    
                    # this should make them empty, since no files yet
                    
                    self.modules_tag_search.Drop( self.modules_services.combined_deleted_file_service_id, tag_service_id )
                    self.modules_tag_search.Generate( self.modules_services.combined_deleted_file_service_id, tag_service_id )
                    
                    self._CacheSpecificMappingsDrop( self.modules_services.combined_deleted_file_service_id, tag_service_id )
                    self._CacheSpecificMappingsGenerate( self.modules_services.combined_deleted_file_service_id, tag_service_id )
                    
                
                combined_deleted_files_current_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( self.modules_services.combined_deleted_file_service_id, HC.CONTENT_STATUS_CURRENT )
                
                file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_COVERED_BY_COMBINED_DELETED_FILE )
                
                for ( i, file_service_id ) in enumerate( file_service_ids ):
                    
                    deleted_files_table_name = ClientDBFilesStorage.GenerateFilesTableName( file_service_id, HC.CONTENT_STATUS_DELETED )
                    
                    for ( chunk_of_hash_ids, num_done, num_to_do ) in HydrusDB.ReadLargeIdQueryInSeparateChunks( self._c, 'SELECT hash_id FROM {};'.format( deleted_files_table_name ), 1024 ):
                        
                        message = 'deleted files cache: service {}, done {}'.format( HydrusData.ConvertValueRangeToPrettyString( i + 1, len( file_service_ids ) ), HydrusData.ConvertValueRangeToPrettyString( num_done, num_to_do ) )
                        self._controller.frame_splash_status.SetSubtext( message )
                        
                        for hash_id in chunk_of_hash_ids:
                            
                            row = self._Execute( 'SELECT hash_id, timestamp FROM {} WHERE hash_id = ?;'.format( deleted_files_table_name ), ( hash_id, ) ).fetchone()
                            
                            existing_row = self._Execute( 'SELECT hash_id, timestamp FROM {} WHERE hash_id = ?;'.format( combined_deleted_files_current_files_table_name ), ( hash_id, ) ).fetchone()
                            
                            if existing_row is None:
                                
                                rows = [ row ]
                                
                                # this should now populate the tag caches and search cache
                                
                                self._AddFiles( self.modules_services.combined_deleted_file_service_id, rows )
                                
                            else:
                                
                                # it doesn't really matter, but let's try to have the earliest timestamp here to start with, since that'll be roughly 'natural' going forwards
                                
                                if row[1] is not None and ( existing_row[1] is None or row[1] < existing_row[1] ):
                                    
                                    self._Execute( 'UPDATE {} SET timestamp = ? WHERE hash_id = ?;'.format( combined_deleted_files_current_files_table_name ), ( row[1], hash_id ) )
                                    
                                
                            
                        
                    
                
                self.modules_db_maintenance.TouchAnalyzeNewTables()
                
                self._cursor_transaction_wrapper.CommitAndBegin()
                
                #
                
                # ipfs is also getting specific caches and tag search too, so we'll do that here
                
                tag_service_ids = self.modules_services.GetServiceIds( HC.REAL_TAG_SERVICES )
                
                file_service_ids = self.modules_services.GetServiceIds( ( HC.IPFS, ) )
                
                for file_service_id in file_service_ids:
                    
                    hash_ids = self.modules_files_storage.GetCurrentHashIdsList( file_service_id )
                    
                    for tag_service_id in tag_service_ids:
                        
                        time.sleep( 0.01 )
                        
                        self.modules_tag_search.Drop( file_service_id, tag_service_id )
                        self.modules_tag_search.Generate( file_service_id, tag_service_id )
                        
                        self._CacheSpecificMappingsDrop( file_service_id, tag_service_id )
                        
                        self._CacheSpecificMappingsCreateTables( file_service_id, tag_service_id )
                        self.modules_mappings_cache_specific_display.Generate( file_service_id, tag_service_id, populate_from_storage = False )
                        
                        BLOCK_SIZE = 1000
                        
                        for ( i, block_of_hash_ids ) in enumerate( HydrusData.SplitListIntoChunks( hash_ids, BLOCK_SIZE ) ):
                            
                            with self._MakeTemporaryIntegerTable( block_of_hash_ids, 'hash_id' ) as temp_hash_id_table_name:
                                
                                message = 'ipfs: {}_{} - {}'.format( file_service_id, tag_service_id, HydrusData.ConvertValueRangeToPrettyString( i * BLOCK_SIZE, len( hash_ids ) ) )
                                
                                self._controller.frame_splash_status.SetSubtext( message )
                                
                                self._CacheSpecificMappingsAddFiles( file_service_id, tag_service_id, block_of_hash_ids, temp_hash_id_table_name )
                                self.modules_mappings_cache_specific_display.AddFiles( file_service_id, tag_service_id, block_of_hash_ids, temp_hash_id_table_name )
                                
                            
                        
                        self.modules_db_maintenance.TouchAnalyzeNewTables()
                        
                        self._cursor_transaction_wrapper.CommitAndBegin()
                        
                    
                
            
            #
            
            result = self._Execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( 'has_icc_profile', ) ).fetchone()
            
            if result is None:
                
                try:
                    
                    self._Execute( 'CREATE TABLE IF NOT EXISTS has_icc_profile ( hash_id INTEGER PRIMARY KEY );' )
                    
                    self._controller.frame_splash_status.SetSubtext( 'scheduling files for icc profile scan' )
                    
                    table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                    
                    hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime IN {};'.format( table_join, HydrusData.SplayListForDB( HC.FILES_THAT_CAN_HAVE_ICC_PROFILE ) ) ) )
                    
                    self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_HAS_ICC_PROFILE )
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    message = 'Trying to schedule image files for icc maintenance failed! Please let hydrus dev know!'
                    
                    self.pub_initial_message( message )
                    
                
            
            #
            
            result = self._Execute( 'SELECT 1 FROM sqlite_master WHERE name = ?;', ( 'pixel_hash_map', ) ).fetchone()
            
            if result is None:
                
                try:
                    
                    self._Execute( 'CREATE TABLE IF NOT EXISTS pixel_hash_map ( hash_id INTEGER, pixel_hash_id INTEGER, PRIMARY KEY ( hash_id, pixel_hash_id ) );' )
                    
                    self._CreateIndex( 'pixel_hash_map', [ 'pixel_hash_id' ] )
                    
                    self._controller.frame_splash_status.SetSubtext( 'scheduling files for pixel hash generation' )
                    
                    table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                    
                    hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime IN {};'.format( table_join, HydrusData.SplayListForDB( HC.FILES_THAT_CAN_HAVE_PIXEL_HASH ) ) ) )
                    
                    self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_PIXEL_HASH )
                    
                except Exception as e:
                    
                    HydrusData.PrintException( e )
                    
                    message = 'Trying to schedule image files for pixel hash maintenance failed! Please let hydrus dev know!'
                    
                    self.pub_initial_message( message )
                    
                
            
        
        if version == 467:
            
            try:
                
                self._controller.frame_splash_status.SetSubtext( 'fixing a pixel duplicates storage problem' )
                
                bad_ids = self._STS( self._Execute( 'SELECT hash_id FROM pixel_hash_map WHERE hash_id = pixel_hash_id;' ) )
                
                self.modules_files_maintenance_queue.AddJobs( bad_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_PIXEL_HASH )
                
                self._Execute( 'DELETE FROM pixel_hash_map WHERE hash_id = pixel_hash_id;' )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to schedule image files for pixel hash maintenance failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 469:
            
            try:
                
                self._controller.frame_splash_status.SetSubtext( 'scheduling video for better silent audio track check' )
                
                table_join = self.modules_files_storage.GetTableJoinLimitedByFileDomain( self.modules_services.combined_local_file_service_id, 'files_info', HC.CONTENT_STATUS_CURRENT )
                
                hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM {} WHERE mime IN {} AND has_audio = ?;'.format( table_join, HydrusData.SplayListForDB( HC.VIDEO ) ), ( True, ) ) )
                
                self.modules_files_maintenance_queue.AddJobs( hash_ids, ClientFiles.REGENERATE_FILE_DATA_JOB_FILE_METADATA )
                
            except Exception as e:
                
                HydrusData.PrintException( e )
                
                message = 'Trying to schedule audible video files for audio track recheck failed! Please let hydrus dev know!'
                
                self.pub_initial_message( message )
                
            
        
        if version == 470:
            
            ( result, ) = self._Execute( 'SELECT sql FROM sqlite_master WHERE name = ?;', ( 'file_viewing_stats', ) ).fetchone()
            
            if 'preview_views' in result:
                
                self._controller.frame_splash_status.SetSubtext( 'reworking file viewing stats' )
                
                self._Execute( 'ALTER TABLE file_viewing_stats RENAME TO file_viewing_stats_old;' )
                
                self._Execute( 'CREATE TABLE IF NOT EXISTS file_viewing_stats ( hash_id INTEGER, canvas_type INTEGER, last_viewed_timestamp INTEGER, views INTEGER, viewtime INTEGER, PRIMARY KEY ( hash_id, canvas_type ) );' )
                self._CreateIndex( 'file_viewing_stats', [ 'last_viewed_timestamp' ] )
                self._CreateIndex( 'file_viewing_stats', [ 'views' ] )
                self._CreateIndex( 'file_viewing_stats', [ 'viewtime' ] )
                
                self._Execute( 'INSERT INTO file_viewing_stats SELECT hash_id, ?, ?, preview_views, preview_viewtime FROM file_viewing_stats_old;', ( CC.CANVAS_PREVIEW, None ) )
                self._Execute( 'INSERT INTO file_viewing_stats SELECT hash_id, ?, ?, media_views, media_viewtime FROM file_viewing_stats_old;', ( CC.CANVAS_MEDIA_VIEWER, None ) )
                
                self.modules_db_maintenance.AnalyzeTable( 'file_viewing_stats' )
                
                self._Execute( 'DROP TABLE file_viewing_stats_old;' )
                
            
        
        self._controller.frame_splash_status.SetTitleText( 'updated db to v{}'.format( HydrusData.ToHumanInt( version + 1 ) ) )
        
        self._Execute( 'UPDATE version SET version = ?;', ( version + 1, ) )
        
    
    def _UpdateMappings( self, tag_service_id, mappings_ids = None, deleted_mappings_ids = None, pending_mappings_ids = None, pending_rescinded_mappings_ids = None, petitioned_mappings_ids = None, petitioned_rescinded_mappings_ids = None ):
        
        ( current_mappings_table_name, deleted_mappings_table_name, pending_mappings_table_name, petitioned_mappings_table_name ) = ClientDBMappingsStorage.GenerateMappingsTableNames( tag_service_id )
        
        if mappings_ids is None: mappings_ids = []
        if deleted_mappings_ids is None: deleted_mappings_ids = []
        if pending_mappings_ids is None: pending_mappings_ids = []
        if pending_rescinded_mappings_ids is None: pending_rescinded_mappings_ids = []
        if petitioned_mappings_ids is None: petitioned_mappings_ids = []
        if petitioned_rescinded_mappings_ids is None: petitioned_rescinded_mappings_ids = []
        
        mappings_ids = self._FilterExistingUpdateMappings( tag_service_id, mappings_ids, HC.CONTENT_UPDATE_ADD )
        deleted_mappings_ids = self._FilterExistingUpdateMappings( tag_service_id, deleted_mappings_ids, HC.CONTENT_UPDATE_DELETE )
        pending_mappings_ids = self._FilterExistingUpdateMappings( tag_service_id, pending_mappings_ids, HC.CONTENT_UPDATE_PEND )
        pending_rescinded_mappings_ids = self._FilterExistingUpdateMappings( tag_service_id, pending_rescinded_mappings_ids, HC.CONTENT_UPDATE_RESCIND_PEND )
        
        tag_ids_to_filter_chained = { tag_id for ( tag_id, hash_ids ) in itertools.chain.from_iterable( ( mappings_ids, deleted_mappings_ids, pending_mappings_ids, pending_rescinded_mappings_ids ) ) }
        
        chained_tag_ids = self.modules_tag_display.FilterChained( ClientTags.TAG_DISPLAY_ACTUAL, tag_service_id, tag_ids_to_filter_chained )
        
        file_service_ids = self.modules_services.GetServiceIds( HC.FILE_SERVICES_WITH_SPECIFIC_MAPPING_CACHES )
        
        change_in_num_mappings = 0
        change_in_num_deleted_mappings = 0
        change_in_num_pending_mappings = 0
        change_in_num_petitioned_mappings = 0
        change_in_num_files = 0
        
        hash_ids_lists = ( hash_ids for ( tag_id, hash_ids ) in itertools.chain.from_iterable( ( mappings_ids, pending_mappings_ids ) ) )
        hash_ids_being_added = { hash_id for hash_id in itertools.chain.from_iterable( hash_ids_lists ) }
        
        hash_ids_lists = ( hash_ids for ( tag_id, hash_ids ) in itertools.chain.from_iterable( ( deleted_mappings_ids, pending_rescinded_mappings_ids ) ) )
        hash_ids_being_removed = { hash_id for hash_id in itertools.chain.from_iterable( hash_ids_lists ) }
        
        hash_ids_being_altered = hash_ids_being_added.union( hash_ids_being_removed )
        
        filtered_hashes_generator = self._CacheSpecificMappingsGetFilteredHashesGenerator( file_service_ids, tag_service_id, hash_ids_being_altered )
        
        self._Execute( 'CREATE TABLE IF NOT EXISTS mem.temp_hash_ids ( hash_id INTEGER );' )
        
        self._ExecuteMany( 'INSERT INTO temp_hash_ids ( hash_id ) VALUES ( ? );', ( ( hash_id, ) for hash_id in hash_ids_being_altered ) )
        
        pre_existing_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM temp_hash_ids WHERE EXISTS ( SELECT 1 FROM {} WHERE hash_id = temp_hash_ids.hash_id );'.format( current_mappings_table_name ) ) )
        
        num_files_added = len( hash_ids_being_added.difference( pre_existing_hash_ids ) )
        
        change_in_num_files += num_files_added
        
        # BIG NOTE:
        # after testing some situations, it makes nicest logical sense to interleave all cache updates into the loops
        # otherwise, when there are conflicts due to sheer duplication or the display system applying two tags at once with the same implications, we end up relying on an out-of-date/unsynced (in cache terms) specific cache for combined etc...
        # I now extend this to counts, argh. this is not great in overhead terms, but many optimisations rely on a/c counts now, and the fallback is the combined storage ac count cache
        
        if len( mappings_ids ) > 0:
            
            for ( tag_id, hash_ids ) in mappings_ids:
                
                if tag_id in chained_tag_ids:
                    
                    self._CacheCombinedFilesDisplayMappingsAddMappingsForChained( tag_service_id, tag_id, hash_ids )
                    
                
                self._ExecuteMany( 'DELETE FROM ' + deleted_mappings_table_name + ' WHERE tag_id = ? AND hash_id = ?;', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_deleted_deleted = self._GetRowCount()
                
                self._ExecuteMany( 'DELETE FROM ' + pending_mappings_table_name + ' WHERE tag_id = ? AND hash_id = ?;', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_pending_deleted = self._GetRowCount()
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO ' + current_mappings_table_name + ' VALUES ( ?, ? );', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_current_inserted = self._GetRowCount()
                
                change_in_num_deleted_mappings -= num_deleted_deleted
                change_in_num_pending_mappings -= num_pending_deleted
                change_in_num_mappings += num_current_inserted
                
                self.modules_mappings_counts_update.UpdateCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, num_current_inserted, - num_pending_deleted ) ] )
                
                if tag_id not in chained_tag_ids:
                    
                    self.modules_mappings_counts_update.UpdateCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, num_current_inserted, - num_pending_deleted ) ] )
                    
                
                self._CacheSpecificMappingsAddMappings( tag_service_id, tag_id, hash_ids, filtered_hashes_generator )
                
            
        
        if len( deleted_mappings_ids ) > 0:
            
            for ( tag_id, hash_ids ) in deleted_mappings_ids:
                
                if tag_id in chained_tag_ids:
                    
                    self._CacheCombinedFilesDisplayMappingsDeleteMappingsForChained( tag_service_id, tag_id, hash_ids )
                    
                
                self._ExecuteMany( 'DELETE FROM ' + current_mappings_table_name + ' WHERE tag_id = ? AND hash_id = ?;', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_current_deleted = self._GetRowCount()
                
                self._ExecuteMany( 'DELETE FROM ' + petitioned_mappings_table_name + ' WHERE tag_id = ? AND hash_id = ?;', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_petitions_deleted = self._GetRowCount()
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO ' + deleted_mappings_table_name + ' VALUES ( ?, ? );', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_deleted_inserted = self._GetRowCount()
                
                change_in_num_mappings -= num_current_deleted
                change_in_num_petitioned_mappings -= num_petitions_deleted
                change_in_num_deleted_mappings += num_deleted_inserted
                
                self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, num_current_deleted, 0 ) ] )
                
                if tag_id not in chained_tag_ids:
                    
                    self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, num_current_deleted, 0 ) ] )
                    
                
                self._CacheSpecificMappingsDeleteMappings( tag_service_id, tag_id, hash_ids, filtered_hashes_generator )
                
            
        
        if len( pending_mappings_ids ) > 0:
            
            for ( tag_id, hash_ids ) in pending_mappings_ids:
                
                if tag_id in chained_tag_ids:
                    
                    self._CacheCombinedFilesDisplayMappingsPendMappingsForChained( tag_service_id, tag_id, hash_ids )
                    
                
                self._ExecuteMany( 'INSERT OR IGNORE INTO ' + pending_mappings_table_name + ' VALUES ( ?, ? );', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_pending_inserted = self._GetRowCount()
                
                change_in_num_pending_mappings += num_pending_inserted
                
                self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, 0, num_pending_inserted ) ] )
                
                if tag_id not in chained_tag_ids:
                    
                    self.modules_mappings_counts_update.AddCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, 0, num_pending_inserted ) ] )
                    
                
                self._CacheSpecificMappingsPendMappings( tag_service_id, tag_id, hash_ids, filtered_hashes_generator )
                
            
        
        if len( pending_rescinded_mappings_ids ) > 0:
            
            for ( tag_id, hash_ids ) in pending_rescinded_mappings_ids:
                
                if tag_id in chained_tag_ids:
                    
                    self._CacheCombinedFilesDisplayMappingsRescindPendingMappingsForChained( tag_service_id, tag_id, hash_ids )
                    
                
                self._ExecuteMany( 'DELETE FROM ' + pending_mappings_table_name + ' WHERE tag_id = ? AND hash_id = ?;', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
                
                num_pending_deleted = self._GetRowCount()
                
                change_in_num_pending_mappings -= num_pending_deleted
                
                self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_STORAGE, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, 0, num_pending_deleted ) ] )
                
                if tag_id not in chained_tag_ids:
                    
                    self.modules_mappings_counts_update.ReduceCounts( ClientTags.TAG_DISPLAY_ACTUAL, self.modules_services.combined_file_service_id, tag_service_id, [ ( tag_id, 0, num_pending_deleted ) ] )
                    
                
                self._CacheSpecificMappingsRescindPendingMappings( tag_service_id, tag_id, hash_ids, filtered_hashes_generator )
                
            
        
        #
        
        post_existing_hash_ids = self._STS( self._Execute( 'SELECT hash_id FROM temp_hash_ids WHERE EXISTS ( SELECT 1 FROM {} WHERE hash_id = temp_hash_ids.hash_id );'.format( current_mappings_table_name ) ) )
        
        self._Execute( 'DROP TABLE temp_hash_ids;' )
        
        num_files_removed = len( pre_existing_hash_ids.intersection( hash_ids_being_removed ).difference( post_existing_hash_ids ) )
        
        change_in_num_files -= num_files_removed
        
        for ( tag_id, hash_ids, reason_id ) in petitioned_mappings_ids:
            
            self._ExecuteMany( 'INSERT OR IGNORE INTO ' + petitioned_mappings_table_name + ' VALUES ( ?, ?, ? );', [ ( tag_id, hash_id, reason_id ) for hash_id in hash_ids ] )
            
            num_petitions_inserted = self._GetRowCount()
            
            change_in_num_petitioned_mappings += num_petitions_inserted
            
        
        for ( tag_id, hash_ids ) in petitioned_rescinded_mappings_ids:
            
            self._ExecuteMany( 'DELETE FROM ' + petitioned_mappings_table_name + ' WHERE tag_id = ? AND hash_id = ?;', ( ( tag_id, hash_id ) for hash_id in hash_ids ) )
            
            num_petitions_deleted = self._GetRowCount()
            
            change_in_num_petitioned_mappings -= num_petitions_deleted
            
        
        service_info_updates = []
        
        if change_in_num_mappings != 0: service_info_updates.append( ( change_in_num_mappings, tag_service_id, HC.SERVICE_INFO_NUM_MAPPINGS ) )
        if change_in_num_deleted_mappings != 0: service_info_updates.append( ( change_in_num_deleted_mappings, tag_service_id, HC.SERVICE_INFO_NUM_DELETED_MAPPINGS ) )
        if change_in_num_pending_mappings != 0: service_info_updates.append( ( change_in_num_pending_mappings, tag_service_id, HC.SERVICE_INFO_NUM_PENDING_MAPPINGS ) )
        if change_in_num_petitioned_mappings != 0: service_info_updates.append( ( change_in_num_petitioned_mappings, tag_service_id, HC.SERVICE_INFO_NUM_PETITIONED_MAPPINGS ) )
        if change_in_num_files != 0: service_info_updates.append( ( change_in_num_files, tag_service_id, HC.SERVICE_INFO_NUM_FILES ) )
        
        if len( service_info_updates ) > 0: self._ExecuteMany( 'UPDATE service_info SET info = info + ? WHERE service_id = ? AND info_type = ?;', service_info_updates )
        
    
    def _UpdateServerServices( self, admin_service_key, serverside_services, service_keys_to_access_keys, deletee_service_keys ):
        
        admin_service_id = self.modules_services.GetServiceId( admin_service_key )
        
        admin_service = self.modules_services.GetService( admin_service_id )
        
        admin_credentials = admin_service.GetCredentials()
        
        ( host, admin_port ) = admin_credentials.GetAddress()
        
        #
        
        current_service_keys = self.modules_services.GetServiceKeys()
        
        for serverside_service in serverside_services:
            
            service_key = serverside_service.GetServiceKey()
            
            if service_key in current_service_keys:
                
                service_id = self.modules_services.GetServiceId( service_key )
                
                service = self.modules_services.GetService( service_id )
                
                credentials = service.GetCredentials()
                
                upnp_port = serverside_service.GetUPnPPort()
                
                if upnp_port is None:
                    
                    port = serverside_service.GetPort()
                    
                    credentials.SetAddress( host, port )
                    
                else:
                    
                    credentials.SetAddress( host, upnp_port )
                    
                
                service.SetCredentials( credentials )
                
                self.modules_services.UpdateService( service )
                
            else:
                
                if service_key in service_keys_to_access_keys:
                    
                    service_type = serverside_service.GetServiceType()
                    name = serverside_service.GetName()
                    
                    service = ClientServices.GenerateService( service_key, service_type, name )
                    
                    access_key = service_keys_to_access_keys[ service_key ]
                    
                    credentials = service.GetCredentials()
                    
                    upnp_port = serverside_service.GetUPnPPort()
                    
                    if upnp_port is None:
                        
                        port = serverside_service.GetPort()
                        
                        credentials.SetAddress( host, port )
                        
                    else:
                        
                        credentials.SetAddress( host, upnp_port )
                        
                    
                    credentials.SetAccessKey( access_key )
                    
                    service.SetCredentials( credentials )
                    
                    ( service_key, service_type, name, dictionary ) = service.ToTuple()
                    
                    self._AddService( service_key, service_type, name, dictionary )
                    
                
            
        
        for service_key in deletee_service_keys:
            
            try:
                
                self.modules_services.GetServiceId( service_key )
                
            except HydrusExceptions.DataMissing:
                
                continue
                
            
            self._DeleteService( service_id )
            
        
        self._cursor_transaction_wrapper.pub_after_job( 'notify_account_sync_due' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_data' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_gui' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
        
    
    def _UpdateServices( self, services ):
        
        current_service_keys = self.modules_services.GetServiceKeys()
        
        future_service_keys = { service.GetServiceKey() for service in services }
        
        for service_key in current_service_keys:
            
            if service_key not in future_service_keys:
                
                service_id = self.modules_services.GetServiceId( service_key )
                
                self._DeleteService( service_id )
                
            
        
        for service in services:
            
            service_key = service.GetServiceKey()
            
            if service_key in current_service_keys:
                
                self.modules_services.UpdateService( service )
                
            else:
                
                ( service_key, service_type, name, dictionary ) = service.ToTuple()
                
                self._AddService( service_key, service_type, name, dictionary )
                
            
        
        self._cursor_transaction_wrapper.pub_after_job( 'notify_account_sync_due' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_data' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_services_gui' )
        self._cursor_transaction_wrapper.pub_after_job( 'notify_new_pending' )
        
    
    def _Vacuum( self, names: typing.Collection[ str ], maintenance_mode = HC.MAINTENANCE_FORCED, stop_time = None, force_vacuum = False ):
        
        ok_names = []
        
        for name in names:
            
            db_path = os.path.join( self._db_dir, self._db_filenames[ name ] )
            
            try:
                
                HydrusDB.CheckCanVacuumCursor( db_path, self._c )
                
            except Exception as e:
                
                if not self._have_printed_a_cannot_vacuum_message:
                    
                    HydrusData.Print( 'Cannot vacuum "{}": {}'.format( db_path, e ) )
                    
                    self._have_printed_a_cannot_vacuum_message = True
                    
                
                continue
                
            
            if self._controller.ShouldStopThisWork( maintenance_mode, stop_time = stop_time ):
                
                return
                
            
            ok_names.append( name )
            
        
        if len( ok_names ) == 0:
            
            HydrusData.ShowText( 'A call to vacuum was made, but none of those databases could be vacuumed! Maybe drive free space is tight and/or recently changed?' )
            
            return
            
        
        job_key_pubbed = False
        
        job_key = ClientThreading.JobKey()
        
        job_key.SetStatusTitle( 'database maintenance - vacuum' )
        
        self._CloseDBConnection()
        
        try:
            
            for name in ok_names:
                
                time.sleep( 1 )
                
                try:
                    
                    db_path = os.path.join( self._db_dir, self._db_filenames[ name ] )
                    
                    if not job_key_pubbed:
                        
                        self._controller.pub( 'modal_message', job_key )
                        
                        job_key_pubbed = True
                        
                    
                    self._controller.frame_splash_status.SetText( 'vacuuming ' + name )
                    job_key.SetVariable( 'popup_text_1', 'vacuuming ' + name )
                    
                    started = HydrusData.GetNowPrecise()
                    
                    HydrusDB.VacuumDB( db_path )
                    
                    time_took = HydrusData.GetNowPrecise() - started
                    
                    HydrusData.Print( 'Vacuumed ' + db_path + ' in ' + HydrusData.TimeDeltaToPrettyTimeDelta( time_took ) )
                    
                except Exception as e:
                    
                    HydrusData.Print( 'vacuum failed:' )
                    
                    HydrusData.ShowException( e )
                    
                    text = 'An attempt to vacuum the database failed.'
                    text += os.linesep * 2
                    text += 'If the error is not obvious, please contact the hydrus developer.'
                    
                    HydrusData.ShowText( text )
                    
                    self._InitDBConnection()
                    
                    return
                    
                
            
            job_key.SetVariable( 'popup_text_1', 'cleaning up' )
            
        finally:
            
            self._InitDBConnection()
            
            self.modules_db_maintenance.RegisterSuccessfulVacuum( name )
            
            job_key.SetVariable( 'popup_text_1', 'done!' )
            
            job_key.Finish()
            
            job_key.Delete( 10 )
            
        
    
    def _Write( self, action, *args, **kwargs ):
        
        result = None
        
        if action == 'analyze': self.modules_db_maintenance.AnalyzeDueTables( *args, **kwargs )
        elif action == 'associate_repository_update_hashes': self.modules_repositories.AssociateRepositoryUpdateHashes( *args, **kwargs )
        elif action == 'backup': self._Backup( *args, **kwargs )
        elif action == 'clear_deferred_physical_delete': self.modules_files_storage.ClearDeferredPhysicalDelete( *args, **kwargs )
        elif action == 'clear_false_positive_relations': self.modules_files_duplicates.DuplicatesClearAllFalsePositiveRelationsFromHashes( *args, **kwargs )
        elif action == 'clear_false_positive_relations_between_groups': self.modules_files_duplicates.DuplicatesClearFalsePositiveRelationsBetweenGroupsFromHashes( *args, **kwargs )
        elif action == 'clear_orphan_file_records': self._ClearOrphanFileRecords( *args, **kwargs )
        elif action == 'clear_orphan_tables': self._ClearOrphanTables( *args, **kwargs )
        elif action == 'content_updates': self._ProcessContentUpdates( *args, **kwargs )
        elif action == 'cull_file_viewing_statistics': self._CullFileViewingStatistics( *args, **kwargs )
        elif action == 'db_integrity': self._CheckDBIntegrity( *args, **kwargs )
        elif action == 'delete_imageboard': self.modules_serialisable.DeleteYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_IMAGEBOARD, *args, **kwargs )
        elif action == 'delete_local_booru_share': self.modules_serialisable.DeleteYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_LOCAL_BOORU, *args, **kwargs )
        elif action == 'delete_pending': self._DeletePending( *args, **kwargs )
        elif action == 'delete_serialisable_named': self.modules_serialisable.DeleteJSONDumpNamed( *args, **kwargs )
        elif action == 'delete_service_info': self._DeleteServiceInfo( *args, **kwargs )
        elif action == 'delete_potential_duplicate_pairs': self.modules_files_duplicates.DuplicatesDeleteAllPotentialDuplicatePairs( *args, **kwargs )
        elif action == 'dirty_services': self._SaveDirtyServices( *args, **kwargs )
        elif action == 'dissolve_alternates_group': self.modules_files_duplicates.DuplicatesDissolveAlternatesGroupIdFromHashes( *args, **kwargs )
        elif action == 'dissolve_duplicates_group': self.modules_files_duplicates.DuplicatesDissolveMediaIdFromHashes( *args, **kwargs )
        elif action == 'duplicate_pair_status': self._DuplicatesSetDuplicatePairStatus( *args, **kwargs )
        elif action == 'duplicate_set_king': self.modules_files_duplicates.DuplicatesSetKingFromHash( *args, **kwargs )
        elif action == 'file_maintenance_add_jobs': self.modules_files_maintenance_queue.AddJobs( *args, **kwargs )
        elif action == 'file_maintenance_add_jobs_hashes': self.modules_files_maintenance_queue.AddJobsHashes( *args, **kwargs )
        elif action == 'file_maintenance_cancel_jobs': self.modules_files_maintenance_queue.CancelJobs( *args, **kwargs )
        elif action == 'file_maintenance_clear_jobs': self.modules_files_maintenance.ClearJobs( *args, **kwargs )
        elif action == 'fix_logically_inconsistent_mappings': self._FixLogicallyInconsistentMappings( *args, **kwargs )
        elif action == 'imageboard': self.modules_serialisable.SetYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_IMAGEBOARD, *args, **kwargs )
        elif action == 'ideal_client_files_locations': self._SetIdealClientFilesLocations( *args, **kwargs )
        elif action == 'import_file': result = self._ImportFile( *args, **kwargs )
        elif action == 'import_update': self._ImportUpdate( *args, **kwargs )
        elif action == 'local_booru_share': self.modules_serialisable.SetYAMLDump( ClientDBSerialisable.YAML_DUMP_ID_LOCAL_BOORU, *args, **kwargs )
        elif action == 'maintain_hashed_serialisables': result = self.modules_serialisable.MaintainHashedStorage( *args, **kwargs )
        elif action == 'maintain_similar_files_search_for_potential_duplicates': result = self._PerceptualHashesSearchForPotentialDuplicates( *args, **kwargs )
        elif action == 'maintain_similar_files_tree': self.modules_similar_files.MaintainTree( *args, **kwargs )
        elif action == 'migration_clear_job': self._MigrationClearJob( *args, **kwargs )
        elif action == 'migration_start_mappings_job': self._MigrationStartMappingsJob( *args, **kwargs )
        elif action == 'migration_start_pairs_job': self._MigrationStartPairsJob( *args, **kwargs )
        elif action == 'process_repository_content': result = self._ProcessRepositoryContent( *args, **kwargs )
        elif action == 'process_repository_definitions': result = self.modules_repositories.ProcessRepositoryDefinitions( *args, **kwargs )
        elif action == 'push_recent_tags': self._PushRecentTags( *args, **kwargs )
        elif action == 'regenerate_local_hash_cache': self._RegenerateLocalHashCache( *args, **kwargs )
        elif action == 'regenerate_local_tag_cache': self._RegenerateLocalTagCache( *args, **kwargs )
        elif action == 'regenerate_similar_files': self.modules_similar_files.RegenerateTree( *args, **kwargs )
        elif action == 'regenerate_searchable_subtag_maps': self._RegenerateTagCacheSearchableSubtagMaps( *args, **kwargs )
        elif action == 'regenerate_tag_cache': self._RegenerateTagCache( *args, **kwargs )
        elif action == 'regenerate_tag_display_mappings_cache': self._RegenerateTagDisplayMappingsCache( *args, **kwargs )
        elif action == 'regenerate_tag_display_pending_mappings_cache': self._RegenerateTagDisplayPendingMappingsCache( *args, **kwargs )
        elif action == 'regenerate_tag_mappings_cache': self._RegenerateTagMappingsCache( *args, **kwargs )
        elif action == 'regenerate_tag_parents_cache': self._RegenerateTagParentsCache( *args, **kwargs )
        elif action == 'regenerate_tag_pending_mappings_cache': self._RegenerateTagPendingMappingsCache( *args, **kwargs )
        elif action == 'regenerate_tag_siblings_and_parents_cache': self.modules_tag_display.RegenerateTagSiblingsAndParentsCache( *args, **kwargs )
        elif action == 'register_shutdown_work': self.modules_db_maintenance.RegisterShutdownWork( *args, **kwargs )
        elif action == 'repopulate_mappings_from_cache': self._RepopulateMappingsFromCache( *args, **kwargs )
        elif action == 'repopulate_tag_cache_missing_subtags': self._RepopulateTagCacheMissingSubtags( *args, **kwargs )
        elif action == 'repopulate_tag_display_mappings_cache': self._RepopulateTagDisplayMappingsCache( *args, **kwargs )
        elif action == 'relocate_client_files': self._RelocateClientFiles( *args, **kwargs )
        elif action == 'remove_alternates_member': self.modules_files_duplicates.DuplicatesRemoveAlternateMemberFromHashes( *args, **kwargs )
        elif action == 'remove_duplicates_member': self.modules_files_duplicates.DuplicatesRemoveMediaIdMemberFromHashes( *args, **kwargs )
        elif action == 'remove_potential_pairs': self.modules_files_duplicates.DuplicatesRemovePotentialPairsFromHashes( *args, **kwargs )
        elif action == 'repair_client_files': self._RepairClientFiles( *args, **kwargs )
        elif action == 'repair_invalid_tags': self._RepairInvalidTags( *args, **kwargs )
        elif action == 'reprocess_repository': self.modules_repositories.ReprocessRepository( *args, **kwargs )
        elif action == 'reset_repository': self._ResetRepository( *args, **kwargs )
        elif action == 'reset_repository_processing': self._ResetRepositoryProcessing( *args, **kwargs )
        elif action == 'reset_potential_search_status': self._PerceptualHashesResetSearchFromHashes( *args, **kwargs )
        elif action == 'save_options': self._SaveOptions( *args, **kwargs )
        elif action == 'serialisable': self.modules_serialisable.SetJSONDump( *args, **kwargs )
        elif action == 'serialisable_atomic': self.modules_serialisable.SetJSONComplex( *args, **kwargs )
        elif action == 'serialisable_simple': self.modules_serialisable.SetJSONSimple( *args, **kwargs )
        elif action == 'serialisables_overwrite': self.modules_serialisable.OverwriteJSONDumps( *args, **kwargs )
        elif action == 'set_password': self._SetPassword( *args, **kwargs )
        elif action == 'set_repository_update_hashes': self.modules_repositories.SetRepositoryUpdateHashes( *args, **kwargs )
        elif action == 'schedule_repository_update_file_maintenance': self.modules_repositories.ScheduleRepositoryUpdateFileMaintenance( *args, **kwargs )
        elif action == 'sync_tag_display_maintenance': result = self._CacheTagDisplaySync( *args, **kwargs )
        elif action == 'tag_display_application': self.modules_tag_display.SetApplication( *args, **kwargs )
        elif action == 'update_server_services': self._UpdateServerServices( *args, **kwargs )
        elif action == 'update_services': self._UpdateServices( *args, **kwargs )
        elif action == 'vacuum': self._Vacuum( *args, **kwargs )
        else: raise Exception( 'db received an unknown write command: ' + action )
        
        return result
        
    
    def pub_content_updates_after_commit( self, service_keys_to_content_updates ):
        
        self._after_job_content_update_jobs.append( service_keys_to_content_updates )
        
    
    def pub_initial_message( self, message ):
        
        self._initial_messages.append( message )
        
    
    def pub_service_updates_after_commit( self, service_keys_to_service_updates ):
        
        self._cursor_transaction_wrapper.pub_after_job( 'service_updates_data', service_keys_to_service_updates )
        self._cursor_transaction_wrapper.pub_after_job( 'service_updates_gui', service_keys_to_service_updates )
        
    
    def publish_status_update( self ):
        
        self._controller.pub( 'set_status_bar_dirty' )
        
    
    def GetInitialMessages( self ):
        
        return self._initial_messages
        
    
    def RestoreBackup( self, path ):
        
        for filename in self._db_filenames.values():
            
            HG.client_controller.frame_splash_status.SetText( filename )
            
            source = os.path.join( path, filename )
            dest = os.path.join( self._db_dir, filename )
            
            if os.path.exists( source ):
                
                HydrusPaths.MirrorFile( source, dest )
                
            else:
                
                # if someone backs up with an older version that does not have as many db files as this version, we get conflict
                # don't want to delete just in case, but we will move it out the way
                
                HydrusPaths.MergeFile( dest, dest + '.old' )
                
            
        
        additional_filenames = self._GetPossibleAdditionalDBFilenames()
        
        for additional_filename in additional_filenames:
            
            source = os.path.join( path, additional_filename )
            dest = os.path.join( self._db_dir, additional_filename )
            
            if os.path.exists( source ):
                
                HydrusPaths.MirrorFile( source, dest )
                
            
        
        HG.client_controller.frame_splash_status.SetText( 'media files' )
        
        client_files_source = os.path.join( path, 'client_files' )
        client_files_default = os.path.join( self._db_dir, 'client_files' )
        
        if os.path.exists( client_files_source ):
            
            HydrusPaths.MirrorTree( client_files_source, client_files_default )
            
        
    
