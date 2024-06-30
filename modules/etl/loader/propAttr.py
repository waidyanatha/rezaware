#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "propAttr"
__package__= "loader"
__module__ = "etl"
__app__ = "rezaware"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import configparser    
    import logging
    import traceback
    import functools
    import findspark
    findspark.init()
    from pyspark.sql import functions as F
    from pyspark.sql import DataFrame
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    from pymongo import MongoClient

    from datetime import datetime, date, timedelta

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS configure the master property details, groups, reference, and geom entities

    Contributors:
        * farmraider@protonmail.com

    Resources:

'''

class properties():

    ''' Function --- INIT ---

            author: <farmraider@protonmail.com>
    '''
    def __init__(
        self,
        realm:str=None,
        desc :str=None,
        **kwargs):
        """
        Decription:
            Initializes the features: class property attributes, app configurations, 
                logger function, data store directory paths, and global classes 
        Attributes:
            desc (str) identify the specific instantiation and purpose
        Returns:
            None
        """

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        if desc is None or "".join(desc.split())=="":
            self.__desc__ = " ".join([self.__app__,self.__module__,
                                      self.__package__,self.__name__])
        else:
            self.__desc__ = desc

        self._data = None
        self._realm= realm #None
        self._realmList = [
            'DATABASE', # relational databases like postgres
            'NOSQLDB',  # NoSQL databases like mongodb
            'FILES', # csv, pdf, type text, audio, video files
        ]
        self._dbTypesList = [
            'mongodb',   # working and tested with community edition v4.4
            'cassandra', # TBD
            'hbase',   # TBD
            'neo4j',   # TBD
            'couchdb', # TBD
        ]
        self._asTypeList = [
            'STR',   # text string ""
            'LIST',  # list of values []
            'DICT',  # dictionary {}
            'ARRAY', # numpy array ()
            'SET',   # set of values ()
            'PANDAS', # pandas dataframe
            'SPARK',  # spark dataframe
        ]   # list of data types to convert content to
        ''' --- NoSQL DB properties --- '''
        self._dbHostIP=None
        self._dbPort = None
        self._collections = None
        self._connect = None
        self._documents = None        
        ''' --- SPARK properties --- '''
        self._sparkMaster =  None

        global pkgConf  # this package configparser class instance
        global appConf  # configparser class instance
        global logger   # rezaware logger class instance
#         global clsSDB   # etl loader sparkRDB class instance

        __s_fn_id__ = f"{self.__name__} function <__init__>"
        
        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self._rezHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self._rezHome)
            
            ''' innitialize the logger '''
            from rezaware.utils import Logger as logs
            logger = logs.get_logger(
                cwd=self._rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)

            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)

            ''' Set the wrangler root directory '''
#             self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self._appDir = pkgConf.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
#             self.dataDir = pkgConf.get("CWDS","DATA")
            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self._appDir, self.__conf_fname__))

#             ''' import spark RDBM work load utils to read and write data '''
#             from rezaware.modules.etl.loader import sparkRDBM as db
#             self._clsSDB = db.dataWorkLoads(
#                 desc=self.__desc__,
#                 db_type = self.dbType,
#                 db_name = self.dbName,
#                 db_schema=self.dbSchema,
#                 db_user = self.dbUser,
#                 db_pswd = self.dbPswd,
#                 spark_save_mode='append',
#             )
#             ''' import spark nosql work load utils to read and write data '''
#             from rezaware.modules.etl.loader import sparkNoSQL as nosql
#             self._clsNoSQL = nosql.dataWorkLoads(
#                 db_type = self.dbType, # database type postgres (preferred), mysql, etc
#                 db_name = self.dbName,
#                 db_format=self.dbFormat,
#                 db_user = self.dbUser,
#                 db_pswd = self.dbPswd,
#                 db_auth_source = self.dbAuthSource,
#                 db_auth_mechanism=self.dbAuthMechanism,
#                 desc=self.__desc__
#                 **kwargs,
#             )
#             ''' import spark time-series work load utils for rolling mean/variance computations '''
#             from rezaware.modules.ml.timeseries import rollingstats as stats
#             self._clsStats = stats.RollingStats(desc=self.__desc__)
#             ''' import assset performance index class '''
#             from mining.modules.finance.analysis import technical as indx
#             self._clsIndx =indx.AssetMovement(desc=self.__desc__)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None


#     ''' Function --- CLASS PROPERTIES ---

#             author: <farmraider@protonmail.com>

#     '''
#     ''' --- SPARK DATABASE CIONNECTION --- '''
#     @property
#     def clsSDB(self):
#         """
#         Description:
#         Attributes :
#         Returns :
#         Exceptions :
#         """

#         __s_fn_id__ = f"{self.__name__} function <@property clsSDB>"

#         try:
#             if self._clsSDB is None:
#                 ''' import spark RDBM work load utils to read and write data '''
#                 from rezaware.modules.etl.loader import sparkRDBM as db
#                 self._clsSDB = db.dataWorkLoads(
#                     desc=self.__desc__,
#                     db_type = self.dbType,
#                     db_name = self.dbName,
#                     db_schema=self.dbSchema,
#                     db_user = self.dbUser,
#                     db_pswd = self.dbPswd,
#                     spark_save_mode='append',
#                 )
#                 logger.warning("%s Invalid spark RDBM class object set with default parameters %s", 
#                                __s_fn_id__, type(self._clsNoSQL))
# #                 raise ConnectionError("No spark database connection detected")

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._clsSDB

#     @clsSDB.setter
#     def clsSDB(self,clsObj):

#         __s_fn_id__ = f"{self.__name__} function <@setter clsSDB>"

#         try:
#             if clsObj is None:
#                 raise ConnectionError("Invalid spark session, cannot assign to property")

#             self._clsSDB = clsObj 

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._clsSDB


#     ''' Function --- CLASS PROPERTIES ---

#             author: <farmraider@protonmail.com>

#     '''
#     ''' --- SPARK NOSQL DATABASE CIONNECTION --- '''
#     @property
#     def clsNoSQL(self):
#         """
#         Description:
#         Attributes :
#         Returns :
#         Exceptions :
#         """

#         __s_fn_id__ = f"{self.__name__} function <@property clsNoSQL>"

#         try:
#             if self._clsNoSQL is None:
#                 ''' try import spark nosql work load utils to read and write data '''
#                 from rezaware.modules.etl.loader import sparkNoSQL as nosql
#                 self._clsNoSQL = nosql.dataWorkLoads(
#                     db_type = self.dbType, # database type postgres (preferred), mysql, etc
#                     db_name = self.dbName,
#                     db_format=self.dbFormat,
#                     db_user = self.dbUser,
#                     db_pswd = self.dbPswd,
#                     db_auth_source = self.dbAuthSource,
#                     db_auth_mechanism=self.dbAuthMechanism,
#                     desc=self.__desc__
#                     **kwargs,
#                 )
#                 logger.warning("%s Invalid NoSQL class object set with default parameters %s", 
#                                __s_fn_id__, type(self._clsNoSQL))
# #                 raise ConnectionError("No spark NoSL database connection detected")

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._clsNoSQL

#     @clsNoSQL.setter
#     def clsNoSQL(self,clsObj):

#         __s_fn_id__ = f"{self.__name__} function <@setter clsNoSQL>"

#         try:
#             if clsObj is None:
#                 raise ConnectionError("Invalid spark session, cannot assign to property")

#             self._clsNoSQL = clsObj 

#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._clsNoSQL


    @property
    def dbHostIP(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbHostIP>"

        try:
            if self._dbHostIP is None and appConf.get('NOSQLDB','DBHOSTIP'):
                self._dbHostIP = appConf.get('NOSQLDB','DBHOSTIP')
                logger.warning("%s set class @property dbHostIP to %s from config data in %s",
                               __s_fn_id__,self._dbHostIP.upper(),__conf_fname__.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbHostIP

    @dbHostIP.setter
    def dbHostIP(self,db_host_ip:str = "127.0.0.1"):

        __s_fn_id__ = f"{self.__name__} function <@dbHostIP.setter>"

        try:
            if not (db_host_ip is None and db_host_ip==""):
                self._dbHostIP = db_host_ip
                logger.debug("%s set class @property dbHostIP to %s",
                               __s_fn_id__,self._dbHostIP.upper())
            else:
                raise ConnectionError("Undefined hostip; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbHostIP

    ''' DB TYPE '''
    @property
    def dbType(self) -> str:
        
        __s_fn_id__ = f"{self.__name__} function <@property dbType>"

        try:
            if self._dbType is None and appConf.get('NOSQLDB','DBTYPE'):
                self._dbType = appConf.get('NOSQLDB','DBTYPE')
                logger.warning("%s set class @property dbType to %s",
                               __s_fn_id__,self._dbType.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbType

    @dbType.setter
    def dbType(self,db_type:str):

        __s_fn_id__ = f"{self.__name__} function <@dbType.setter>"
        try:
            if db_type in self._dbTypesList:
                self._dbType = db_type
            elif appConf.get('NOSQLDB','DBTYPE'):
                self._dbType = appConf.get('NOSQLDB','DBTYPE')
                logger.warning("%s set class @property dbType to %s from config data in %s",
                               __s_fn_id__,self._dbType.upper(),__conf_fname__.upper())
            else:
                raise ConnectionError("Undefined dbType; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbType

    ''' DB PORT '''
    @property
    def dbPort(self) -> int:
        
        __s_fn_id__ = f"{self.__name__} function <@property dbPort>"

        try:
            if self._dbPort is None and appConf.get('NOSQLDB','DBPORT'):
                self._dbPort = appConf.get('NOSQLDB','DBPORT')
                logger.warning("%s set class @property dbPort to %s from config data in %s",
                               __s_fn_id__,self._dbPort.upper(),__conf_fname__.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPort

    @dbPort.setter
    def dbPort(self,db_port=27017) -> int:

        __s_fn_id__ = f"{self.__name__} function <@dbPort.setter>"

        try:
            if isinstance(db_port,int):
                self._dbPort = db_port
                logger.debug("%s set class @property dbPort to %s",
                               __s_fn_id__,self._dbPort.upper())

            else:
                raise ConnectionError("dbPort must be a valid integer")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPort

    ''' DB FORMAT '''
    @property
    def dbFormat(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbFormat>"

        try:
            if self._dbFormat is None and appConf.get('NOSQLDB','DBFORMAT'):
                self._dbFormat = appConf.get('NOSQLDB','DBFORMAT')
                logger.warning("%s Nonetype class @property dbFormat set to %s from config data in %s",
                               __s_fn_id__,self._dbFormat.upper(),__conf_fname__.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbFormat

    @dbFormat.setter
    def dbFormat(self,db_format:str) -> str:

        __s_fn_id__ = f"{self.__name__} function @dbFormat.setter"

        try:
            if not (db_driver is None and db_format==""):
                self._dbFormat = db_format
                logger.debug("%s set class @property dbFormat to %s",
                               __s_fn_id__,self._dbFormat.upper())

            else:
                raise ConnectionError("Undefined dbFormat; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbFormat

    ''' DB NAME '''
    @property
    def dbName(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbName>"

        try:
            if self.realm not in self._realmList:
                raise AttributeError("Invalid realm %s set propert to one of: %s" 
                                     % (type(self._realm), ",".join(self._realmList)))
            ''' read from app.cfg '''
            if self._dbName is None and appConf.get(self._realm,'DBNAME'):
                self._dbName = appConf.get(self._realm,'DBNAME')

            logger.warning("%s set %s class @property dbName to %s",
                           __s_fn_id__, self._realm, self._dbName.upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbName

    @dbName.setter
    def dbName(self,db_name:str) -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbName.setter>"

        try:
            if db_name is not None and "".join(db_name.split())!="":
                self._dbName = db_name
                logger.debug("%s set class @property dbName to %s",__s_fn_id__,self._dbName.upper())
            else:
                raise ConnectionError("Undefined dbName; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbName

    ''' DB USER '''
    @property
    def dbUser(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbUser>"

        try:
#             if self._dbUser is None and appConf.get('NOSQLDB','DBUSER'):
#                 self._dbUser = appConf.get('NOSQLDB','DBUSER')
            if self._dbUser is None and os.getenv('NOSQLDB_DBUSER'):
                self._dbUser = os.getenv('NOSQLDB_DBUSER')

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbUser

    @dbUser.setter
    def dbUser(self,db_user:str) -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbUser.setter>"
        try:
            if db_user is not None and "".join(db_user.split())!="":
                self._dbUser = db_user
                logger.warning("%s set class property dbUser to %s",__s_fn_id__,self._dbUser.upper())
            else:
                raise ConnectionError("Undefined dbUser; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbUser

    ''' DB PASSWORD '''
    @property
    def dbPswd(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbPswd>"

        try:
#             if self._dbPswd is None and appConf.get('NOSQLDB','DBPSWD'):
#                 self._dbPswd = appConf.get('NOSQLDB','DBPSWD')
            if self._dbPswd is None and os.getenv('NOSQLDB_DBPSWD'):
                self._dbPswd = os.getenv('NOSQLDB_DBPSWD')
                logger.debug("%s dbUser property %s read from environment %s",
                             __s_fn_id__, self._dbPswd, os.getenv('NOSQLDB_DBPSWD'))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPswd

    @dbPswd.setter
    def dbPswd(self,db_pswd:str) -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbPswd.setter>"
        try:
            if db_pswd is not None and "".join(db_pswd.split())!="":
                self._dbPswd = db_pswd
                logger.debug("%s set class property dbPswd to %s",__s_fn_id__,self._dbPswd.upper())
            else:
                raise ConnectionError("Undefined dbPswd; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbPswd

    ''' DB AUTHSOURCE '''
    @property
    def dbAuthSource(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbAuthSource>"

        try:
            if self._dbAuthSource is None and appConf.get('NOSQLDB','DBAUTHSOURCE'):
                self._dbAuthSource = appConf.get('NOSQLDB','DBAUTHSOURCE')

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbAuthSource

    @dbAuthSource.setter
    def dbAuthSource(self,db_auth_source:str="") -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbAuthSource.setter>"

        try:
            if db_auth_source is not None or "".join(db_auth_source.split())!="":
                self._dbAuthSource = db_auth_source
                logger.debug("%s set class property dbAuthSource to %s",
                             __s_fn_id__,self._dbAuthSource.upper())
            else:
                raise ConnectionError("Undefined dbAuthSource; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbAuthSource

    ''' DB MECHANISM '''
    @property
    def dbAuthMechanism(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property dbAuthMechanism>"

        try:
            if self._dbAuthMechanism is None and appConf.get('NOSQLDB','DBAUTHMECHANISM'):
                self._dbAuthMechanism = appConf.get('NOSQLDB','DBAUTHMECHANISM')

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbAuthMechanism

    @dbAuthMechanism.setter
    def dbAuthMechanism(self,db_auth_mechanism:str) -> str:

        __s_fn_id__ = f"{self.__name__} function <@dbAuthMechanism.setter>"
        try:
            if db_auth_mechanism is not None and "".join(db_auth_mechanism.split())!="":
                self._dbAuthMechanism = db_auth_mechanism
                logger.debug("%s set class property dbAuthMechanism to %s",
                             __s_fn_id__,self._dbAuthMechanism.upper())
            else:
                raise ConnectionError("Undefined dbAuthMechanism; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbAuthMechanism

    ''' SPARK MASTER '''
    @property
    def sparkMaster(self) -> str:

        __s_fn_id__ = f"{self.__name__} function <@property sparkMaster>"

        try:
            if self._sparkMaster is None and appConf.get('SPARK','MASTER'):
                self._sparkMaster = appConf.get('SPARK','MASTER')

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._sparkMaster

    @sparkMaster.setter
    def sparkMaster(self,spark_master:str) -> str:

        __s_fn_id__ = f"{self.__name__} function <@sparkMaster.setter>"
        try:
            if not (spark_master is None and spark_master==""):
                self._sparkMaster = spark_master
            else:
                raise ConnectionError("Undefined sparkMaster; set in app.cfg or as class property")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._sparkMaster

    
    ''' Function - connect

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._connect

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def connect(self):

        __s_fn_id__ = f"{self.__name__} function <@property connect>"

        try:
            if self._connect is None and \
                self.dbHostIP and \
                self.dbUser and \
                self.dbPswd and \
                self.dbAuthSource and \
                self.dbAuthMechanism:
                if self.dbType.lower() == 'mongodb':
                    self._connect = MongoClient(
                        self._dbHostIP,
                        username=self._dbUser,
                        password=self._dbPswd,
                        authSource=self._dbAuthSource,
                        authMechanism=self._dbAuthMechanism
                    )
                    logger.warning("%s Nonetype connection set to %s using existing properties",
                                   __s_fn_id__,self._connect)
                elif self.dbType.lower() == 'cassandra':
                    raise RuntimError("cassandra is to be included in a future release")
                else:
                    raise ValueError("Undefined dbType. It must be one of %s" % self._dbTypeList)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._connect

    @connect.setter
    def connect(self,connect_properties:dict={}):

        __s_fn_id__ = f"{self.__name__} function <@connect.setter>"

        _db_host_ip=None
        _db_user=None
        _db_pswd=None
        _db_auth=None
        _db_mech=None

        try:
            ''' check if properties in args or config file are defined '''
            if not (len(connect_properties) > 0 or "NOSQLDB" in appConf.sections()):
                raise TypeError("Input attributes in %s and NOSQLDB section in %s undefined"
                                % (__s_fn_id__,self.__conf_fname__))
            ''' check and set DBHOSTIP from args or app config '''
            if "DBHOSTIP" in connect_properties.keys():
#                 _db_host_ip = connect_properties['DBHOSTIP']
                self.dbHostIP = connect_properties['DBHOSTIP']
            elif appConf.get('NOSQLDB','DBHOSTIP'):
#                 _db_host_ip = appConf.get('NOSQLDB','DBHOSTIP')
                self.dbHostIP = appConf.get('NOSQLDB','DBHOSTIP')
            else:
                raise ValueError("Undefined DBHOSTIP in function args and app config file. aborting")

            ''' check and set DBTYPE '''
            if "DBTYPE" in connect_properties.keys():
                self.dbType = connect_properties['DBTYPE']
            elif self._dbType is not None:
                logger.warning("%s reusing already set property dbType %s",
                            __s_fn_id__,self._dbType.upper())
            elif appConf.get('NOSQLDB','DBTYPE'):
                self.dbType = appConf.get('NOSQLDB','DBTYPE')
            else:
                raise ValueError("Undefined DBTYPE in function args and app config file. aborting")

            ''' check and set DBUSER from args or app config '''
            if "DBUSER" in connect_properties.keys():
#                 _db_user = connect_properties['DBUSER']
                self.dbUser = connect_properties['DBUSER']
            elif self._dbUser is not None:
                logger.warning("%s reusing already set property dbUser %s",
                            __s_fn_id__,self._dbUser.upper())
            elif appConf.get('NOSQLDB','DBUSER'):
#                 _db_user = appConf.get('NOSQLDB','DBUSER')
                self.dbUser = appConf.get('NOSQLDB','DBUSER')
            else:
                raise ValueError("Undefined DBUSER in function args and app config file. aborting")

            ''' check and set DBPSWD from args or app config '''
            if "DBPSWD" in connect_properties.keys():
#                 _db_pswd = connect_properties['DBPSWD']
                self.dbPswd = connect_properties['DBPSWD']
            elif self._dbPswd is not None:
                logger.warning("%s reusing already set property dbPswd %s",
                            __s_fn_id__,self._dbPswd.upper())
            elif appConf.get('NOSQLDB','DBPSWD'):
#                 _db_pswd = appConf.get('NOSQLDB','DBPSWD')
                self.dbPswd = appConf.get('NOSQLDB','DBPSWD')
            else:
                raise ValueError("Undefined DBPSWD in function args and app config file. aborting")

            ''' check and set DBAUTHSOURCE from args or app config '''
            if "DBAUTHSOURCE" in connect_properties.keys():
#                 _db_auth = connect_properties['DBAUTHSOURCE']
                self.dbAuthSource = connect_properties['DBAUTHSOURCE']
            elif self._dbAuthSource is not None:
                logger.warning("%s reusing already set property authSource %s",
                            __s_fn_id__,self._dbAuthSource.upper())
            elif self.dbName is not None:
                self.dbAuthSource = self._dbName
#                 _db_auth = self._dbName
                logger.warning("Unspecified DBAUTHSOURCE try with authSource = dbName")
            elif appConf.get('NOSQLDB','DBAUTHSOURCE'):
#                 _db_auth = appConf.get('NOSQLDB','DBAUTHSOURCE')
                self.dbAuthSource = appConf.get('NOSQLDB','DBAUTHSOURCE')
                logger.warning("Trying db auth source with %s value",self.__conf_fname__.upper())
            else:
                raise ValueError("Undefined DBAUTHSOURCE in function args and app config file. aborting")

            ''' check and set DBAUTHMECHANISM from args or app config '''
            if "DBAUTHMECHANISM" in connect_properties.keys():
#                 _db_mech = connect_properties['DBAUTHMECHANISM']
                self.dbAuthMechanism = connect_properties['DBAUTHMECHANISM']
            elif self._dbAuthMechanism is not None:
                logger.warning("%s reusing already set property dbAuthMechanism %s",
                            __s_fn_id__,self._dbAuthMechanism.upper())
            elif appConf.get('NOSQLDB','DBAUTHMECHANISM'):
#                 _db_mech = appConf.get('NOSQLDB','DBAUTHMECHANISM')
                self.dbAuthMechanism = appConf.get('NOSQLDB','DBAUTHMECHANISM')
            else:
                raise ValueError("Undefined DBAUTHMECHANISM in function args and app config file. aborting")

            ''' initialize noSQLdbconnect '''
            if self.dbType.lower() == 'mongodb':
#                 self._connect = MongoClient(
#                     _db_host_ip,
#                     username=_db_user,
#                     password=_db_pswd,
#                     authSource=_db_auth,
#                     authMechanism=_db_mech
#                 )
                self._connect = MongoClient(
                    self._dbHostIP,
                    username=self._dbUser,
                    password=self._dbPswd,
                    authSource=self._dbAuthSource,
                    authMechanism=self._dbAuthMechanism
                )
                logger.debug("%s %s",__s_fn_id__,str(self._connect))
            elif self.dbType.lower() == 'cassandra':
                raise RuntimError("cassandra is to be included in a future release")
            else:
                raise ValueError("Undefined dbType. It must be one of %s" % 
                                 str(self._dbTypeList).upper())

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._connect

    ''' Function - collection

            parameters:

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    @property
    def collections(self) -> list:

        __s_fn_id__ = f"{self.__name__} function <@property collections>"

        try:
            if self._collections is None and self.dbName and self.dbAuthSource:
                if self.dbType.lower() == 'mongodb':
#                     print(self.dbName,self.dbAuthSource,self.connect)
                    db = self.connect[self.dbName]
                    self._collections = db.list_collection_names()
                elif self.dbType.lower() == 'cassendra':
                    print('TBD')
                else:
                    raise AttributeError('Something was wrong')
                    
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._collections

    @collections.setter
    def collections(self, collection_properties:dict={}) -> list:

        __s_fn_id__ = f"{self.__name__} function <@collections.setter>"

        _coll_list=[]

        try:
            ''' set the dbName if specified '''
            if collection_properties is not None and "DBNAME" in collection_properties.keys():
                self._dbName = collection_properties['DBNAME']
            ''' set the dbType if specified '''
            if collection_properties is not None and "DBTYPE" in collection_properties.keys():
                self._dbType = collection_properties['DBTYPE'].lower()
            if collection_properties is not None and "DBAUTHSOURCE" in collection_properties.keys():
                self._dbAuthSource = collection_properties['DBAUTHSOURCE']
            elif self._dbAuthSource is None:
                self._dbAuthSource = self.dbName
            else:
                pass

            if self.dbType.lower() == 'mongodb':
                db = self.connect[self.dbName]
                _coll_list = db.list_collection_names()
            elif self.dbType.lower() == 'cassendra':
                print('TBD')
            else:
                raise AttributeError('Something was wrong')

            if _coll_list is None or len(_coll_list)<=0:
                raise RuntimeError("list_collection_names() returned an empty %s object" 
                                   % (type(_coll_list)))
            ''' select collections with specified regex '''
            if collection_properties is not None and "COLLLIST" in collection_properties.keys():
                self._collections = list(filter(lambda _coll: 
                                                _coll in collection_properties['COLLLIST'],
                                                _coll_list
                                               ))
            elif collection_properties is not None and "HASINNAME" in collection_properties.keys():
                if isinstance(collection_properties['HASINNAME'],str):
                    ''' TODO redundant move all to a list, maybe option for OR/AND for 
                        a list of HASINNAME keys'''
                    r = re.compile(f"{collection_properties['HASINNAME']}")
                    self._collections = list(filter(r.search, _coll_list))
                elif isinstance(collection_properties['HASINNAME'],list):
                    self._collections = list(filter(lambda x: \
                                                    all(y in x \
                                                        for y in collection_properties['HASINNAME']),\
                                                    _coll_list))
            else:
                self._collections = _coll_list

            if self._collections is None or len(self._collections)<=0:
                logger.warning("%s collections class @property is empty %s object",
                               __s_fn_id__,type(self._collections))
            else:
                logger.debug("%s collections class property has %d elements in %s",
                               __s_fn_id__,len(self._collections),type(self._collections))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._collections


    ''' Function - data

            parameters:
                
            return (dtype) any data type: str, dict, list, dataframe, array, and so on

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def documents(self):
        """
        Description:
        Attributes:
        Returns:
        Exceptions:
        """
        return self._documents

    @documents.setter
    def documents(self, docMeta:dict):
        """
        Description:
        Attributes:
        Returns:
        Exceptions:
        """
    
        __s_fn_id__ = f"{self.__name__} function <@documents.setter>"

        _asType = None  # mandatory - data convertion type from store dict
        _dbName = None  # database name to query collections
        _dbColl = None   # mandatory - file path from store dict
        _docFind = None   # either - file name from store dict
#         _docType = None   # or - file type from store dict

        try:
            if ("ASTYPE" in [x.upper() for x in docMeta.keys()]) \
                and (docMeta['ASTYPE'].upper() in self._asTypeList):
                _asType = docMeta['ASTYPE']
            else:
                ''' set asTpe to a dict by daefulat '''
                _asType = "DICT"

            if not ("DBNAME" in [x.upper() for x in docMeta.keys()]):
                raise ValueError("Missing DBNAME and must be specified")
            _dbName = docMeta['DBNAME']

            ''' if not specified will return data from all collections '''
            if "COLLECTION" in [x.upper() for x in docMeta.keys()]:
                _dbColl = docMeta['COLLECTION']

            ''' if not specified will return data for all documents '''
            if "FIND" in [x.upper() for x in docMeta.keys()]\
                and isinstance(docMeta['FIND'],dict):
                _docFind = docMeta['FIND']
#             elif ("DOCTYPE" in [x.upper() for x in docMeta.keys()]) \
#                 and (docMeta['DOCNAME'].upper() in self._docTypeList):
#                 _docType = docMeta['DOCTYPE']
#             else:
#                 raise ValueError("Either a DOCNAME or DOCTYPE must be specified")

            self._documents = self.read_documents(
                as_type=_asType,
                db_name=_dbName,
                db_coll=_dbColl,
                doc_find=_docFind,
#                 doc_type=_fType,
            )
            logger.debug("%s execute find {%s} from %s",__s_fn_id__,str(_docFind), _dbColl)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._documents

    ''' --- REALM --- '''
    @property
    def realm(self) -> DataFrame:
        """
        Description:
            realm @property and @setter functions. make sure it is a valid realm
        Attributes:
            realm in @setter will instantiate self._realm  
        Returns :
            self._realm (str) 
        """

        __s_fn_id__ = f"{self.__name__} function <@property realm>"

        try:
            if self._realm.upper() not in self._realmList:
                raise KeyError("Invalid realm; must be one of %s"
                                 % self._realmList)
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._realm.upper()

    @realm.setter
    def realm(self,realm) -> DataFrame:

        __s_fn_id__ = f"{self.__name__} function <realm.@setter>"

        try:
            if realm.upper() not in self._realmList:
                raise KeyError("Invalid %s realm; must be one of %s"
                                 % (type(realm), self._realmList))

            self._realm = realm.upper()

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._realm

