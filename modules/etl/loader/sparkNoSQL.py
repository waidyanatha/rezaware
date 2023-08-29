#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "sparkNoSQL"
__module__ = "etl"
__package__ = "loader"
__app__ = "rezaware"
__ini_fname__ = "app.ini"
__conf_fname__ = "app.cfg"

''' Load necessary and sufficient python librairies that are used throughout the class'''
try:
    import os
    import sys
    import functools
    import configparser    
    import logging
    import traceback
    import re
    from pymongo import MongoClient
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import *
    import pandas as pd
    from bson.objectid import ObjectId

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS read and write data to a given location:
        1) local directory
        2) Amazon S3 bucket

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:

'''
class dataWorkLoads():
    ''' Function
            name: __init__
            parameters:
                    @name (str)

            procedure: 

            return DataFrame

    '''
    def __init__(self,
                 desc:str="noSQL DB collection CRUD",
                 **kwargs,
                ):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        ''' --- NoSQL DB properties --- '''
        self._dbHostIP = None
        self._dbPort = None
        self._dbUser = None
        self._dbPswd = None
        self._dbAuthSource = None
        self._dbAuthMechanism = None
#         self._dbtls = None
#         self._dbtlsKeyFile = None
#         self._dbtlsCAFile = None
        
        self._dbType = None
        self._dbTypesList = [
            'mongodb',   # working and tested with community edition v4.4
            'cassandra', # TBD
            'hbase',   # TBD
            'neo4j',   # TBD
            'couchdb', # TBD
        ]
        self._dbName = None
        self._dbFormat = None
        self._collections = None
        self._connect = None
        self._documents = None

        self._asTypeList = [
            'STR',   # text string ""
            'LIST',  # list of values []
            'DICT',  # dictionary {}
            'ARRAY', # numpy array ()
            'SET',   # set of values ()
            'PANDAS', # pandas dataframe
            'SPARK',  # spark dataframe
        ]   # list of data types to convert content to
#         self._docTypeList = [
#             'CSV',   # comma separated value
#             'JSON',  # Javascript object notation
#             'TXT',   # text file
#         ]

        ''' --- SPARK properties --- '''
        self._sparkMaster =  None

        ''' initiate to load app.cfg data '''
        global logger
        global pkgConf
        global appConf
        
        __s_fn_id__ = f"{self.__name__} function <__init__>"
        
        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self.rezHome)
            ''' initialize the logger '''
            from rezaware.utils import Logger as logs
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class %s Package",self.__name__,self.__package__)

            ''' Set the wrangler root directory '''
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = pkgConf.get("CWDS","DATA")

            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self.appDir, self.__conf_fname__))

            logger.debug("%s initialization for %s module package %s %s done. Starting workloads: %s."
                         %(self.__app__.upper(),
                           self.__module__.upper(),
                           self.__package__.upper(),
                           self.__name__.upper(),
                           self.__desc__))
            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None

    ''' Functions - list of fget and fset @property and @*.setter functions
            dbHostIP, dbType, dbPort, dbDriver, dbName, 
            dbUser, dbPswd, dbAuthSource, dbAuthMechanism

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._documents

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
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
            if self._dbName is None and appConf.get('NOSQLDB','DBNAME'):
                self._dbName = appConf.get('NOSQLDB','DBNAME')
                logger.warning("%s set class @property dbName to %s",__s_fn_id__,self._dbName.upper())

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
            if self._dbUser is None and appConf.get('NOSQLDB','DBUSER'):
                self._dbUser = appConf.get('NOSQLDB','DBUSER')

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
            if self._dbPswd is None and appConf.get('NOSQLDB','DBPSWD'):
                self._dbPswd = appConf.get('NOSQLDB','DBPSWD')

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
                raise ValueError("Undefined dbType. It must be one of %s" % str(self._dbTypeList).upper())

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


    ''' Function - read_doc

            parameters:
                store_mode - local-fs sets to read and write on your local machine file system
                           aws-s3-bucket sets to read and write with an AWS S3 bucket 
            procedure: checks if it is a valid value and sets the mode
            return (str) self._documents

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def converter(func):

        @functools.wraps(func)
        def wrapper_converter(self,
                 as_type:str,   # mandatory - define the data type to return
                 db_name:str,
                 db_coll:str,      # mandatory - relative path, w.r.t. self.storeRoot
                 doc_find:dict={}, # optional - name of the file to read
#                  doc_type:str=None    # optional - read all the files of same type
                  **kwargs,
                ):

            __s_fn_id__ = f"{self.__name__} function <wrapper_converter>"
            self._documents = None
            _the_docs = None

            try:
                _the_docs = func(self,as_type,db_name,db_coll,doc_find, **kwargs)
                ''' validate before proceeding '''
                if not isinstance(_the_docs,list) or len(_the_docs)<=0:
                    raise AttributeError("No docs of %s received to process" % type(_the_docs))

                if as_type.upper() == 'DICT':
                    self._documents = list(_the_docs)
                elif as_type.upper() == 'STR':
                    self._documents=' '.join(list(_the_docs))
                elif as_type.upper() == 'PANDAS':
    #                 tmp_df = pd.DataFrame()
    #                 for _docs in _the_docs:
    #                     tmp_df = pd.concat([tmp_df,pd.DataFrame(_docs)])
    # #                     logger.debug("tmp_df type %s",type(tmp_df))
    #                 self._documents=tmp_df
                    self._documents=pd.DataFrame(_the_docs)
                    if "_id" in self._documents.columns:
                        self._documents['_id'] = self._documents['_id'].astype('str')
                elif as_type.upper() == 'SPARK':
                    self._documents=_the_docs
    #                 print("pandas",type(self._documents))
                else:
                    ''' dtype unspecified return as dictionary '''
                    self._documents=list(_the_docs)

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._documents

        return wrapper_converter

    @converter
    def read_documents(
        self,
        as_type:str = "",
        db_name:str = "",
        db_coll:list= [],
        doc_find:dict={},
        **kwargs):

        __s_fn_id__ = f"{self.__name__} function <read_documents>"

        _appName = " ".join([
            self.__app__,
            self.__name__,
            self.__package__,
            self.__module__
        ])   # spark app name

        doc_list_ = None
        doc_dics = None
        _docs_sdf = None

        try:
            if db_name is not None and "".join(db_name.split())!="":
                self.dbName = db_name
            if "DBAUTHSOURCE" in kwargs.keys():
                self.dbAuthSource = kwargs['DBAUTHSOURCE']
            elif self.dbAuthSource is None:
                self.dbAuthSource = self._dbName
            else:
                pass
            ''' read collections list from DB '''
            if len(db_coll)>0:
                self.collections={"COLLLIST":db_coll}
#             elif "HASINNAME" in kwargs.keys():
#                 self.collections=kwargs
            elif isinstance(kwargs,dict):
                self.collections=kwargs
            if self. _collections is None or len(self._collections)<=0:
                raise ValueError("%s database: %s has no collections" 
                                        % (self._dbType, self._dbName))
            logger.debug("%s Filtered %d collection(s) %s ... from %s %s",
                         __s_fn_id__,len(self._collections),
                         str(self._collections[:3]),self._dbType, self._dbName)

            if doc_find is None:
                doc_find = {}

#             logger.debug("%s Prepared to read documents from "+\
#                          "database %s, %d collection %s with %s find condition",
#                          __s_fn_id__,self.dbName,len(self.collections),
#                          str(self.collections[:3])+"...",doc_find)

#             if self.dbType.lower() == 'mongodb':
#                 ''' get data from MongoDB collection '''
#                 db = self.connect[self.dbName]
#                 _coll_list = db.list_collection_names()
#                 if _coll_list is None or len(_coll_list)<=0:
#                     raise DatabaseError("%s database: %s has no collections" 
#                                         % (self._dbType, self._dbName))
#                 logger.debug("%s %s database: %s has %d collections",
#                              __s_fn_id__,self._dbType, self._dbName,len(_coll_list))
#                 if self.collections:
# #                     logger.debug("Filtering collections by %s",str(self.collections))
#                     self._collections = list(filter(lambda _coll: 
#                                                     _coll in self.collections, 
#                                                     _coll_list
#                                                    ))
#                 else:
#                     logger.debug("%s No filters appied collections are %s",
#                                  __s_fn_id__,str(self.collections))
#                     self._collections = _coll_list

#                 if self.collections is not None and len(self.collections)
#                 logger.debug("%s Filtered %d set of collection %s",
#                              __s_fn_id__,str(self.collections[:3]))

            ''' read data from all the collections '''
            if as_type.upper() == "SPARK":
                ''' read with spark '''
#                     spark = SparkSession.builder.appName(_appName).getOrCreate()
#                     empty_rdd = spark.sparkContext.emptyRDD()
#                     _docs_sdf = spark.createDataFrame(data=empty_rdd,schema=StructType([]))
#                     _docs_sdf = spark.emptyDataFrame()
#                     _inp_uri = f"{self.dbType}://"+\
#                                 f"{self.dbUser}:"+\
#                                 f"{self.dbPswd}@"+\
#                                 f"{self.dbHostIP}/"+\
#                                 f"{self.dbName}."
                for _coll_idx,_coll in enumerate(self._collections):
                    try:
                        _inp_uri = f"{self.dbType}://"+\
                                    f"{self.dbUser}:"+\
                                    f"{self.dbPswd}@"+\
                                    f"{self.dbHostIP}/"+\
                                    f"{self.dbName}."+\
                                    f"{_coll}"+ \
                                    f"?authSource={self.dbAuthSource}"
#                             _inp_uri = _inp_uri + f"{_coll}" + f"?authSource={self.dbAuthSource}"
#                             logger.debug("%s input uri: %s",__s_fn_id__,_inp_uri)

                        # Create Spark session
                        spark = SparkSession.builder \
                            .appName(_appName) \
                            .master(self.sparkMaster) \
                            .config("spark.mongodb.input.uri", _inp_uri) \
                            .getOrCreate()

                        sdf = spark.read.format(self.dbFormat)\
                            .option( "uri", _inp_uri)\
                            .load()

                        if _coll_idx == 0:
                            _docs_sdf = sdf.alias('_docs_sdf')
                        else:
                            _old_docs_sdf = _docs_sdf.alias('_docs_sdf')
                            _docs_sdf = _old_docs_sdf.unionByName(sdf)
                            logger.debug("%s",str(_docs_sdf.head(10)))
                        logger.debug("%s Union sdf size %d",__s_fn_id__,_docs_sdf.count())

                    except Exception as err:
                        logger.warning("%s collection: %s in database: %s had errors: %s \n",
                                       __s_fn_id__,_coll, self.dbName, err)
                        logger.error(traceback.format_exc())
                        pass

                if (not _docs_sdf is None) and (_docs_sdf.count() > 0):
                    doc_list_=_docs_sdf

#                 logger.info("%s %s method Loaded %d documents from %d collections",
#                             __s_fn_id__,as_type.upper(), doc_list_.count(),len(self.collections))
            else:
                ''' read with pymongo '''
                doc_list_=[]
                if self.dbType.lower() == 'mongodb':
                    ''' get data from MongoDB collection '''
                    db = self.connect[self.dbName]

                    for _coll in self._collections:
                        try:
#                             logger.debug("%s Find %s in %s collection",__s_fn_id__,doc_find,_coll)
                            _coll_cur = db[_coll].find(doc_find)
                            if len(list(_coll_cur.clone())) <=0:
                                raise ValueError("No data")
                            doc_list_.extend(list(_coll_cur.clone()))

                        except Exception as err:
                            logger.warning("%s collection: %s in database: %s had errors: %s \n",
                                           __s_fn_id__,_coll, self._dbName, err)
                            pass

            if doc_list_ is None or len(doc_list_)<=0:
                raise RuntimeError("doc_list_ is an empty %s object" % type(doc_list_))
            logger.debug("%s %s method Loaded %d documents from %d collections",
                        __s_fn_id__,as_type.upper(), len(doc_list_),len(self._collections))

#             elif self.dbType.lower() == 'cassandra':
#                 ''' get data from Cassandra collection '''
#                 raise RuntimeError("cassandra read is tbd")
#             else:
#                 raise ValueError("Something was wrong")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return doc_list_

    ''' Function - write collection

            parameters:

            author: <nuwan.waidyanatha@rezgateway.com>
            
    '''
    def createDocs(func):

        @functools.wraps(func)
        def create_docs_wrapper(
            self,
            db_name:str,   # optional - name of the file to read
            db_coll:str,   # mandatory - relative path, w.r.t. self.storeRoot
            data,   # data to be stored
            uuid_list,
            **kwargs,
        ):

            __s_fn_id__ = f"{self.__name__} function <create_docs_wrapper>"

            try:
                if isinstance(data,list):
                    ''' list to collection '''
                    self._documents = data
                elif isinstance(data,dict):
                    self._documents = [data]
                elif isinstance(self.documents,pd.DataFrame):
                    ''' dataframe to collection '''
                    self._documents = [self.documents.to_dict()]
                else:
                    raise TypeError("Unrecognized data type %s must be either of\n%s"
                                    % (type(self.documents),str(self._asTypeList)))

                _collection=func(self,
                                 db_name,
                                 db_coll,
                                 data,
                                 uuid_list,
                                 **kwargs,
                                )

    #             if self.dbType.lower == 'mongodb':
    #                 _insert_ids = _collection.insert_many(self.documents)

    #             elif self.dbType.lower() == 'cassandra':
    #                 ''' get data from cassandra collection '''
    #                 raise RuntimeError("cassandra write is tbd")
    #             else:
    #                 raise ValueError("Something was wrong")

            except Exception as err:
                logger.error("%s %s \n",__s_fn_id__, err)
                logger.debug(traceback.format_exc())
                print("[Error]"+__s_fn_id__, err)

            return self._documents

        return create_docs_wrapper

    @createDocs
    def write_documents(
        self,
        db_name:str,   # optional - name of the file to read
        db_coll:str,   # mandatory - relative path, w.r.t. self.storeRoot
        data=None,   # data to be stored
        uuid_list:list=[],   # unique identifier name to check if document exists
        **kwargs
    ):
        """
        Description:
        Attributes :
        Returns :
        Exceptions :
        """

        __s_fn_id__ = f"{self.__name__} function <write_documents>"
        _collection = None, 
        _objIds = None

        try:
            ''' set connection parameters from kwargs '''
            if "DBUSER" in kwargs.keys() \
                and isinstance(kwargs['DBUSER'],str) \
                and "".join(kwargs['DBUSER'].split())!="":
                self.dbUser=kwargs['DBUSER']
            if "DBPSWD" in kwargs.keys() and \
                isinstance(kwargs['DBPSWD'],str) \
                and "".join(kwargs['DBPSWD'].split())!="":
                self.dbUser=kwargs['DBPSWD']
            if "DBAUTHSOURCE" in kwargs.keys() \
                and isinstance(kwargs['DBAUTHSOURCE'],str) \
                and "".join(kwargs['DBAUTHSOURCE'].split())!="":
                self.dbUser=kwargs['DBUSER']
            if "DBAUTHMECHANISM" in kwargs.keys() \
                and isinstance(kwargs['DBAUTHMECHANISM'],str) \
                and "".join(kwargs['DBAUTHMECHANISM'].split())!="":
                self.dbUser=kwargs['DBAUTHMECHANISM']

            logger.debug("%s Begin writing document to %s",__s_fn_id__,self.dbType.upper())

            ''' check if collection exists; else create one '''
            if self.dbType.lower() == 'mongodb':
                db = self.connect[db_name]
                ''' confirm database exists '''
                if not db_name in self.connect.list_database_names():
                    if "FORCEDB" in kwargs.keys() and kwargs['FORCEDB']==True:
                        ''' create a new database '''
                        db_name = self.create_db(
                            db_name = db_name,
                            db_user = self._dbUser, # 'rezawareAdmin',
                            db_pswd = self._dbPswd, # 'rezaware',
                            db_authSource=self._dbAuthSource,    # 'admin',
                            db_authMech = self._dbAuthMechanism, # 'SCRAM-SHA-256',
                        )
                        logger.debug("%s forced to create new database %s", __s_fn_id__, db_name.upper())
                        ''' reconnect to newly created db '''
                        db = self.connect[db_name]
                    else:
                        raise RuntimeError("database %s does not exist" % db_name.upper())
                ''' get data from MongoDB collection '''
                if db_coll not in db.list_collection_names():
                    _collection = db[db_coll]
                    logger.info("Created a new collection %s",_collection)
                    ''' insert all the documents '''
                    results = db[db_coll].insert_many(self.documents)
                    logger.info("Inserted %d documents",db[db_coll].count_documents({}))
                else:
                    _insert_count = 0
                    _modify_count = 0
                    for data_dict in self.documents:
#                         ''' add ObjectIds if not in data '''
#                         if '_id' not in data_dict.keys() \
#                             or data_dict['_id'] is None \
#                             or data_dict['_id']=="":
#                             new_id = ObjectId()
#                             while not db[db_coll].find({"_id": {"$eq": ObjectId(new_id)}}):
#                                 new_id = ObjectId()
#                             data_dict['_id']=ObjectId(new_id)

                        ''' find and update on the key list '''
                        if not (len(uuid_list) > 0):
#                             _filter_dict = {}
#                             for _key in uuid_list:
#                                 _filter_dict[_key]=data_dict[_key]
                            uuid_list = list(data_dict.keys())
#                         else:
#                             uuid_list = data_dict.keys()

#                         print(uuid_list)
                        _filter_dict = {}
                        for _uuid in uuid_list:
                            _filter_dict[_uuid]=data_dict[_uuid]
#                         print(_filter_dict)
                        _filtered_cur = db[db_coll].find(_filter_dict)
#                         for x in _filtered_cur.clone():
#                             print(x)
                        ''' if no match then insert with new ObjectID '''
                        if len(list(_filtered_cur.clone())) <=0:
                            ''' add ObjectIds if not in data '''
                            doc = db[db_coll].insert_one(data_dict)
                            _insert_count += 1
#                             print('insert count:',doc.inserted_count)
#                             logger.info("Modified %d documents",doc.inserted_count)
#                             if '_id' not in data_dict.keys() \
#                                 or data_dict['_id'] is None \
#                                 or data_dict['_id']=="":
# #                                 _filtered_cur = data_dict
#                                 new_id = ObjectId()
#                                 while not db[db_coll].find({"_id": {"$eq": ObjectId(new_id)}}):
#                                     new_id = ObjectId()
#                                 data_dict['_id']=ObjectId(new_id)
# #                                 _filtered_cur['_id']=ObjectId(new_id)
#                             ''' insert the new data '''
#                             _filtered_cur.append(data_dict)
#                             print('inserted',doc)
                        else:
                            ''' update all matching documents '''
                            for _cur in _filtered_cur:

                                ''' update with existing ObjectId '''
#                                 doc = db[db_coll].update_one({'_id':_cur['_id']},{"$set": data_dict})
                                doc = db[db_coll].update_one(
                                    filter={
                                        '_id' : _cur['_id'],
                                    },
                                    update={
    #                                             '$setOnInsert': data_dict,
                                        '$set': data_dict,
                                    },
                                    upsert=True,
                                )
                                _modify_count += 1
                    print('Total %d documents, successful insert count = %d & modify count = %d'
                          %(len(self.documents),_insert_count, _modify_count))
                    logger.info("Total %d documents, successful insert count = %d & modify count = %d",
                                len(self.documents),_insert_count, _modify_count)

            elif self.dbType.lower() == 'cassandra':
                ''' get data from cassandra collection '''
                raise RuntimeError("cassandra write is tbd")
            else:
                raise ValueError("Something was wrong")

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return db[db_coll]

    ''' Function --- CREATE DATABASE ---

        author <nuwan.waidyanatha@rezgateway.com>
    '''
    def create_db(
        self,
        db_type : str = None,
        db_name : str = None,   # Valid string
        db_user : str = None,
        db_pswd : str = None,
        db_authSource : str = None,
        db_authMech:str=None,
        **kwargs,
    ) -> str:
        """
            Description:
                Create a database, for the set dbType with given name, authemtication, and
                authentication mechanism. 
                If a username and password is not provided, then the system user and the system
                paswword is used.
            Attributes:
            
        """
        __s_fn_id__ = f"{self.__name__} function <create_db>"

        try:
            self.dbType = db_type
            self.dbName = db_name
            self.dbUser = db_user
            self.dbPswd = db_pswd
            self.dbAuthSource=db_authSource
            self.dbAuthMechanism=db_authMech

            if self._connect is None:
                self.connect={}
            try:
                _dbs = self._connect.list_database_names()
                if self._dbName in _dbs:
                    logger.debug("%s %s is 1 of %d databases in %s",
                                 __s_fn_id__,self._dbName,len(_dbs),self._dbType)
                else:
                    db = self.connect[self._dbName]
                    coll = db["about"]
                    _about_doc = {"name":self._dbName,"desc":self._dbName}
                    ret_ = coll.insert_one(_about_doc)
                    logger.debug("%s id:%s inserted about collection in %s",
                                 __s_fn_id__,ret_.inserted_id,self._dbName)

            except Exception as con_err:
                logger.warning("%s %s \n",__s_fn_id__, con_err)
                logger.warning("%s %s does not exist, creating new database"
                               ,__s_fn_id__,self._dbName)
        
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._dbName


    ''' convert to dict '''
    def convert_2_dict_mongodb(obj):
        result = {}
        for key, val in obj.items():
            if not isinstance(val, dict):
                result[key] = val
                continue

            for sub_key, sub_val in val.items():
                new_key = '{}.{}'.format(key, sub_key)
                result[new_key] = sub_val
                if not isinstance(sub_val, dict):
                    continue

                result.update(convert_2_dict_mongodb(result))
                if new_key in result:
                    del result[new_key]

        return result