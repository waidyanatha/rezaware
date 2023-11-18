#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "reference"
__package__= "utils"
__module__ = "lib"
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
    from datetime import datetime, date, timedelta

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS configure the master hotel details, groups, lookups, and locations

    Contributors:
        * nuwan.waidyanatha@rezgateway.com

    Resources:

'''

class dataWorkLoads():

    ''' Function --- INIT ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def __init__(
        self, 
        desc,
        realm:str= None,
        category:str=None,
        **kwargs):
        """
        Decription:
            Initializes the ExtractFeatures: class property attributes, app configurations, 
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

        self._tbl_name = "util_refer"
        self._data = None
        self._realm= realm
        self._realm_list=None
        self._category = category
        self._category_list=None

        global pkgConf
        global appConf
        global logger
        global clsSDB

        __s_fn_id__ = f"{self.__name__} function <__init__>"
        
        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.rezHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self.rezHome)

            ''' innitialize the logger '''
            from rezaware.utils import Logger as logs
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)

            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s Class",self.__name__)

            ''' import spark RDBM work load utils to read and write data '''
            from rezaware.modules.etl.loader import sparkRDBM as db
            clsSDB = db.dataWorkLoads(
                desc=self.__desc__,
            )
            if ("DBTYPE" in kwargs.keys() and "".join(kwargs['DBTYPE'].split()!=""))\
                or ("DBDRIVER" in kwargs.keys() and "".join(kwargs['DBDRIVER'].split()!=""))\
                or ("DBHOSTIP" in kwargs.keys() and "".join(kwargs['DBHOSTIP'].split()!=""))\
                or ("DBPORT" in kwargs.keys() and "".join(kwargs['DBPORT'].split()!=""))\
                or ("DBNAME" in kwargs.keys() and "".join(kwargs['DBNAME'].split()!=""))\
                or ("DBSCHEMA" in kwargs.keys() and "".join(kwargs['DBSCHEMA'].split()!=""))\
                or ("DBUSER" in kwargs.keys() and "".join(kwargs['DBUSER'].split()!=""))\
                or ("DBPSWD" in kwargs.keys() and "".join(kwargs['DBPSWD'].split()!="")):
                clsSDB.dbConnURL = kwargs
            if ("CONFIG" in kwargs.keys() and "".join(kwargs['CONFIG'].split()!=""))\
                or ("MASTER" in kwargs.keys() and "".join(kwargs['MASTER'].split()!=""))\
                or ("APPNAME" in kwargs.keys() and "".join(kwargs['APPNAME'].split()!=""))\
                or ("JARDIR" in kwargs.keys() and "".join(kwargs['JARDIR'].split()!="")):
                clsSDB.session=kwargs

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


    ''' Function --- CLASS PROPERTY SETTER & GETTER ---

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    ''' --- DATA --- '''
    @property
    def data(self):

        __s_fn_id__ = f"{self.__name__} function <@property data>"

#         __def_tbl_name__ = "util_refer"

        try:
            ''' validate property value '''
            if self._data is  None:
                _query = f"SELECT * FROM {clsSDB.dbSchema}.{self._tbl_name} "
                _query +=f"WHERE deactivate_dt IS NULL "
                self._data = clsSDB.read_data_from_table(select=_query)
                if self._data is None or self._data.count()<=0:
                    raise RuntimeError("Failed to retrieve %s realm data" % self._realm)
                logger.warning("%s read %d rows of data for %s realm", 
                               __s_fn_id__, self._data.count(), self._realm)
            if not isinstance(self._data,DataFrame):
                self._data = self.session.createDataFrame(self._data)
                logger.warning("%s non-pyspark dataset converted to %s with %d rows and %d columns", 
                               __s_fn_id__, type(self._data), 
                               self_.data.count(), len(self_.data.columns))
            if self._data.count() <= 0:
                raise AttributeError("No records in property data; empty %s" % type(self._data)) 
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):

        __s_fn_id__ = f"{self.__name__} function <@data.setter>"

        try:
            ''' validate property value '''
            if data is None:
                raise AttributeError("Cannot set class property data with empty %s" % type(data))
            if not isinstance(data,DataFrame):
                self._data = self.session.createDataFrame(data)
                logger.debug("%s %s dtype convereted to %s with %d rows %d columns",
                         __s_fn_id__,type(data),type(self._data),
                         self._data.count(),len(self._data.columns))
            else:
                self._data = data

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


    ''' --- REALM LIST --- '''
    @property
    def realm_list(self):

        __s_fn_id__ = f"{self.__name__} function <@property realm_list>"

        try:
            ''' validate realm_list value '''
            if not isinstance(self._realm_list, list) or len(self._realm_list)<=0:
                self._realm_list = [x[0] for x in 
                                     clsSDB.get_db_table_info(
                                         db_type=clsSDB.dbType,
                                         db_name=clsSDB.dbName,
                                         db_schema=clsSDB.dbSchema,
                                     )\
                                     .select("table_name").collect()]
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._realm_list


    ''' --- REALM --- '''
    @property
    def realm(self):

        __s_fn_id__ = f"{self.__name__} function <@property realm>"

        try:
            ''' validate realm value '''
            if not isinstance(self._realm, str) or "".join(self._realm.split())=="":
                raise AttributeError("Unspecified class property realm, %s" % type(self._realm))
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._realm

    @realm.setter
    def realm(self,realm):

        __s_fn_id__ = f"{self.__name__} function <@realm.setter>"

        try:
            ''' validate property value '''
            if realm not in self.realm_list:
                raise AttributeError("Cannot set class property with empty %s; specify one of %s" 
                                     % type(realm), self._realm_list)

            self._realm = realm

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._realm

    ''' --- CAEGORY --- '''
    @property
    def category(self):

        __s_fn_id__ = f"{self.__name__} function <@property category>"

        try:
            ''' validate category value '''
            if self._category not in self.category_list:
                raise AttributeError("Unspecified class property category, %s" % type(self._category))
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._category

    @category.setter
    def category(self,category):

        __s_fn_id__ = f"{self.__name__} function <@category.setter>"

        try:
            ''' validate property value '''
            if not isinstance(category, str) or "".join(category.split())=="":
                raise AttributeError("Cannot set class property with empty %s" 
                                     % type(category))

            self._category = category

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._category


    ''' --- CATEGORY LIST --- '''
    @property
    def category_list(self):

        __s_fn_id__ = f"{self.__name__} function <@property category_list>"

        try:
            ''' validate category_list value '''
            if not isinstance(self._category_list, list) or len(self._category_list)<=0:
                self._category_list = [x[0] for x in 
                                       self.data\
                                       .filter(F.col('realm').isin(self._realm))\
                                       .select("category").distinct().collect()]
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._category_list


    ''' Function --- GET REFERENCE ---

        author(s): <nuwan.waidyanatha@rezgateway.com>
    '''
    def get_reference(
        self,
        realm: str = None,
        category:str=None,
        **kwargs
    ) -> DataFrame:
        """
        """

        __s_fn_id__ = f"{self.__name__} function <@property category_list>"

        try:
            filt_ref_=self.data
            if isinstance(realm,str) and "".join(realm.split())!="":
                self.realm = realm
                filt_ref_=filt_ref_.filter(F.col('realm').isin(self._realm))
            if isinstance(category,str) and "".join(category.split())!="":
                self.category=category
                filt_ref_=filt_ref_.filter(F.col('category').isin(self._category))

            if not isinstance(filt_ref_, DataFrame) or filt_ref_.count()<=0:
                raise RuntimeError("returned %s empty reference data for %s realm and %s category" 
                                   % (type(filt_ref_), self._realm, self._category))
            logger.debug("%s filtered %d rows for %s realm and %s category", 
                         __s_fn_id__, filt_ref_.count(), self._realm, self._category)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return filt_ref_


