#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "geomDB"
__module__ = "gis"
__package__ = "postgis"
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
    from pyspark.sql.functions import lit, current_timestamp
    from pyspark.sql import DataFrame
    from google.cloud import storage   # handles GCS reads and writes
    import pandas as pd
    import numpy as np
    import json

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    CLASS process postGIS geometry data queries for specific database, schema, and tables.

        Makes uses of
        * sparkRDBM for reading and writing date from and to database tables
        * each database schema will have a deafult table location_geom with
            table attributes for holding any of the database entity gis data

    Contributors:
        * nuwan.waidyanatha@rezgateway.com
        * samana.thetha@gmail.com

    Resources:
        https://postgis.net/docs/en/using_postgis_dbmanagement.html
'''
class dataWorkLoads():
    ''' Function --- INIT ---
    
        author: <nuwan.waidyanatha@rezgateway.com>

    '''
    def __init__(
        self, 
        desc : str = None,   # identifier for the instances
        db_name : str=None,
        db_schema:str=None,
        **kwargs,
    ):
        """
        Description:
            Initializes the postGIS dataWorkloads: class property attributes, app 
            configurations, logger function, data store directory paths, and global classes
        Attributes:
            desc (str) to change the instance description for identification
            db_name (str),
            db_schema (str),
        Returns:
            None
        """

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        if desc is not None or "".join(desc.split())!="":
            self.__desc__ = desc
        else:
            self.__desc__ = " ".join([self.__app__, self.__module__, self.__package__, self.__name__])

        ''' Initialize property var to hold the data '''
        self._data = None
        self._ConvCombo = [
            'decimal_to_degrees',
            'decimal_to_geometry',
            'degree_to_decimal',
            'degree_to_geometry',
            'geometry_to_decimal',
            'geometry_to_degree',
        ]
#         self._dbName = db_name     # sets the database to interact with
#         self._dbSchema=db_schema # specifies the schema

        ''' initiate to load app.cfg data '''
        global logger  # inherits the utils logger class
        global pkgConf # inherits package app.ini config data
#         global clsSDB  # inherits the loader sparkRDBM dataWorkLoads
        global clsSpark

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
            logger.info("%s %s",self.__name__,self.__package__)

#             ''' instantiate sparkRDBM dataWorkLoads '''
#             from rezaware.modules.etl.loader import sparkRDBM as db
#             clsSDB = db.dataWorkLoads(desc=self.__desc__)
            ''' instantiate sparkRDBM dataWorkLoads '''
            from rezaware.modules.lib.spark import execSession as spark
            clsSpark = db.spawn(desc=self.__desc__)

            logger.info("%s Connection complete! ready to load data.",__s_fn_id__)
            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__.upper(),
                           self.__module__.upper(),
                           self.__package__.upper(),
                           self.__name__.upper(),
                           self.__desc__.upper()))

            print("%s Class initialization complete" % self.__name__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None


    ''' Function -- DATA --
            TODO: 
            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    @property
    def data(self):
        """ @propert data function

            supports a class decorator @property that is used for getting the
            instance specific datafame. The data must be a pyspark dataframe
            and if it is not one, the function will try to convert the to a 
            pyspark dataframe.

            return self._data (pyspark dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <@property data>"

        try:
            if not isinstance(self._data,DataFrame):
                self._data = clsSpark.session.createDataFrame(self._data)
            if self._data.count() <= 0:
                raise ValueError("No records found in data") 

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data

    @data.setter
    def data(self,data):
        """ @data.setter function

            supports the class propert for setting the instance specific data. 
            The data must not be None-Type and must be a pyspark dataframe.

            return self._data (pyspark dataframe)
        """

        __s_fn_id__ = f"{self.__name__} function <@data.setter>"

        try:
            if data is None:
                raise AttributeError("Dataset cannot be empty")
            self._data = clsSpark.session.createDataFrame(data)
            logger.debug("%s data property %s set",__s_fn_id__,type(self._data))
                
        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return self._data


#     ''' Function -- CONVERT COORDINATES --

#             author: <nuwan.waidyanatha@rezgateway.com>
#     '''
#     def convert_coordinates(
#         self,
#         data : DataFrame = None, # non empty dataframe with coordinates columns
#         from_to : tuple = None,  # ordered pair defines convert from to method
#         *coord_cols,  # x, y, z, column names to apply convertion
#         **kwargs,  # additional behaviour changing key value pairs
#     ) -> DataFrame:
#         """
#         Description:
#             For the given columns arguments, the method will apply the coordinte convertions
#             on the defined columns from standard to the required to standard
#         Attributes :
#             * data (DataFrame) non empty dataframe with coordinates columns
#             * from_to (tuple)  ordered pair defines convert from to method
#             * columns (str)  column names to apply convertion
#             * kwargs (dict) additional behaviour changing key value pairs
#         Returns :
#             data (DataFrame) augmented with the new coordinate columns
#         Exceptions:
#             * data is None or empty raise AttributeError; abort
#             * from_to tuple values do not agree with predefined values raise ValueError
#                 try to detect and convert to default geometry format
#             * columns arguments are undefined raise AttributeError; try to detect columns
#                 and apply defined or default convertion
#         """

#         __s_fn_id__ = f"{self.__name__} function <__init__>"

#         __def_geom_col__ = "new_geom"

#         try:
#             ''' validate data '''
#             self.data = data
#             if self._data.count()<=0:
#                 raise AttributeError("Invalid empty %s" % type(self._data))

#             if "GEOMATTR" in kwargs.keys() or "".join(kwargs['GEOMATTR'].split())=="":
#                 if __def_geom_col__ not in self._data.columns:
#                     kwargs['GEOMATTR']=__def_geom_col__
#                 else:
#                     raise AttributeError("default column name %s already in data columns %s"+\
#                                          "use kwargs GEOMATTR key to define another column name" 
#                                          % (__def_geom_col__, self._data.columns))

#             if from_to.lower() == 'decimal_to_degrees':
#                 pass
#             elif from_to.lower() == 'decimal_to_geometry':
#                 pass
#             elif from_to.lower() == 'degree_to_decimal':
#                 pass
#             elif from_to.lower() == 'degree_to_geometry':
#                 pass
#             elif from_to.lower() == 'geometry_to_decimal':
#                 pass
#             elif from_to.lower() == 'geometry_to_degree':
#                 pass
#             elif from_to.lower() not in self._ConvCombo:
#                 raise AttributeError("Unrecognized convertion combination %s; did you mean %s" 
#                                      % (from_to.upper(), self._ConvCombo))
#             else:
#                 raise RuntimeError("Something went wrong")


#         except Exception as err:
#             logger.error("%s %s \n",__s_fn_id__, err)
#             logger.debug(traceback.format_exc())
#             print("[Error]"+__s_fn_id__, err)

#         return self._data
