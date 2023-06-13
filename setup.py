#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# ''' Initialize with default environment variables '''
# __name__ = "setup"
# __module__ = ""
# __package__ = ""
# __app__ = "utils"
# __ini_fname__ = "app.ini"

# ''' Load necessary and sufficient python librairies that are used throughout the class'''
# try:
#     ''' standard python packages '''
#     import os
#     import sys
#     import logging
#     import traceback
#     import configparser
#     import functools
# #     import pandas as pd
#     from datetime import datetime, date, timedelta

#     print("All packages in %s %s %s %s imported successfully!"
#           % (__app__,__module__,__package__,__name__))

# except Exception as e:
#     print("Some software packages in {0} didn't load\n{1}".format(__package__,e))

class Project():
    """ ***************************************************************************************
    
    contributors:
        * nuwan.waidyanatha@rezgateway.com
        
    ******************************************************************************************* 
    """


    def __init__(self, desc : str="setup", **kwargs):

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__desc__ = desc

        __s_fn_id__ = f"{self.__name__} function <__init__>"

        global config
        global logger
        global clsSpark

        try:
            self.cwd=os.path.dirname(__file__)
            sys.path.insert(1,self.cwd)

#             config = configparser.ConfigParser()
#             config.read(os.path.join(self.cwd,self.__ini_fname__))

#             self.rezHome = config.get("CWDS","REZAWARE")
#             sys.path.insert(1,self.rezHome)

            from rezaware import Logger as logs
            ''' innitialize the logger '''
            logger = logs.get_logger(
                cwd=self.rezHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s %s",self.__name__,self.__package__)

            ''' initialize util class to use common functions '''
            from utils.modules.lib.spark import execSession as session
            clsSpark = session.Spawn(desc=self.__desc__)
#             if clsSparkWL.session is None:
#                 clsSparkWL.session = {}
#             self.session = clsSparkWL.session

            ''' Set the utils root directory '''
            self.pckgDir = config.get("CWDS",self.__package__)
            self.appDir = config.get("CWDS",self.__app__)

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s.",
                         self.__app__,self.__module__,self.__package__,
                         self.__name__,self.__desc__)

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None

''' Function --- LOGGER ---
    author: <nuwan.waidyanatha@rezgateway.com>
'''
def get_logger(
    cwd:str=None,
    folder_path:str="logs",
    file_name:str="app.log",
):
    """
    Description:
        Invokes a logging instance for the setup process
    Attribute:
        NA
    Returns:
        logger (logger) object
    """
    __s_fn_id__ = f"{__name__} function <get_logger>"

    __def_log_form__ = '[%(levelname)s] - %(asctime)s - %(name)s - %(message)s'
    
    try:
        ''' validate function inputs '''
        if cwd is None or "".join(cwd.split())=="":
            cwd=os.path.dirname(__file__)
        if folder_path is None or "".join(folder_path.split())=="":
            folder_path="logs"
        if file_name is None or "".join(file_name.split())=="":
            file_name="app.log"

        ''' formulate file path '''
        if not os.path.isdir(os.path.join(cwd,folder_path)):
            os.makedirs(os.path.join(cwd,folder_path))
        _log_fpath=os.path.join(cwd,folder_path,file_name)
        if not os.path.exists(_log_fpath):
            with open(_log_fpath, 'w') as fp:
                pass

        ''' invoke logging instance '''
        logger = logging.getLogger("SETUP")
        logger.setLevel("DEBUG")
        if (logger.hasHandlers()):
            logger.handlers.clear()
        fh = logging.FileHandler(_log_fpath,'w+')
        fh.setLevel("DEBUG")
        formatter = logging.Formatter(__def_log_form__)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    except Exception as err:
        logger.error("%s %s \n",__s_fn_id__, err)
        logger.debug(traceback.format_exc())
        print("[Error]"+__s_fn_id__, err)

    return logger

''' Function --- MAIN --- 
    author: <nuwan.waidyanatha@rezgateway.com>
'''
def main():
    """
    Description:
        The main function will execute all steps for setting up a new project or
        updating the configuration of an existing project
    Attributes:
        NA
    Returns
        NA
    Exception(s):
    """

    __s_fn_id__ = f"{__name__} function <main>"

    global config
    global logger

    print("Starting setup and configure project")

    try:
        cwd=os.path.dirname(__file__)
        sys.path.insert(1,cwd)
        from rezaware import Logger as logs
        ''' innitialize the logger '''
        logger = get_logger(
            cwd=cwd,
            folder_path="logs",
            file_name = "app.log",
        )
        ''' set a new logger section '''
        logger.info('########################################################')
        logger.info("Initializing Main in %s",__name__)
        
        ''' get the parent folder '''
        parent_dir = os.path.abspath(os.path.join(cwd, os.pardir))
        
    except Exception as err:
        logger.error("%s %s \n",__s_fn_id__, err)
        logger.debug(traceback.format_exc())
        print("[Error]"+__s_fn_id__, err)

    return None

''' EXECUTION CONTROLLER '''
if __name__ == "__main__":

    ''' Initialize with default environment variables '''
    __name__ = "setup"
    __module__ = ""
    __package__ = ""
    __app__ = "utils"
    __ini_fname__ = "app.ini"
    __conf_fname__ = "app.cfg"
    __apps_list__ = ['utils','wrangler','visuals','mining']

    ''' Load necessary and sufficient python librairies that are used throughout the class'''
    try:
        ''' standard python packages '''
        import os
        import sys
        import logging
        import traceback
        import configparser
        import functools
    #     import pandas as pd
        from datetime import datetime, date, timedelta

        print("All packages in %s %s %s %s imported successfully!"
              % (__app__,__module__,__package__,__name__))

        main()

    except Exception as e:
        print("Some software packages in {0} didn't load\n{1}".format(__name__,e))
