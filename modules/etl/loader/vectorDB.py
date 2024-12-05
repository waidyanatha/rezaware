#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

''' Initialize with default environment variables '''
__name__ = "vectorDB"
__package__ = "loader"
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
    import functools
    import traceback

    import findspark
    findspark.init()
    from pyspark.sql import functions as F
#     from pyspark.sql.functions import lit, current_timestamp,col,isnan, when, count, countDistinct
    from pyspark.sql import DataFrame
    from typing import List, Iterable, Dict, Tuple, Any

    ''' fixing the problem with sqllite warning '''
    __import__('pysqlite3')
    # import sys
    # sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
    # import pysqlite3
    sys.modules['sqlite3'] = sys.modules["pysqlite3"]
    ''' langchain '''
    from langchain_community.embeddings import OllamaEmbeddings
    # from langchain_community.vectorstores import Chroma
    from langchain_chroma import Chroma
    ''' Chromadb '''
    import chromadb
    from chromadb.config import Settings

    from rezaware.modules.etl.loader import __propAttr__ as attr

    print("All functional %s-libraries in %s-package of %s-module imported successfully!"
          % (__name__.upper(),__package__.upper(),__module__.upper()))

except Exception as e:
    print("Some packages in {0} module {1} package for {2} function didn't load\n{3}"\
          .format(__module__.upper(),__package__.upper(),__name__.upper(),e))

'''
    Class reads and writes data from and to Vector databases using apache pyspark sql functions
        Current working databases: 
        * ChromaDB

    Contributors:
        * nuwan@soulfish.lk

    Resources:
        * Notebooks
            * To test this package use notebook in wrangler/notebooks/etl/load/
        * Installation guide
            * 
        * Acknowledgement
            * Followed some of the work from 
            https://www.linkedin.com/pulse/build-lightning-fast-rag-chatbot-powered-groqs-lpu-ollama-multani-ssloc
            * Consider dockerizing Chroma with 
            https://medium.com/@pierrelouislet/getting-started-with-chroma-db-a-beginners-tutorial-6efa32300902
'''
class dataWorkLoads(attr.properties):

    def __init__(
        self, 
        desc : str="spark vector workloads", # identifier for the instances
        db_type : str = "chromadb", # database type one of self._dbTypeList
        db_root : str = None,   # folder path to all databases and collections
        db_name : str = None,   # the child folder to use on root
        # db_name : str = None,
        **kwargs, # unused at the moment 
    ):
        """
        Description:
            Initializes the dataWorkLoads: class property attributes, app configurations, 
                logger function, data store directory paths, and global classes
        Attributes:
            desc (str) to change the instance description for identification
        Returns:
            None
        """

        ''' instantiate property attributes '''
        super().__init__(
#             desc=self.__desc__,
            realm="VECTORDB"
        )

        self.__name__ = __name__
        self.__package__ = __package__
        self.__module__ = __module__
        self.__app__ = __app__
        self.__ini_fname__ = __ini_fname__
        self.__conf_fname__ = __conf_fname__
        self.__desc__ = desc

        __s_fn_id__ = f"{self.__name__} function <__init__>"

        ''' Initialize the DB parameters '''
        self._dbType=db_type
        
        ''' initiate to load app.cfg data '''
        global logger
        global pkgConf
        global appConf

        try:
            self.cwd=os.path.dirname(__file__)
            pkgConf = configparser.ConfigParser()
            pkgConf.read(os.path.join(self.cwd,__ini_fname__))

            self.projHome = pkgConf.get("CWDS","PROJECT")
            sys.path.insert(1,self.projHome)

            ''' innitialize the logger '''
            from rezaware.utils import Logger as logs
            logger = logs.get_logger(
                cwd=self.projHome,
                app=self.__app__, 
                module=self.__module__,
                package=self.__package__,
                ini_file=self.__ini_fname__)
            ''' set a new logger section '''
            logger.info('########################################################')
            logger.info("%s %s",self.__name__,self.__package__)

            ''' Set the utils root directory '''
            self.pckgDir = pkgConf.get("CWDS",self.__package__)
            self.appDir = pkgConf.get("CWDS",self.__app__)
            ''' get the path to the input and output data '''
            self.dataDir = pkgConf.get("CWDS","DATA")

            appConf = configparser.ConfigParser()
            appConf.read(os.path.join(self.appDir, self.__conf_fname__))

            # __def_db_dir__ = "vectors"
            if db_root is None or "".join(db_root.split())=="":
                # self._dbRoot = os.path.join(
                #     pkgConf.get("CWDS","DATA"),__def_db_dir__)
                self._dbRoot = pkgConf.get("CWDS","DATA")
                logger.debug("%s setting %s default root path %s",
                             __s_fn_id__, self._dbType.upper(), self._dbRoot.upper())
            else:
                self._dbRoot = db_root
            __def_db_name__ = "vectors"
            if not isinstance(db_name,str) or "".join(db_root.split())=="":
                self._dbName = __def_db_name__
                logger.debug("%s setting %s at default baabase name %s",
                             __s_fn_id__, self._dbType.upper(), self._dbName.upper())
            else:
                self._dbName = db_name

            logger.debug("%s initialization for %s module package %s %s done.\nStart workloads: %s."
                         %(self.__app__,
                           self.__module__,
                           self.__package__,
                           self.__name__,
                           self.__desc__))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)

        return None


    ''' Function STORE VECTORS

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def store_vectors(
        self,
        documents:list=None, # list of document chunks
        db_name:str=None,    # optional folder to append to the root
        collection:str=None,   # the documents collection name
        embedding_fn:any=None, # embediing function to use
        **kwargs,
    )->Any:
        """
        Description:
            Stores the vector embedding of the documents in the given database and collection
        Attributes :
            documents (list) list of document chunks
            db_name (str) optional folder to append to the root
            collection (str) the documents collection name
            embedding_fn (any) embediing function to use
        Returns :
            vectorstore (any) specific vectorDB object
        Exceptions :
            collection must be specified
        """

        __s_fn_id__ = f"{self.__name__} function <store_vectors>"

        __def_embedding_fn__=OllamaEmbeddings(model='nomic-embed-text')
        # __def_db_dir__ = "vectors"

        try:
            ''' validate inputs and set to defaults '''
            if not isinstance(documents,list) or len(documents)<=0:
                raise AttributeError("Cannot store embeddings of %s documents" % type(documents))
            # if not os.path.isdir(db_root):
            #     db_root = os.path.join(
            #         pkgConf.get("CWDS","DATA"),__def_db_dir__
            #     )
            #     logger.debug("%s setting %s at default path %s", 
            #                  __s_fn_id__, self._dbType.upper(), 
            #                  db_root.upper())
            if isinstance(db_name,str) and "".join(db_name.split())=="":
                self.dbName = db_name
            # db_path=self._dbRoot
            # if isinstance(db_name,str) and "".join(db_name.split())!="":
            db_path = os.path.join(self._dbRoot,self.dbName)
            logger.debug("%s extending %s root path %s with %s dbname.",
                         __s_fn_id__, self._dbRoot, self._dbType.upper(), db_path.upper())
            if embedding_fn is None:
                embedding_fn = __def_embedding_fn__
            if not isinstance(collection,str) or "".join(collection.split())=="":
                raise AttributeError("Unspecified collection name %s; aborting!" 
                                     % type(collection))
            ''' store vector embeddings in vector database '''
            if self.dbType.lower() == 'chromadb':
                _client_settings = Settings(
                    is_persistent=True,
                    persist_directory=db_path,
                )
                vectorstore = Chroma.from_documents(
                    # persist_directory = db_path,
                    collection_name=collection,
                    documents=documents,
                    embedding=embedding_fn,
                    client_settings = _client_settings,
                    )
                if not isinstance(vectorstore,Chroma):
                    raise ChildProcessError("Failed to store vector embeddings in %s for %s at %s" 
                                            % (self._dbType.upper(), collection.upper(), 
                                               db_path.upper()))
            else:
                raise AttributeError("Invalid %s dbType %s" % (self._realm, self._dbType.upper()))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)
            return None

        finally:
            logger.debug("%s created vectorstore with %d documents in %s collection: %s at %s", 
                             __s_fn_id__, vectorstore._collection.count(), 
                         self._dbType.upper(), collection.upper(), db_path.upper())
            return vectorstore

    ''' Function GET COLLECTIONS

            author: <nuwan@soulfish.lk>
    '''
    def get_collections(
        self,
        db_name:str=None,
        **kwargs,
    )->Any:
        """
        """

        __s_fn_id__ = f"{self.__name__} function <get_collections>"

        __def_db_dir__ = "vectors"

        try:
            db_path=self._dbRoot
            if isinstance(db_name,str) and "".join(db_name.split())!="":
                db_path = os.path.join(self._dbRoot,db_name)
                logger.debug("%s extending %s path with dbname %s",
                             __s_fn_id__, self._dbType.upper(), 
                             db_path.upper())
            ''' read collection list from dbType '''
            if self._dbType=="chromadb":
                client = chromadb.PersistentClient(path=db_path)  # or HttpClient()
                collections = client.list_collections()
            else:
                raise AttributeError("Invalid %s dbType %s" % (self._realm, self._dbType.upper()))

            if not isinstance(collections,list) or len(collections)<=0:
                raise AttributeError("No collections in %s at %s" 
                                     % (self._dbType.upper(), db_path.upper()))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)
            return None

        finally:
            logger.debug("%s read %d collections in %s at %s", 
                             __s_fn_id__, len(collections), self._dbType.upper(), db_path.upper())
            return collections

    ''' Function READ VECTORS

            author: <nuwan.waidyanatha@rezgateway.com>
    '''
    def read_vectors(
        self,
        db_name:str=None,
        collection:str=None,
        embedding_fn:any=None,
        **kwargs,
    )->Any:
        """
        """

        __s_fn_id__ = f"{self.__name__} function <read_vectors>"

        __def_embedding_fn__=OllamaEmbeddings(model='nomic-embed-text')

        try:
            ''' validate inputs and set to defaults '''
            db_path=self._dbRoot
            if isinstance(db_name,str) and "".join(db_name.split())!="":
                db_path = os.path.join(self._dbRoot,db_name)
                logger.debug("%s extending %s path with dbname %s",
                             __s_fn_id__, self._dbType.upper(), 
                             db_path.upper())
            if embedding_fn is None:
                embedding_fn = __def_embedding_fn__
            if not isinstance(collection,str) or "".join(collection.split())=="":
                raise AttributeError("Unspecified collection name %s; aborting!" 
                                     % type(collection))
            ''' store vector embeddings in vector database '''
            if self.dbType.lower() == 'chromadb':
                _client_settings = Settings(
                    is_persistent=True,
                    persist_directory=db_path,
                )
                vectorstore = Chroma(
                    persist_directory = db_path,
                    collection_name=collection,
                    embedding_function=embedding_fn,
                    client_settings=_client_settings
                    )
                if not isinstance(vectorstore,Chroma):
                    raise ChildProcessError("Failed to read %s vector embeddings collection %s at %s" 
                                            % (self._dbType.upper(), collection.upper(), 
                                               db_path.upper()))
            else:
                raise AttributeError("Invalid %s dbType %s" % (self._realm, self._dbType.upper()))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)
            return None

        finally:
            logger.debug("%s loaded %s %s %s collection with %d documents at %s", 
                             __s_fn_id__, self._dbType.upper(), self._realm, collection.upper(), 
                         vectorstore._collection.count(), db_path.upper())
            return vectorstore

    ''' Function --- TEXT TO CHUNKS ---

        authors: <nuwan@soulfish.lk>
    '''
    @staticmethod
    def text_to_chunks(
        text:list=None,
        chunk_size:int=1000, 
        overlap:int=200,
        **kwargs
    )->List:
        """
        Description:
            Split the text to chunks
        Attributes :
            folder_path (str) directing to the folder
        Returns :
            documents (list)
        Exceptions :
            Incorrect folder path raizes exception
            Folder with no PDFs raises an exception
        """

        __s_fn_id__ = f"{aiWorkLoads.__name__} function <text_to_chunks>"

        try:
            ''' validate inputs '''
            if not isinstance(text,list) or len(text)<=0:
                raise AttributeError("Invalid %s text" % type(text))
            if not isinstance(chunk_size,int) and chunk_size<=0:
                raise AttributeError("Invalid chunk_size %d must be > 0; typically 1000")
            if not isinstance(overlap,int) and overlap<0:
                raise AttributeError("Invalid overlap %d must be >= 0")
            logger.debug("%s Splitting %d text documents into %d chunks with %d overlap", 
                         __s_fn_id__, len(text), chunk_size, overlap)
            ''' split the text '''
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=chunk_size, 
                chunk_overlap=overlap
            )
            chunks = text_splitter.split_documents(text)
            if not isinstance(chunks,list) or len(chunks)<=0:
                raise RuntimeError("Failed split %d text document" % len(text))

        except Exception as err:
            logger.error("%s %s \n",__s_fn_id__, err)
            logger.debug(traceback.format_exc())
            print("[Error]"+__s_fn_id__, err)
            return None

        finally:
            logger.info("%s Split %d document into %d chunks", __s_fn_id__, len(text), len(chunks))
            return chunks

    # Function to load and process web content
    def load_and_process_url(self,url):
        loader = WebBaseLoader(url)
        data = loader.load()
        text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
        return text_splitter.split_documents(data)

