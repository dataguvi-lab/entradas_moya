import appconfig as cfg
import pandas as pd
import logging
from dotenv import load_dotenv
from conn_pstg import start_connection_datalake, start_connection_datatalk

load_dotenv()
logger = logging.getLogger(cfg.APP_NAME)

class DataWrapper:

    @staticmethod
    def get_reports_pagamentos():
        conn = start_connection_datalake()           
        df = pd.read_sql_query(cfg.QUERY_PAGAMENTO, conn)
        df = pd.DataFrame(df)
        return df
    
    @staticmethod
    def get_group_gef():
        conn = start_connection_datalake()           
        df = pd.read_sql_query(cfg.QUERY_GROUP_GEF, conn)
        df = pd.DataFrame(df)
        return df
    
    @staticmethod
    def get_group_empenho():
        conn = start_connection_datalake()           
        df = pd.read_sql_query(cfg.QUERY_GROUP_EMPENHO, conn)
        df = pd.DataFrame(df)
        return df
    
    @staticmethod
    def get_data_venc():
        conn = start_connection_datalake()           
        df = pd.read_sql_query(cfg.QUERY_DATA_VENCIMENTO, conn)
        df = pd.DataFrame(df)
        return df
