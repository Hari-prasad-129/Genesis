# Python Modules
import os
from configparser import ConfigParser
from datetime import datetime
import sys
import time
import pandas as pd

# Project Modules
from source import SqlSourceConnection,MySqlSourceConnection,PostgresqlSourceConnection
from validate import Validate
from helper import *
# from main.source import testing
# from sourceConnector.postgresqlConnection import PostgresqlSourceConnection
# from sourceConnector.sqlServerConnection import SqlServerSourceConnection
# from sourceConnector.sqlConnection import SqlSourceConnection
# from sinkConnector.postgresqlConnection import PostgresqlSinkConnection
# from sinkConnector.sqlServerConnection import SqlServerSinkConnection
# from sinkConnector.sqlConnection import SqlSinkConnection
# from main.validate import Validate
# from main.logger_helper import log_output,configuration
# from main.sparkconfig import sparkConfig
# from main.helper import convert_local_files_path,file_size,row_count_in_file,time_taken_to_upload_file

# Other Modules
import psycopg2
from psycopg2 import extras
from psycopg2.extras import RealDictCursor
from azure.storage.blob import BlockBlobService

config = ConfigParser()
config.read(str(os.getcwd()) + '/config.ini')
config.sections()

storage_cred = config['BLOB']

class DataBaseConnection(Validate):

    def __init__(self):
        """Getting inputs from user for all the databases
        """
        self.config_meta_credentials = Validate.validate_config_meta_credentials()
        self.config_history_credentials = Validate.validate_config_history_credentials()
        self.source_credentials = Validate.validate_source_credentials()
        self.sink_credentials = Validate.validate_sink_credentials()

    
    def meta_db_connection(self,source_db):
        """Validating to connect to different databases

        Args:
            source_db (_type_): {json format}

        Returns:
            Class: Database connection
        """
        source = source_db['source']
        if source == "Postgresql":
            self.connection = PostgresqlSourceConnection.db_instance(source_db)
        elif source == 'Sql':
            self.connection = MySqlSourceConnection.db_instance(source_db)
        elif source == "Sqlserver":
            self.connection = SqlSourceConnection.db_instance(source_db)
        return self.connection

    def db_connections(self):
        """
        Connecting to all the database and creadting connection to individual database
        and returning the connection
        """
        self.config_meta_connection = self.meta_db_connection(self.config_meta_credentials)
        self.config_history_connection = self.meta_db_connection(self.config_history_credentials)
        self.source_connection = self.meta_db_connection(self.source_credentials)
        self.sink_connection = self.meta_db_connection(self.sink_credentials)
        return self.config_meta_connection,self.config_history_connection,self.source_connection,self.sink_connection

class ConfigMeta:

    meta_connection: None
    meta_cursor: None
    
    def test(self,config_meta_connection):
        query = "update config_meta_table\
                SET lmd='1900-01-01 00:00:00',previous_state=NULL,current_state=NULL "
        cur = config_meta_connection.cursor()
        cur.execute(query)
        config_meta_connection.commit()

    def meta_config_read(self,config_meta_connection):
        self.test(config_meta_connection)
        query = "SELECT * FROM airbyte.public.config_meta_table"
        ConfigMeta.meta_connection = config_meta_connection
        ConfigMeta.meta_cursor = config_meta_connection.cursor(cursor_factory=RealDictCursor)
        self.meta_cursor.execute(query)
        
        meta_data = []
        for data in self.meta_cursor.fetchall():
            if len(data) <= 0:
                print(IndexError)
                return IndexError
            else:
                dict_qs = {}
                for key,value in data.items():
                    dict_qs[key] = value
                meta_data.append(dict_qs)
        
        return meta_data

    
    def update_meta(self,meta_id,updated_last_modified_date,status=None):
        if status == None:
            update_lmd_query = f"UPDATE airbyte.public.config_meta_table\
                SET lmd= '{updated_last_modified_date}',\
                current_state = 'success'\
                WHERE id = {meta_id}"
            self.meta_cursor.execute(update_lmd_query)
            self.meta_connection.commit()
        else:
            update_lmd_query = f"UPDATE airbyte.public.config_meta_table\
                SET lmd= '{updated_last_modified_date}',\
                current_state = '{status}',\
                previous_state = CASE WHEN previous_state IS NULL THEN 'failed'\
                                ELSE previous_state\
                                    END\
                WHERE id = {meta_id}"
            self.meta_cursor.execute(update_lmd_query)
            self.meta_connection.commit()


class SourceLinkedList(ConfigMeta):

    def create_folder(self,db_folder,schema_folder):
        self.db_folder = []
        self.schema_folder = []

        if db_folder not in self.db_folder:
            
            self.db_folder.append(db_folder)
        if schema_folder not  in self.schema_folder:
            self.schema_folder.append(schema_folder)


    def read_source_table_data(self,source_connection,meta_data):
        for data in meta_data:
            db_name,schema_name,table_name = str(data['sourcedetails']).split('.')
            meta_id = data['id']
            last_modified_date = data['lmd']
            updated_last_modified_date = datetime.now()
            query = f"SELECT * FROM {db_name}.{schema_name}.{table_name} WHERE modified_at > '{last_modified_date}' and modified_at <= '{updated_last_modified_date}'"
            cur = source_connection.cursor()
            cur.execute(query)
            list_of_all_rows = []
            
            for row in cur.fetchall():
                list_of_all_rows.append(row)
            
            all_columns = [name[0] for name in cur.description]

            df = pd.DataFrame(list_of_all_rows,columns=all_columns)
            
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['modified_at'] = pd.to_datetime(df['modified_at'])
            
            
            if os.path.exists(os.getcwd() + '\store_local') == False:
                os.mkdir(os.getcwd() + f'\store_local')
            if (os.path.exists(os.getcwd() + f'\store_local\{db_name}')) == False:
                os.mkdir(os.getcwd()+f'\store_local\{db_name}')
            if os.path.exists(os.getcwd() + f'\store_local\{db_name}\{schema_name}') == False:
                os.mkdir(os.getcwd()+f'\store_local\{db_name}\{schema_name}')
            
            df.to_parquet(os.getcwd() + f"\store_local\{db_name}\{schema_name}\{table_name}.parquet")
            
            # try:
            #     rdd = spark.sparkContext.parallelize(tuple(list_of_all_rows)).toDF(columns)
            #     rdd.write.mode("overwrite").format("parquet").save(os.getcwd()  + f"/store_local/{db_name}/{schema_name}/{table_name}")
            #     # Calling parent class method to update last modified date in meta data table 
            #     super().update_meta(meta_id,updated_last_modified_date)

            # except Exception as e:
            #     status = "failed"
            #     super().update_meta(meta_id,updated_last_modified_date,status)
                # print(e)
                # sys.exit()


class ConfigHistory(DataBaseConnection):

    id = 0
    history_connection: None
    history_cursor: None

    def __init__(self):
        self.id += 1

    def history_log_data(self,config_history_connection,table_name,folderpath,blobpath,adls_start_time_to_upload,error_message=None):
        ConfigHistory.history_connection = config_history_connection
        ConfigHistory.history_cursor = config_history_connection.cursor()
        
        tablename = table_name
        blob_path = blobpath
        folderpath = os.getcwd() + '/store_local/' + folderpath
        size_of_data = file_size(folderpath)
        rowcount_src_to_adls = row_count_in_file(folderpath)
        timetaken_src_to_adls = time_taken_to_upload_file(adls_start_time_to_upload)
        error_msg_src_to_adls = error_message

        full_table_name = 'airbyte.public.config_history'
        tuples = []
        ts = (str(tablename),str(blob_path),str(size_of_data),str(rowcount_src_to_adls),str(timetaken_src_to_adls),str(error_msg_src_to_adls))
        tuples.append(ts)
        columns = 'tablename,folderpath,size_of_data,rowcount_src_to_adls,timetaken_src_to_adls,error_msg_src_to_adls'
        
        query = "INSERT INTO %s (%s) VALUES %%s" % (full_table_name,columns)
        
        try:
            extras.execute_values(self.history_cursor,query,tuples)
            # logger.info("Inserted data into Sink")
            config_history_connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error : %s" % error)
            # logger.error(("Error : %s" % error))
            config_history_connection.rollback()
            config_history_connection.close()
    

class AzureStorageAccountDetails():

    def azure_account_keys():
        try:
            block_blob_service = BlockBlobService(
                account_name=storage_cred['account_name'], 
                account_key= str(storage_cred['account_key']))
            # logger.info(block_blob_service)
            return block_blob_service
        except Exception as e:
            # logger.error(e)
            print(e)


class UploadToADLS(AzureStorageAccountDetails,ConfigHistory):

    def upload_to_blob(set_of_local_files_data,config_history_connection):
        
        block_blob_service = AzureStorageAccountDetails.azure_account_keys()
    
        for root_folder in set_of_local_files_data:
            adls_start_time_to_upload = datetime.now()
            file_path,db_name,schema_name,table_name = root_folder.split('/')

            file_path = db_name +"/"+ schema_name +"/"+ table_name
            complete_table_name = str(file_path).replace('/','.')

            aws_cred = config['BLOB']
            blob_client = block_blob_service
            
            for local_file in os.listdir(os.getcwd() + f'/store_local/{file_path}'):
                if 'parquet' == str(local_file)[-7:]:
                    try:
                        blob_client.create_blob_from_path(
                            str(aws_cred['container']) + '/' + file_path,
                            local_file, os.getcwd() + f"/store_local/{file_path}/{local_file}")
                        folderpath = file_path + "/" + local_file
                        
                        blobpath = aws_cred['container'] + '/' + file_path + "/" + local_file
                    
                        # logger.info("Files uploaded to ADLS")
                        config_history = ConfigHistory()
                        config_history.history_log_data(
                            config_history_connection,complete_table_name,
                            folderpath,blobpath,adls_start_time_to_upload)
                       
                    except Exception as error_message:
                        print(error_message,"error message")
                        folderpath = str(None)
                        blobpath = str(None)
                        config_history = ConfigHistory()
                        config_history.history_log_data(
                            config_history_connection,complete_table_name,
                            folderpath,blobpath,adls_start_time_to_upload,error_message)
                        


def insert_values(set_of_conf,sink_connection):
    for root_folder in set_of_conf:
        file_path,db_name,schema_name,table_name = root_folder.split('/')
        file_path += db_name +"/"+ schema_name +"/"+ table_name
        
        full_table_name = db_name +"."+ schema_name +"."+ table_name +"1"
        rdd = spark.read\
            .format("parquet")\
            .option("header","true")\
            .load(os.getcwd() + f'/store_local/{file_path}')
        
        df = rdd.toPandas()
        tuples = [tuple(x) for x in df.to_numpy()]
        columns = ','.join(list(df.columns))
        primary_key = columns.split(',')[0]
        query = f"INSERT INTO %s (%s) VALUES %%s \
            ON CONFLICT ({primary_key}) \
            DO\
                UPDATE SET " % (full_table_name,columns)
        # extracting primary key
        col_dict = {primary_key:primary_key}
        # looping all the columns and updating the upsert query with columns and values
        for col in columns.split(',')[1:]:
            query += (col +f" = EXCLUDED.{col},")
        # Excluding comma (,) at the end of the upsert query
        query = query[:-1]

        old_query = f"INSERT INTO %s (%s) VALUES %%s \
            ON CONFLICT ({primary_key})\
            DO\
            UPDATE SET first_name = EXCLUDED.first_name,\
            last_name = EXCLUDED.last_name,\
            created_at = EXCLUDED.created_at,\
            modified_at = EXCLUDED.modified_at" % (full_table_name,columns)

        cursor = sink_connection.cursor()

        try:
            extras.execute_values(cursor,query,tuples)
            logger.info("Inserted data into Sink")
            sink_connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error : %s" % error)
            logger.error(("Error : %s" % error))
            sink_connection.rollback()
            
    print("Inserted data into Target Databases")  




if __name__ == "__main__":
    # Connecting to All database i.e Meta Table,History Table,Source Table,Sink Table
    db = DataBaseConnection()
    config_meta_connection,config_history_connection,source_connection,sink_connection = db.db_connections()
   
    # Creating instance for meta table
    config_meta = ConfigMeta()
    # reading data from config meta table
    meta_data = config_meta.meta_config_read(config_meta_connection)
    print("Got Meta data")
    # Creating instance for Source table
    source = SourceLinkedList()
    # reading data from source table 
    source_data = source.read_source_table_data(source_connection,meta_data)
    print("Got source data")
    # converting localfiles int list of local files path
    list_of_local_files_path = convert_local_files_path()

    # # Uploading the local files to Azure Blob storage
    blob = UploadToADLS.upload_to_blob(list_of_local_files_path,config_history_connection)
    print("Uploaded to Blob")
    # insert_values(list_of_local_files_path,sink_connection)
   