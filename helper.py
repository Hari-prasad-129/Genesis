from datetime import datetime
import os
from pyarrow import parquet as pq

def convert_local_files_path():
   """this function splits the files which are stored in local

   Returns:
       list: [list_of_local_file_path_data]
   """
   list_of_conf = []
   for root, dirs, files in os.walk(os.getcwd() +'/store_local'):
      for file in files:
         root_folder = root.split('/store_local')
         if len(root_folder[1].split('/')) == 4:
               list_of_conf.append(root_folder[1])
    
   list_of_local_file_path_data = list(set(list_of_conf))
   return list_of_local_file_path_data


def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def file_size(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)



def row_count_in_file(file_path):
    try:
        size_of_file = pq.read_table(file_path,columns=[])
        return size_of_file.num_rows
    except:
        size_of_file = os.path.getsize(file_path)
        return size_of_file


def time_taken_to_upload_file(start_time):
   td = (datetime.now() - start_time).total_seconds() * 10**3
   seconds = str(td).split('.')[0]

   if (1000 >= float(seconds)):
      convert = (float(seconds)/1000)
      message = f"The time of execution to upload file is : {convert:.03f} secs"
   else:
      message = f"The time of execution to upload file is : {td:.03f}ms"
   
   return message


