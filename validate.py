import sys
import json

class Validate:

    @staticmethod
    def validate_config_meta_credentials():
        if len(sys.argv) < 2:
            print("error: no meta data database input file")
            sys.exit()
        else:
            json_file = open(sys.argv[1])
            data = json.load(json_file)
            return data

    @staticmethod
    def validate_config_history_credentials():

        if len(sys.argv) < 3:
            print("error: no history database input file")
            sys.exit()
        else:
            json_file = open(sys.argv[2])
            data = json.load(json_file)
            return data
    
    @staticmethod
    def validate_source_credentials():

        if len(sys.argv) < 4:
            print("error: no sink database input file")
            sys.exit()
        else:
            json_file = open(sys.argv[3])
            data = json.load(json_file)
            return data
    
    @staticmethod
    def validate_sink_credentials():

        if len(sys.argv) < 5:
            print("error: no sink database input file")
            sys.exit()
        else:
            json_file = open(sys.argv[4])
            data = json.load(json_file)
            return data