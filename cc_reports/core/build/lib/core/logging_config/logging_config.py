import logging
import json 
import os

def log_config(config_path):

    with open(config_path, 'r') as config_file:
        config = json.load(config_file)

    log_file_path = config.get("log_file", "default.log")

    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)


    logging.basicConfig(
        filename=log_file_path,  
        filemode='a',       
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s' 
    )
    
    return logging.getLogger()
