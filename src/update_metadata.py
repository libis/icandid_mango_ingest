# To be used with ManGO Ingest: A metadata ingestion tool for iRODS collections
# This script integrates with the Mango Metadata Schema for schema validation and metadata application.

import sys
import os
import yaml
import ssl
from datetime import datetime
sys.path.append('/app/lib/mango-mdschema/')

import json
import pathlib
import re
import tarfile
from rich.console import Console

from irods.session import iRODSSession
from irods.collection import iRODSCollection

from pathlib import Path
from mango_mdschema.schema import Schema

verbosity_level = 0
console = Console()

## python print() override using Rich
def print(*args, verbosity=1, **kwargs):
    """Override the Python built in print function with the rich library version and only really print when asked"""
    if verbosity <= verbosity_level:
        console.log(*args, **kwargs)

## simple caching and re-use, to expand like mango flow/ mango portal with expiry checks?
irods_session: iRODSSession | None = None

## session init
def get_irods_session() -> iRODSSession:
    """
    Initialize and return an iRODS session.
    Ensures proper session management, including SSL configuration.
    
    Returns:
        iRODSSession: An active iRODS session object.
    """
    if irods_session:
        return irods_session

    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment.json")
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None
    )
    ssl_settings = {"ssl_context": ssl_context}

    print(f"Getting session with {env_file}", verbosity=2)

    return iRODSSession(irods_env_file=env_file, **ssl_settings)

def read_json(json_file):
    with open(json_file, "r") as f:
        return json.load(f)

def update_json(file, json_data):
    try:
        with open(file, "w") as f:
            json.dump( json_data, f, indent=2)
    except Exception as e:
        print(f"Could not update {file} ")
        print(f"{e} ")

def remove_none_values(d):
    """
    Recursively removes keys with None values from a nested dictionary or list.
    
    Args:
        d (dict or list): Input dictionary or list.
    
    Returns:
        dict or list: Cleaned dictionary or list with None values removed.
    """
    if isinstance(d, dict):
        return {k: remove_none_values(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [remove_none_values(v) for v in d]
    else:
        return d

def upload_metadata(metadata_file, path, config=None, irods_session=None):
    """
    Uploads metadata to iRODS using a schema if available.
    
    Args:
        metadata_file (str): Path to the metadata file (JSON format).
        path (str): Target iRODS collection path.
        kwargs: Additional arguments such as config and default_map.
    """
    metadata_json = read_json(metadata_file)
    schema_file = config[ f"{list(metadata_json.keys())[0]}_schema_file" ] 
    print (f"  schema_file : { schema_file }", verbosity=3 )

    if os.path.isfile(  schema_file ):
        mdschema = Schema(schema_file)
        upload_metadata_with_schema(metadata_json, path, mdschema,  config=config, irods_session=irods_session)
    else:
        print(
            f"schema {schema_file} not found\nOnly upload with metadata schema is suported"
        )

def upload_metadata_with_schema(metadata_json, path, mdschema,  config=None, irods_session=None):
    """
    Validates and applies metadata to an iRODS collection using a schema.
    
    Args:
        metadata_json (dict): Metadata to upload.
        path (str): iRODS collection path.
        mdschema (Schema): Mango schema object for validation and application.
        kwargs: Additional arguments such as config and default_map.
    """

    if not path:
        raise Exception("?path is missing! path is mandatory")  

    if not metadata_json:
        raise Exception("metadata_json is missing! metadata_json is mandatory")
 

    obj = irods_session.collections.get( path )

    mango_metadata = mdschema.extract(obj)
    #print (mango_metadata)
    #print (metadata_json)
    if metadata_json:
        cleaned_metadata =  remove_none_values(metadata_json)

        if json.dumps(cleaned_metadata, sort_keys=True) != json.dumps(mango_metadata, sort_keys=True):
            print (f"metadata for {obj.name} ({list(metadata_json.keys())[0]}) will be updated ", verbosity=3, style="#a86911")

            print (f"new metadata:   { json.dumps(cleaned_metadata, sort_keys=True)}", verbosity=3, style="#a86911")
            
            try:
                validated = mdschema.validate(metadata_json)
                mdschema.apply(obj, metadata_json) # includes validation
                print (f"add metadata schema '{list(metadata_json.keys())[0]}' to {obj.name}", verbosity=3, style="green")
            except (ConversionError, ValidationError) as err:
                print("Oops metadata for {mdschema.name} was not valid: {err}")
                raise Exception("Oops metadata for {mdschema.name} was not valid: {err}")  
        else:
            print (f"metadata for {obj.name} ({list(metadata_json.keys())[0]}) is uptodate ", verbosity=3, style="green")

    return metadata_json

   
def update(full_path: str, **kwargs):
    """
    Processes a directory path to upload metadata to iRODS.
    
    Args:
        full_path (str): Directory full_path to process.
        config_file (str, optional): Path to a JSON config file.
    
    if "{full_path}+verified.metadata.json" file exists, this data will be add/updated to Mango
    """
    try:   
        print (f"{ kwargs } ", verbosity=5 )
        if kwargs["config_file"]:
            config_file = kwargs["config_file"]
            config = read_json( config_file )

        if "irods_session" in kwargs:
            irods_session = kwargs["irods_session"]
        else:
            raise (BaseException ("Cannot obtain a valid irods session") )

        date_format = "%m/%d/%Y, %H:%M:%S" 
        irods_collection = kwargs['destination']
        sync = kwargs['sync']
        local_base_path = os.path.join(kwargs['path'], '')


        global verbosity_level
        verbosity_level = kwargs["verbosity"]  if kwargs["verbosity"]  else 0

            
        while os.path.join(full_path, '') != os.path.join(kwargs['path'], ''):
            metadata_file = full_path + ".verified.metadata.json"
            if not os.path.isfile( metadata_file ):
                metadata_file = full_path + ".metadata.json"

            print (f"{ full_path } contains { metadata_file }?: { os.path.isfile( metadata_file )}", verbosity=4 )

            local_path = pathlib.Path(full_path).absolute()
            rel_local_path = local_path.relative_to(local_base_path)

            if os.path.isfile( metadata_file ):
                if sync or (datetime.strptime(config["last_update_time"] , date_format).timestamp() < os.path.getmtime( Path(metadata_file) )):
                    print (f"update { full_path } with file { metadata_file }", verbosity=2 )
                    #print ( f" sync: {  sync }", verbosity=3)
                    #print ( f" getmtime: {os.path.getmtime( Path(metadata_file) )}", verbosity=3)
                    #print ( f" last_update_time: {datetime.strptime(config["last_update_time"] , date_format).timestamp()}", verbosity=3)
                    #print ( f" getmtime: { datetime.fromtimestamp( os.path.getmtime( Path(metadata_file) )).strftime(date_format) }", verbosity=3)
                    #print ( f" last_update_time: { config["last_update_time"] }", verbosity=3)
                    # consruct the irods destination full_path
                    dst_path = str(
                        pathlib.PurePosixPath(irods_collection, str(rel_local_path.as_posix()))
                    )
                    upload_metadata(metadata_file, dst_path, config=config, irods_session=irods_session )
                    print(f"{dst_path} metadata updated with {metadata_file}", verbosity=1)
                    
            full_path, last_element = os.path.split( full_path )            

        if config_file:
            config["last_update_time"] =  datetime.now().strftime(date_format)
            # print(f"Update {config_file} with {config["last_update_time"] }", verbosity=2) 
            update_json(config_file, config)

        return {}
    except Exception as e:
        print (e)
        raise (e)
        