import os, os.path
import re
import json

from lib.folder_processor import FolderProcessor

from pathlib import Path

from pprint import pprint
import rich.panel
from rich.markup import escape
from rich.console import Console

'''
Het proces omvat het aanmaken van folders en bijbehorende metadata.json-bestanden in een gestructureerde map (config["mango_records_dir"]).
Deze mapstructuur, samen met de gegenereerde tar.gz-bestanden, kan vervolgens worden geüpload naar Mango via het mango_ingest.
De metadata (metadata.json) wordt automatisch gegenereerd op basis van de gebruikte mappenstructuur en de daarin aanwezige datums.
'''
console = Console()

###################################################################################### 
def exit_program():
    print("Exiting the program...")
    exit()
###################################################################################### 

def read_json_config(json_file) -> dict:
    with open(json_file, "r", encoding="utf-8") as f:
        return json.load(f)
###################################################################################### 

def load_config_file(file_path):
    try:
        return read_json_config(file_path)
    except Exception as e:
        console.print(f"Problem reading config file {file_path}: {e}", style="red bold")
        exit(1)
  
###################################################################################### 

def process_dataset(folder_processor,provider,dataset,folders,collections, config):

    ingest_path = Path(config["mango_records_dir"]) / provider / dataset
    
    os.makedirs(ingest_path, exist_ok=True)          
    
    dataset_from_collection = next((collection for collection in collections if collection["internalident"] == dataset), None) 
    if dataset_from_collection:
        folder_processor.create_provider_metadata_file(provider=provider,alternateName=dataset_from_collection['provider'])
        path = Path(config["root_folder"]) / provider / dataset
        console.log(f"folder {path} linked to") 
        console.log(f"{dataset_from_collection}") 

        #pattern = str(path)+ "/(.*[0-9]{4}-[0-9]{2}-[0-9]{2}/backlog/[0-9]{4}_[0-9]{2}|[0-9]{4}_[0-9]{2}/[0-9]{2})"
        #results =  sorted( list(filter(lambda f:  re.search(pattern, f), folders)) )
        dataset_pattern = re.compile(f"{path}/(.*[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}/backlog/[0-9]{{4}}_[0-9]{{2}}|[0-9]{{4}}_[0-9]{{2}}/[0-9]{{2}})")
        results = list([f for f in folders if dataset_pattern.search(f)])

        folder_structure = folder_processor.create_folders_metadata(path=path, folders=results)
        
#        for folder in folder_structure:
#            #print (folder_structure)
#            f = folder.to_dict()
#            print (f"{f.keys()}")
#            print (f"aggregation {f["start"]} - {f["end"]} ( {f["type"]} )")
#            print (f"creation    {f["creation_period"]["start"]} - {f["creation_period"]["end"]}")

        for folder in folder_structure:
            if folder.to_dict()['type'] == None:
                print (folder_structure)
                print (folder)
                exit(0) 

        folder_processor.create_dataset_metadata_file(
            provider=provider,
            dataset=dataset,
            dataset_from_collection=dataset_from_collection,
            folder_structure=folder_structure
        )

        folder_processor.create_dataset_tar_files(
            provider=provider,
            dataset=dataset,
            folder_structure=folder_structure
        )
    else:
        console.log(
            rich.panel.Panel(
                escape(
                    f"\nNo dataset description available in collections for {dataset}\n"
                    # f"collections: {collections}\n"
                ),
                style="red bold",
                expand=True,
            )
        )
###################################################################################### 
        

def entry_point():
    config_file = Path(os.getenv('CONFIG_FILE', '~/config/config.json')).expanduser()
    config = load_config_file(config_file)
   
    collections_file = config["collections_file"] if config["collections_file"] else '~/config/collections.json'
    collections = load_config_file(collections_file)

    folder_processor = FolderProcessor( 
        root_folder=config["root_folder"], 
        filter_pattern=config["folder_filter_pattern"],
        ignore_pattern=config["folder_ignore_pattern"],
        root_ingest_path=config["mango_records_dir"],
        provider_dataset_regex = config["provider_dataset_regex"]
    )

    folders = folder_processor.get_folders()
    pd_struct = folder_processor.get_pd_struct()

    for provider in pd_struct:
        console.log( provider )
        for dataset in list( pd_struct[provider].keys() ):
            console.log(  dataset ) 
            process_dataset(folder_processor, provider, dataset, folders, collections, config)
        

if __name__ == "__main__":
    entry_point()
    
exit_program()


