import os, os.path
from itertools import groupby, chain
from os import walk
import re
import json
import tarfile
import time


from rich.console import Console
from pprint import pprint

from datetime import datetime, timedelta, date

from lib.aggregation_periode import AggregationPeriod

console = Console()

class FolderProcessor:
    def __init__(self, root_folder="/", filter_pattern=".*", ignore_pattern=None, root_ingest_path=None, provider_dataset_regex=None):
        self.root_folder = root_folder
        self.filter_pattern = filter_pattern
        self.ignore_pattern = re.compile(f"{ignore_pattern}")
        self.provider_dataset_regex= re.compile(f"{provider_dataset_regex}")
        self.root_ingest_path = root_ingest_path
        self.folders = []
        self.pd_struct = {}



    def get_nth_part_of_path(self, path=None, delimiter="/", nth=0):
        parts = path.strip(delimiter).split(delimiter)
        return parts[nth] if len(parts) > nth else None     

    def sort_multi(self, lst, index_normal, index_reversed):
        return list(chain.from_iterable([sorted(list(j), key=lambda v:self.get_nth_element(v,index_reversed), reverse=True ) 
            for i, j in groupby(sorted(lst), key=lambda v:self.get_nth_element(v,index_normal) ) ] ) )

    def sort_multi_path(self, folders):
        folders_parts = list(map(lambda x: x.split(os.path.sep), folders ))
        folders_parts = self.sort_multi(folders_parts, 4, 6)
        return list( map(lambda x:os.path.join('/', *x), folders_parts ))


    def get_folders(self, path=None):
        if path == None:
            path= self.root_folder
        
        console.log(f"get folders from root_folder:{path} && filter:{self.filter_pattern} && ignore {self.ignore_pattern}", style="blue")

        stack = [path]
        folders = []
        while stack:
            current_path = stack.pop()
            with os.scandir(current_path) as dirpath:
                for entry in dirpath:
                    if entry.is_dir():
                        if re.search(self.filter_pattern, entry.path) and not re.search(self.ignore_pattern, entry.path):
                           folders.append(entry.path)
                        stack.append(entry.path)

        self.folders = self.sort_multi_path(folders)

        #for x in list(folders ):
        #    print(x)
        
        #self.folders = list(folders)
        self.get_provider_dataset_struc()
        return list(self.folders)
    

    def create_provider_metadata_file(self, provider=None,alternateName=None):
        if provider is not None and alternateName is not None:
            # create provider_metadate.json
            provider_metadata_file = os.path.join( self.root_ingest_path, provider+".metadata.json")
            provider_metadata = {"provider": {"name": provider, "alternateName": alternateName}}
            f = open(provider_metadata_file, "w")
            f.write(json.dumps(provider_metadata, indent=2, ensure_ascii=False))
            f.close()

    def create_dataset_metadata_file(self, provider=None, dataset=None, dataset_from_collection=None,folder_structure=None):
        if provider is not None and dataset is not None and dataset_from_collection is not None:
            dataset_metadata_file = os.path.join( self.root_ingest_path,  provider, dataset+".metadata.json")

            until =  "..." if dataset_from_collection["until"] == "2060-12-31" else dataset_from_collection["until"]
            dataset_metadata = {
                "dataset": {
                    "alternateName": dataset_from_collection["internalident"],
                    "description": dataset_from_collection["description"],
                    "license": dataset_from_collection["license"],
                    "name":dataset_from_collection["name"],
                    "temporalCoverage": dataset_from_collection["from"]+"/"+until,
                    "funder": {
                        "email": dataset_from_collection["requestoremail"],
                        "name": dataset_from_collection["requestor"]
                    },
                    "aggregationProcess": {
                        "aggregationPeriod": list( map(lambda ap:  ap.to_text(), folder_structure) )
                    }
                }
            }
            
            f = open(dataset_metadata_file, "w")
            f.write(json.dumps(dataset_metadata, indent=2, ensure_ascii=False))
            f.close()
            console.log(f"Metadata {dataset_metadata_file} created successfully!", style="green")

    def create_dataset_tar_files(self, provider=None, dataset=None, folder_structure=None):
        if provider is not None and dataset is not None:
            dataset_path = os.path.join( self.root_ingest_path,  provider, dataset )
            for folder in folder_structure:
                folder = folder.to_dict()
                if folder["type"] == "bulk":
                    tar_name_part = "bulk_records_created"
                if folder["type"] == "periodically":
                    tar_name_part = "periodically_created"

                archive_name = f"{folder["start"].strftime('%Y%m%d')}_{folder["end"].strftime('%Y%m%d')}_{tar_name_part}_{folder["creation_period"]["end"].strftime('%Y%m%d')}_{folder["creation_period"]["start"].strftime('%Y%m%d')}.tar.gz"
                console.log( f"Create {archive_name}", style="blue")
                archive_file = os.path.join( dataset_path , archive_name)
                self.create_tar_gz(archive_file, folder["folders"] )


    def create_folders_metadata(self, path=None, folders=[]):
        # create an aggragation_periode
        # Regex pattern with named groups

        aggregation_periode = AggregationPeriod()

        bulk_pattern = re.compile("(?P<path>"+ str(path) +")/(?P<aggregation_date>[0-9]{4}-[0-9]{2}-[0-9]{2})/backlog/(?P<creation_date>[0-9]{4}_[0-9]{2})")
        periodic_pattern = re.compile("(?P<path>"+ str(path) +")/(?P<aggregation_date>[0-9]{4}_[0-9]{2}/[0-9]{2})")

        periodic_folders =  list( filter(lambda f: re.search(periodic_pattern, f), folders) )
        predicted_period = self.predict_period_from_folders(periodic_folders,periodic_pattern=periodic_pattern)

        aggregation_periodes = []
        for folder in folders:
            match = re.search(bulk_pattern, folder)
            if match:
                if aggregation_periode.to_dict()["type"] is not None and aggregation_periode.to_dict()["type"] != "bulk": 
                    aggregation_periode.finalize_creation_period()
                    #if aggregation_periode.to_dict()["start"] == None:
                    #        print ("aggregation_periode. test1")
                    #        print (aggregation_periode.to_dict())
                    aggregation_periodes.append(aggregation_periode)                  
                    aggregation_periode = AggregationPeriod()
            
                aggregation_date = datetime.strptime( match.group('aggregation_date') , '%Y-%m-%d')
                creation_date_text = match.group('creation_date')
                creation_date = datetime.strptime( creation_date_text, '%Y_%m')
                creation_date = creation_date.replace(day=1)

                #prev_date_end  =  aggregation_periode.to_dict()["end"]
                #prev_creation_date_end =  aggregation_periode.to_dict()["creation_period"]["end"]
                #print ( f"aggregation_date: { aggregation_date }\nprev_date_end: {aggregation_periode.to_dict()["end"]}" )
                #print ( f"creation_date: { creation_date }\nprev_creation_date_end: {aggregation_periode.to_dict()["creation_period"]["end"]}" )
                if aggregation_periode.to_dict()["end"] is not None and aggregation_periode.to_dict()["end"] != aggregation_date:
                    # non-sequential aggregation dates detected
                    # build in tollerance of 7 days              
                    # Add the current aggregation_periode instance to the aggregation_periodes collection and reinitialize the aggregation_periode instance.
                    tolerance_period = timedelta(days=7)
                    diff = (aggregation_date - timedelta(days=1)) - aggregation_periode.to_dict()["end"]

                    #print (f"aggregation_date: {aggregation_date}")
                    #print (f"aggregation_periode.to_dict()[\"end\"]: {aggregation_periode.to_dict()["end"]}")
                    #print (f"aggregation_date - timedelta(days=1) - aggregation_periode.to_dict()[\"end\"]: {diff}")
                    
                    if tolerance_period < diff:
                        aggregation_periode.finalize_creation_period()
                        #if aggregation_periode.to_dict()["start"] == None:
                        #    print ("aggregation_periode. test2")
                        #    print (aggregation_periode.to_dict())
                        aggregation_periodes.append(aggregation_periode)
                        aggregation_periode = AggregationPeriod()


                if aggregation_periode.to_dict()["creation_period"]["end"] is not None:
                    diff_month = (creation_date.year - aggregation_periode.to_dict()["creation_period"]["end"].year) * 12 + (creation_date.month - aggregation_periode.to_dict()["creation_period"]["end"].month)
                    # sequential aggregation dates can contain the same creation month
                    # diff_month can be 0 or 1
                    if diff_month > 1:
                        # non-sequential creation months detected
                        # Add the current aggregation_periode instance to the aggregation_periodes collection and reinitialize the aggregation_periode instance.
                        aggregation_periode.finalize_creation_period()
                        aggregation_periodes.append(aggregation_periode)
                        aggregation_periode = AggregationPeriod()
                        

                # Update aggregation_periode instance
                aggregation_periode.update_type("bulk")
                aggregation_periode.update_dates(aggregation_date)
                aggregation_periode.update_creation_dates(creation_date)
                aggregation_periode.add_creation_periode(creation_date_text)
                aggregation_periode.add_folder(folder)
                
            match = re.search(periodic_pattern, folder)
            if match:
                if aggregation_periode.to_dict()["type"] is not None and aggregation_periode.to_dict()["type"] != "periodically": 
                    aggregation_periode.finalize_creation_period()
                    #if aggregation_periode.to_dict()["start"] == None:
                    #    print ("aggregation_periode. test4")
                    #    print (aggregation_periode.to_dict())     
                    aggregation_periodes.append(aggregation_periode)
                    aggregation_periode = AggregationPeriod()
                    
                aggregation_date = match.group('aggregation_date')
                creation_date_text = aggregation_date
                aggregation_date = datetime.strptime( match.group('aggregation_date') , '%Y_%m/%d')
                creation_date = aggregation_date

                #prev_creation_date_end =  aggregation_periode.to_dict()["creation_period"]["end"]

                #print (f"creation_date: {creation_date}\nprev_creation_date_end: {prev_creation_date_end}")
                #print (folder)
                if aggregation_periode.to_dict()["creation_period"]["end"] is not None:
                    diff_days = abs(creation_date - aggregation_periode.to_dict()["creation_period"]["end"]).days
                    #diff_days = (creation_date.year - aggregation_periode.to_dict()["creation_period"]["end"].year) * 12 + (creation_date.month - aggregation_periode.to_dict()["creation_period"]["end"].month)
                    # what should be considered as sequential aggregation/creation data 
                    # What if there are no tweets for a few days
                    # What on sundays for newpapers from BelgaPress
                    # Flemish Parliament documents are aggregated once a week
                    # ...
                    # To have some kind of tollerace in aggregation errors (if the collector didn't run for a few days)
                    # take the max value of ((predicted_period*2)+1) or (predicted_period+5)
                    # print (f" diff_days :::::::::: {diff_days}")
                    if diff_days > max(predicted_period+5,(predicted_period*2)+1):
                        # non-consecutive creation months detected
                        # Add the current aggregation_periode instance to the aggregation_periodes collection and reinitialize the aggregation_periode instance.
                        aggregation_periode.finalize_creation_period()
                        #if aggregation_periode.to_dict()["start"] == None:
                        #    print ("aggregation_periode. test5")
                        #    print (aggregation_periode.to_dict())                       

                        aggregation_periodes.append(aggregation_periode)
                        aggregation_periode = AggregationPeriod()
                        

                aggregation_periode.update_type("periodically")
                aggregation_periode.update_dates(aggregation_date)
                aggregation_periode.update_creation_dates(creation_date)
                aggregation_periode.add_creation_periode(creation_date_text)
                aggregation_periode.add_folder(folder)
                       
        aggregation_periode.finalize_creation_period()
        aggregation_periodes.append(aggregation_periode)

        return (aggregation_periodes)

    def get_pd_struct(self):
        return self.pd_struct
    
    def get_provider_dataset_struc(self):
        for f in self.folders:
            match = re.search(self.provider_dataset_regex, f)
            if match:
                if match.groupdict().get('provider'): 
                    provider = match.group('provider')
                    if provider not in  self.pd_struct:
                        self.pd_struct[provider]={} 
                    if match.groupdict().get('dataset'): 
                        dataset = match.group('dataset')
                        if dataset not in  self.pd_struct[provider]:
                            self.pd_struct[provider][dataset]=[] 
        
        

    @staticmethod
    def create_tar_gz(archive_name, folders):
        if not os.path.exists(archive_name):
            with tarfile.open(archive_name, "w:gz") as tar:
                for folder in folders:
                    tar.add(folder, arcname=folder.split('/')[-1])  # Add folder to the archive
            console.log(f"Archive {archive_name} created successfully!", style="green")
        else:
            console.log(f"Archive {archive_name} already existed!", style="red")

    @staticmethod
    def predict_period_from_folders(folders,periodic_pattern=None):
        periodic_dates =  list( map(lambda f: datetime.strptime(  re.search(periodic_pattern, f).group('aggregation_date') , '%Y_%m/%d'), folders) )
        differences = {}
        for i in range(1, len(periodic_dates)):
            diff = periodic_dates[i] - periodic_dates[i - 1]
            differences[diff.days] = differences.get(diff.days, 0) + 1
        
        if not differences:
            return 1
      
        return max(differences, key=differences.get) 

    @staticmethod
    # Function to extract the nth element
    def get_nth_element(parts, n):
        return parts[n] if n < len(parts) else ""  # Return nth element or empty string if out of range


#     def folder_to_struct(path, result_type="hash"):
#         if result_type not in ["hash", "array"]:
#             raise ValueError("result_type must be 'hash' or 'array'.")
# 
#         def build_hash(folder):
#             structure = {}
#             for entry in os.listdir(folder):
#                 full_path = os.path.join(folder, entry)
#                 if os.path.isdir(full_path):
#                     if re.search("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", entry):
#                         structure[entry] = build_array_wrapper(full_path)
#                     elif re.search("^[0-9]{4}_[0-9]{2}$", entry):
#                         structure[entry] = build_array_wrapper(full_path)
#             return structure
# 
#         def build_array_wrapper(folder):
#             structure = []
# 
#             def build_array(folder):
#                 for entry in os.listdir(folder):
#                     full_path = os.path.join(folder, entry)
#                     if os.path.isdir(full_path):
#                         if re.search("^[0-9]{4}_[0-9]{2}$", entry) or re.search("^[0-9]{2}$", entry):
#                             structure.append(entry)
#                         build_array(full_path)
#             build_array(folder)
#             return structure
# 
# 
#         if result_type == "hash":
#             return build_hash(path)
#         elif result_type == "array":
#             return build_array_wrapper(path)
# 