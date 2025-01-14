import re

from datetime import datetime, timedelta

class AggregationPeriod:
    def __init__(self):
        self.type = None
        self.start = None
        self.end = None
        self.folders = []
        self.creation_period = {"start": None, "end": None, "periode": []}

    def reset(self):
        self.type = None
        self.start = None
        self.end = None
        self.folders = []
        self.creation_period = {"start": None, "end": None, "periode": []}

    def update_type(self, type):
        self.type = type

    def update_dates(self, date):
        if not self.start:
            self.start = date
        self.end = date

    def update_creation_dates(self, date):
        if not self.creation_period["start"]:
            self.creation_period["start"] = date
        self.creation_period["end"] = date

    def add_creation_periode(self, periode):
        self.creation_period["periode"].append( periode )

    def add_folder(self, folder):
        self.folders.append( folder )

    def finalize_creation_period(self):        
        dates = sorted(self.creation_period["periode"])
        start_date = dates[0] if dates else None
        end_date = dates[-1] if dates else None

        if self.type == "bulk":
            bulk_pattern = r"^[0-9]{4}_[0-9]{2}$"
            if re.search(bulk_pattern, start_date):
                start_date = datetime.strptime( f"{start_date}_01" , '%Y_%m_%d')
            
            if re.search(bulk_pattern, end_date):
                year, month = end_date.split('_')
                end_date = datetime.strptime( f"{end_date}_{self.get_last_day_of_month( int(year), int(month))}" , '%Y_%m_%d')
        
        if self.type == "periodically":
            periodic_pattern  = r"^[0-9]{4}_[0-9]{2}/[0-9]{2}$"
            if re.search(periodic_pattern, start_date):
                start_date = datetime.strptime( start_date , '%Y_%m/%d')

            if re.search(periodic_pattern, end_date):
                end_date = datetime.strptime( end_date , '%Y_%m/%d')

        self.creation_period["start"] = start_date
        self.creation_period["end"] = end_date

    def to_dict(self):
        return {
            "type": self.type,
            "start": self.start,
            "end": self.end,
            "folders": self.folders,
            "creation_period": self.creation_period,
        }
    
    def to_text(self):
        if self.type == "bulk":
            if self.start == self.end:
                return f"On {self.start.strftime('%Y-%m-%d')}: collected data from {self.creation_period['start']} - {self.creation_period['end']}"
            else:
                return f"Between {self.start.strftime('%Y-%m-%d')} - {self.end.strftime('%Y-%m-%d')}: collected data from {self.creation_period['start']} - {self.creation_period['end']}"

        if self.type == "periodically":
            if self.start == self.end:
                return f"On {self.start.strftime("%Y-%m-%d")}: collected data from {self.start.strftime("%Y-%m-%d")}"
            else:
                return f"Between {self.start.strftime("%Y-%m-%d")} - {self.end.strftime("%Y-%m-%d")}: periodically collected new data"

    @staticmethod    
    def get_last_day_of_month(year, month):
        # Get the first day of the next month and subtract one day
        next_month = datetime(year, month, 28)  + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
        return last_day.day
