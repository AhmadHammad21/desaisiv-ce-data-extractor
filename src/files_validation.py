import re
from brokercecore.data_extractors.active_list_extractor import ActiveListExtractor
from brokercecore.data_extractors.raw_data_extractor import RawDataExtractor

def is_raw_data(file):
    pattern = re.compile("[0-9]+(\\\\|/).*\.(xls|xlsx|xlsb|csv)$")
    return pattern.match(file.lower())

def is_active_list(file):
    pattern = re.compile("activelist(\\\\|/).*\.(xls|xlsx|xlsm|xltx|xltm|xlsb|csv)$")
    return pattern.match(file.lower())

def is_valid_report(file):
    pattern = re.compile("[0-9]+(\\\\|/).*\.(pdf)$")
    return pattern.match(file.lower())



def active_list_extraction(active_list_files: list, 
                           zip_path: str, 
                           final_data: dict):
    # Active List
    for i, active_list_file in enumerate(active_list_files):
        
        if is_lead_generation:
            active_list_object = ActiveListExtractor(f"{zip_path}{active_list_file}",partial_validation=True)
        else:
            active_list_object = ActiveListExtractor(f"{zip_path}{active_list_file}")
        active_list_object.process()

        try:
            active_list, classes = active_list_object.return_df_and_classes()
        except Exception as e:
            raise Exception(str(e) + f"\nfor {active_list_file}")
        
        if i == 0:
            final_data["active_list"] = {"data": active_list, "classes": classes}

        if "one_year" in active_list_file:
            final_data['one_year_previous_active_list'] = {"data": active_list, "classes": classes}

        if "two_year" in active_list_file:
            final_data['two_year_previous_active_list'] = {"data": active_list, "classes": classes}

    print("Done processing Active list")

    return final_data

def raw_data_extraction(raw_data_files: list,
                        zip_path: str,
                        final_data: dict):
    
    if len(raw_data_files) >= 1:
        raw_data = {}
        for file in raw_data_files:
            insur_company_id = int(re.sub(r"\\", "/", file).split("/")[0])
            if str(insur_company_id) not in raw_data.keys():
                raw_data[str(insur_company_id)] = []

            raw_data[str(insur_company_id)].append(f"{zip_path}{file}")

        final_data["raw_data"] = {}
        final_data["raw_data"]["data"] = {}

        for id in raw_data:
            final_data["raw_data"]["data"][id] = RawDataExtractor().process(raw_data[id])

    print("Done processing Raw Data")

    return final_data