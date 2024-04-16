from src.export_data.amplitude import get_data_from_amplitude
from src.export_data.extra_files import get_extra_data_file
from src.analysis.statistic import analysis_file, update_db, Raw_data, map_reduce, merge_analysis_file
import os, threading, json

import time


from datetime import date, timedelta, datetime

def time_seireas():
    start_dt = date(2020, 1, 1)
    end_dt = date(2020, 5, 31)
    # difference between current and previous date
    all_dates = {}
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        # Create a key as 'Year-Month'
        key = current_dt.strftime('%Y-%m')
        
        # Initialize the list for this month if not already done
        if key not in all_dates:
            all_dates[key] = []
        
        # Add the current date to this month's list
        all_dates[key].append(current_dt)
        
        # Increment the date by one day
        current_dt += delta

    return all_dates

def get_data_processing(start, end, this_date):
    print(f'data collecting {start} - {end}')
    raw_data = get_data_from_amplitude(start=start, end=end)
    order_raw_data = []
    for i in range(24):
        order_raw_data.append([])
    for i in raw_data:
        time = int(i.split("#")[0].split("_")[-1])
        order_raw_data[time].append(i)
    final_list =  []
    for i in order_raw_data:
        mk = ["000" for k in range(len(i))]
        for y in i:
            no = int(y.split("#")[-1].split(".json.gz")[0])
            mk[no] = y
        final_list.extend(mk)
    raw_data = []
    # raw_data = ['259095/259095_2022-11-16_0#0.json', '259095/259095_2022-11-16_1#0.json', '259095/259095_2022-11-16_2#0.json', '259095/259095_2022-11-16_3#0.json', '259095/259095_2022-11-16_4#0.json', '259095/259095_2022-11-16_5#0.json', '259095/259095_2022-11-16_6#0.json', '259095/259095_2022-11-16_7#0.json', '259095/259095_2022-11-16_8#0.json', '259095/259095_2022-11-16_9#0.json', '259095/259095_2022-11-16_10#0.json', '259095/259095_2022-11-16_11#0.json', '259095/259095_2022-11-16_12#0.json', '259095/259095_2022-11-16_13#0.json', '259095/259095_2022-11-16_14#0.json', '259095/259095_2022-11-16_15#0.json', '259095/259095_2022-11-16_16#0.json', '259095/259095_2022-11-16_17#0.json', '259095/259095_2022-11-16_18#0.json', '259095/259095_2022-11-16_19#0.json', '259095/259095_2022-11-16_20#0.json', '259095/259095_2022-11-16_21#0.json', '259095/259095_2022-11-16_22#0.json', '259095/259095_2022-11-16_23#0.json']
    print(f'[Info] Analysing each log files')
    for log in final_list:
        
        # file_path = os.path.join(log)
        data = get_extra_data_file(jsonfilename=log)
        analysis_result = analysis_file(data=data)
        new_file = log.replace('.gz', '')
        with open(new_file, 'w') as f:
            json.dump(analysis_result, f, indent=2)
            f.close()
        raw_data.append(new_file)
        os.remove(log)
    
    final_file = f'259095/{this_date.strftime("%Y-%m-%d")}.json'
    response = merge_analysis_file(files=raw_data, final_file=final_file)
    # if final_result == {}:
    #     pass
    # else:
    #     update_db(analysis_result=final_result)
    return response

    print("--------------------------------------------------------------")

start_time = time.time()

time_list = time_seireas()
start_date = datetime.strptime('20181125T23', '%Y%m%dT%H')
yearly = []
for y in time_list:
    print(f"--------- {y} ---------")
    monthly = []
    for i in time_list[y]:
        today = datetime.now()
        start = f'{i.strftime("%Y%m%d")}T00'
        if i == today.date():
            end = today.strftime("%Y%m%dT%H")
        elif i == start_date.date():
            start = start_date.strftime("%Y%m%dT%H")
        elif i > start_date.date() and i < today.date():
            end = f'{i.strftime("%Y%m%d")}T23'
        else:
            end = False

        if end:
            response = get_data_processing(start, end, i)
            if response:
                monthly.append(response)
    final_file = f"259095/{y}.json"
    response = merge_analysis_file(files=monthly, final_file=final_file)
    if response:
        yearly.append(final_file)

final_file = f"259095/{time_list[list(time_list.keys())[0]][0].year}.json"
merge_analysis_file(files=yearly, final_file=final_file)

end_time = time.time()
print(end_time - start_time)