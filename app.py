from src.export_data.amplitude import get_data_from_amplitude
from src.export_data.extra_files import get_extra_data_file
from src.analysis.statistic import analysis_file, update_db
import os

from datetime import date, timedelta, datetime

def time_seireas():
    start_dt = date(2023, 1, 1)
    end_dt = date(2024, 3, 23)
    # difference between current and previous date
    delta = timedelta(days=1)
    # store the dates between two dates in a list
    dates = []
    while start_dt <= end_dt:
        # add current date to list by converting    it to iso format
        dates.append(start_dt)
        # increment start date by timedelta
        start_dt += delta
    return dates

def get_data_processing(start, end):
    print(f'data collecting {start} - {end}')
    raw_data = get_data_from_amplitude(start=start, end=end)
    order_raw_data = ["000" for i in range(24)]
    for i in raw_data:
        time = int(i.split("#")[0].split("_")[-1])
        order_raw_data[time] = i
    order_raw_data = [i for i in order_raw_data if i != "000"]
    for log in order_raw_data:
        print(f'[Info] {log}')
        # file_path = os.path.join(log)
        data = get_extra_data_file(jsonfilename=log)
        analysis_result = analysis_file(data=data)
        if analysis_result == {}:
            pass
        else:
          update_db(analysis_result=analysis_result)
        os.remove(log)
    print("--------------------------------------------------------------")

time_list = time_seireas()
start_date = datetime.strptime('20181125T23', '%Y%m%dT%H')
for i in time_list:
    today = datetime.now()
    # print(today.date())
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
        get_data_processing(start, end)