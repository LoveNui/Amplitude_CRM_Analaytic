import json, threading, os
from ..model.crud import create_user, update_user, get_user_by_id

def analysis_file(data):
    final_result = {}

    for item in data:
        amplitude_id = item["amplitude_id"]
        if final_result.get(amplitude_id):
            pass
        else:
            final_result[amplitude_id] = {
                "user_id": 0,
                "total_amount" : 0,
                "purchase" : 0,
                "video" : 0,
                "event" : 0,
                "session" : []
            }
        
        if item["user_properties"].get("Player ID"):
            if final_result[amplitude_id]["user_id"] == 0:
                final_result[amplitude_id]["user_id"] = item["user_properties"]["Player ID"]
        
        final_result[amplitude_id]["event"] = final_result[amplitude_id]["event"] + 1

        if item.get("session_id"):
            if item["session_id"] != -1 :
                if not item["session_id"] in final_result[amplitude_id]["session"]:
                    final_result[amplitude_id]["session"].append(item["session_id"])
        
        if item["event_type"] == "Watched Video Ad":
            final_result[amplitude_id]["video"] = final_result[amplitude_id]["video"] + 1
        
        if item["event_properties"].get("$revenue"):
            final_result[amplitude_id]["total_amount"] = final_result[amplitude_id]["total_amount"] + float(item["event_properties"]["$revenue"])
            final_result[amplitude_id]["purchase"] = final_result[amplitude_id]["purchase"] + 1

    for i in list(final_result.keys()):
        sessions = final_result[i]["session"]
        final_result[i]["session"] = len(sessions)
        if len(sessions) == 0:
            final_result[i]["first"] = -1
            final_result[i]["last"] = -1
        else:
            final_result[i]["first"] = sessions[0]
            final_result[i]["last"] = sessions[-1]

    return final_result

def update_db(analysis_result):
    for i in analysis_result:
        user = get_user_by_id(i)
        # print(analysis_result[i])
        amplitude_id = i
        user_id = analysis_result[i]["user_id"]
        total_spent = analysis_result[i]["total_amount"]
        purchase = analysis_result[i]["purchase"]
        video = analysis_result[i]["video"]
        event = analysis_result[i]["event"]
    
        if user:
            session = analysis_result[i]["session"]
            if session == 0:
                first_session = user.first_session
                last_session = user.last_session
            else:
                if user.last_session == analysis_result[i]["first"]:
                    session = session - 1
                else:
                    pass
                if user.first_session:
                    first_session = user.first_session
                else:
                    first_session = analysis_result[i]["first"]
                last_session = analysis_result[i]["last"]
            v2_user = update_user(
                amplitude_id=amplitude_id,
                user_id=user_id,
                total_amont = user.total_amont + total_spent, 
                purchase = user.purchase + purchase, 
                video = user.video + video,
                event = user.event + event,
                session = user.session + session,
                first_session = first_session,
                last_session = last_session
            )
            if v2_user:
                pass
            else:
                print("-----------------------------------------------------")
                print(i)

        else:
            session = analysis_result[i]["session"]
            if session == 0:
                first_session = None
                last_session = None
            else:
                first_session = analysis_result[i]["first"]
                last_session = analysis_result[i]["last"]
            
            new_user = create_user(
                amplitude_id=amplitude_id,
                user_id=user_id, 
                total_amont=total_spent, 
                purchase=purchase, 
                video=video,
                event=event,
                session=session,
                last_session=last_session,
                first_session=first_session
                )
            if new_user:
                pass
            else:
                print("-----------------------------------------------------")
                print(i)

class Merge_result(object):
    def __init__(self, first_data:dict, second_data:dict):

        self.first_data = first_data
        self.second_data = second_data
        self.result = {}

    def reducer(self):
        # print("---------------- merge_data start ----------------")
        for i in list(self.first_data.keys()):
            first = self.first_data[i]
            if self.second_data.get(i):
                second = self.second_data.pop(i)
                self.result[i] = first
                m_r = {}
                if first["user_id"] == 0:
                    m_r["user_id"] = second["user_id"]
                else:
                    m_r["user_id"] = first["user_id"]
                m_r["total_amount"] = first["total_amount"] + second["total_amount"]
                m_r["purchase"] = first["purchase"] + second["purchase"]
                m_r["video"] = first["video"] + second["video"]
                m_r["event"] = first["event"] + second["event"]
                if first["last"] == second["first"] and second["first"] != -1:
                    m_r["session"] = first["session"] + second["session"] - 1
                else:
                    m_r["session"] = first["session"] + second["session"]
                if first["session"] == 0:
                    m_r["first"] = second["first"]
                else:
                    m_r["first"] = first["first"]
                m_r["last"] = second["last"]
                self.result[i] = m_r
            else:
                self.result[i] = first
        
        for y in list(self.second_data.keys()):
            self.result[y] = self.second_data[y]
        # print(self.result)
        # print("---------------- merge_data end ----------------")

def map_reduce(data):

    if len(data) == 0:
        return {}
    elif len(data) == 2:
        mk = Merge_result(first_data=data[0], second_data=data[1])
        mk.reducer()
        return mk.result
    elif len(data) == 1:
        return data[0]
    else:
        threads = []
        for i in range(0, len(data), 2):
            if i + 1 == len(data):
                threads.append(Merge_result(first_data=data[i], second_data={}))
            else:
                threads.append(Merge_result(first_data=data[i], second_data=data[i+1]))
        run_threads =[]
        for k in threads:
            thread1 = threading.Thread(target=k.reducer, args=())
            run_threads.append(thread1)
        for y in run_threads:
            y.start()
        for y in run_threads:
            y.join()
    
        results = []
        for m in threads:
            results.append(m.result)
        return map_reduce(results)

def merge_analysis_file(files, final_file):
    analysis = []
    for k in files:
        with open(k, 'r') as f:    
            analysis.append(json.load(f))
    print(f'[Info] Merge analysis')
    final_result = map_reduce(analysis)
    print(f'[Info] Save merge result')
    for i in files:
        os.remove(i)
    if final_result:
        with open(final_file, 'w') as f:
            json.dump(final_result, f, indent=2)
        return final_file
    else:
        return False

class Raw_data():
    def __init__(self, data:list):

        self.data = data
        self.result = {}

    def analysis_file(self):
        print("---------------- analysis start ----------------")
        final_result = {}
        with open(self.data) as f:
            raw = json.loads(f)
        for item in raw:
            amplitude_id = item["amplitude_id"]
            if final_result.get(amplitude_id):
                pass
            else:
                final_result[amplitude_id] = {
                    "user_id": 0,
                    "total_amount" : 0,
                    "purchase" : 0,
                    "video" : 0,
                    "event" : 0,
                    "session" : []
                }
            
            if item["user_properties"].get("Player ID"):
                if final_result[amplitude_id]["user_id"] == 0:
                    final_result[amplitude_id]["user_id"] = item["user_properties"]["Player ID"]
            
            final_result[amplitude_id]["event"] = final_result[amplitude_id]["event"] + 1

            if item.get("session_id"):
                if item["session_id"] != -1 :
                    if not item["session_id"] in final_result[amplitude_id]["session"]:
                        final_result[amplitude_id]["session"].append(item["session_id"])
            
            if item["event_type"] == "Watched Video Ad":
                final_result[amplitude_id]["video"] = final_result[amplitude_id]["video"] + 1
            
            if item["event_properties"].get("$revenue"):
                final_result[amplitude_id]["total_amount"] = final_result[amplitude_id]["total_amount"] + float(item["event_properties"]["$revenue"])
                final_result[amplitude_id]["purchase"] = final_result[amplitude_id]["purchase"] + 1

        for i in list(final_result.keys()):
            sessions = final_result[i]["session"]
            final_result[i]["session"] = len(sessions)
            if len(sessions) == 0:
                final_result[i]["first"] = -1
                final_result[i]["last"] = -1
            else:
                final_result[i]["first"] = sessions[0]
                final_result[i]["last"] = sessions[-1]

        self.result = final_result
        print("---------------- analysis end ----------------")

