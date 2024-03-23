import json
from ..model.crud import create_user, update_user, get_user_by_id

def analysis_file(data):
    final_result = {}

    for item in data:
        try:
            userid  = item["user_properties"]["Player ID"]
            if item.get(userid):
                pass
            else:
                final_result[userid] = {
                    "total_amount" : 0,
                    "purchase" : 0,
                    "video" : 0,
                    "event" : 0,
                    "session" : []
                }
            if not item["event_type"] in ["[AppsFlyer] Install", "Account Created", "App Opened", "App Closed"]:
                if not item["session_id"] in final_result[userid]["session"] :
                    if item["session_id"] == -1 or item["session_id"] == None:
                        pass
                    else:
                        final_result[userid]["event"] = final_result[userid]["event"] + 1
                        final_result[userid]["session"].append(item["session_id"])
                        if item["event_properties"].get("$revenue"):
                            final_result[userid]["total_amount"] = final_result[userid]["total_amount"] + float(item["event_properties"]["$revenue"])
                            final_result[userid]["purchase"] = final_result[userid]["purchase"] + 1
                        
                        if item["event_type"] == "Watched Video Ad":
                            final_result[userid]["video"] = final_result[userid]["video"] + 1    
        except:
            pass
    
    return final_result

def update_db(analysis_result):
    for i in analysis_result:
        user = get_user_by_id(i)
        print(analysis_result[i])
        if analysis_result[i]["session"] == []:
            pass
        else:
            
            if user:
                if user.last_session == analysis_result[i]["session"][0]:
                    len_session = user.session + len(analysis_result[i]["session"]) - 1
                else:
                    len_session = user.session + len(analysis_result[i]["session"])
                v2_user = update_user(
                    user_id=i,
                    total_amont = user.total_amont + analysis_result[i]["total_amount"], 
                    purchase = user.purchase + analysis_result[i]["purchase"], 
                    video = user.video + analysis_result[i]["video"],
                    event = user.event + analysis_result[i]["event"],
                    session = len_session,
                    last_session = analysis_result[i]["session"][-1]
                )
                if v2_user:
                    pass
                else:
                    print("-----------------------------------------------------")
                    print(i)
            else:
                new_user = create_user(
                    user_id=i, 
                    total_amont=analysis_result[i]["total_amount"], 
                    purchase=analysis_result[i]["purchase"], 
                    video=analysis_result[i]["video"],
                    event=analysis_result[i]["event"],
                    session=len(analysis_result[i]["session"]),
                    last_session=analysis_result[i]["session"][-1]
                    )
                if new_user:
                    pass
                else:
                    print("-----------------------------------------------------")
                    print(i)
