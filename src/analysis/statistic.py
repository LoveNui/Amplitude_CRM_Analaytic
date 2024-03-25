import json
from ..model.crud import create_user, update_user, get_user_by_id

def analysis_file(data):
    final_result = {}

    for item in data:
        amplitude_id = item["amplitude_id"]
        if item.get(amplitude_id):
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
            final_result[amplitude_id]["user_id"] = item["user_properties"]["Player ID"]
        
        final_result[amplitude_id]["event"] = final_result[amplitude_id]["event"] + 1

        if item.get("session_id"):
            if item["session_id"] != -1 :
                if not item["session_id"] in final_result[amplitude_id]["session"]:
                    final_result[amplitude_id]["session"].append(item["session_id"])
        
        if item["event_type"] == "Watched Video Ad":
            final_result[amplitude_id]["video"] = final_result[amplitude_id]["video"] + 1
        
        if item["event_properties"].get("$price"):
            final_result[amplitude_id]["total_amount"] = final_result[amplitude_id]["total_amount"] + float(item["event_properties"]["$price"])
            final_result[amplitude_id]["purchase"] = final_result[amplitude_id]["purchase"] + 1
    
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
            session = len(analysis_result[i]["session"])
            if session == 0:
                first_session = user.first_session
                last_session = user.last_session
            else:
                if user.last_session == analysis_result[i]["session"][0]:
                    session = session - 1
                else:
                    pass
                if user.first_session:
                    first_session = user.first_session
                else:
                    first_session = analysis_result[i]["session"][0]
                last_session = analysis_result[i]["session"][-1]
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
            session = len(analysis_result[i]["session"])
            if session == 0:
                first_session = None
                last_session = None
            else:
                first_session = analysis_result[i]["session"][0]
                last_session = analysis_result[i]["session"][-1]
            
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
