import requests
import io
import zipfile

def get_data_from_amplitude(start:str, end:str):
    url = f"https://amplitude.com/api/2/export?start={start}&end={end}"

    auth=requests.auth.HTTPBasicAuth('25277d5f0222c15e302f3ee147285a90', 'f0ff50e3b4b6af6470892a4767ee0337')
    response = requests.request("GET", url, auth=auth)
    try:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall()
        return z.namelist()
    except:
        return []