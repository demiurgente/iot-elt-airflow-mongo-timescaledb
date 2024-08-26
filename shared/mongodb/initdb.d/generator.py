import pandas as pd
import numpy as np
import uuid
from datetime import datetime as dt
import datetime
import os
import requests, zipfile, io


OSFM_URI = "https://datasets.simula.no/downloads/pmdata.zip"
cut_off_date = "20200101"
bucket_interval = "4h"
r = requests.get(OSFM_URI)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("./pmdata")

participants = [i for i in range(1,17)]

def get_dir(x):
    path = "./pmdata/p"
    if x < 10:
        return path + f"0{x}/fitbit/"
    else:
        return path + f"{x}/fitbit/"

def process_records():
    hrs = []
    steps = []
    sleeps = []
    users = []
    for p in participants:
        hr = pd.read_json(f"{get_dir(p)}heart_rate.json")
        hr.rename(columns={"dateTime":"dt_key"}, inplace=True)
        hr.set_index("dt_key", inplace=True)
        hr = pd.DataFrame(hr['value'].tolist(),index=hr.index)
        hr = hr.reset_index()
        hr = hr[['dt_key','bpm', 'confidence']]\
            .groupby(hr.dt_key.dt.floor(bucket_interval)) \
            .apply(lambda g: [{
                'ts': x[0],
                'bpm': x[1],
                'confidence': x[2]
                } for x in g.values.tolist()])
        hr = hr.reset_index()
        hr = hr[pd.to_datetime(hr.dt_key) <= cut_off_date]
        print("hr")
        hr.rename({0:"metrics","dt_key":"created_at"},inplace=True,axis=1)
        hr['ended_at'] = hr.created_at + pd.Timedelta(bucket_interval)
        device_id = uuid.uuid4().hex
        hr['device_id'] = device_id
        hr = hr[['device_id','created_at','ended_at','metrics']]
        hrs.append(hr)
        sp = pd.read_json(f"{get_dir(p)}sleep.json")
        sp = sp[["startTime","endTime","duration"]]
        sp.columns=["sleepStartTs","sleepEndTs","duration"]
        sp["sleepStartTs"] = pd.to_datetime(sp.sleepStartTs)
        sp["sleepEndTs"] = pd.to_datetime(sp.sleepEndTs)
        sp['dt_key'] = sp['sleepEndTs']
        sp = sp[pd.to_datetime(sp.dt_key) <= cut_off_date]
        print("sp")
        sp = sp \
            .groupby(sp.dt_key.dt.floor(bucket_interval)) \
            .apply(lambda g: [{
                'start_ts': x[0],
                'end_ts': x[1],
                'duration': x[2]
                } for x in g.values.tolist()])
        sp = sp.reset_index()
        sp.rename({0:"metrics","dt_key":"created_at"},inplace=True,axis=1)
        sp['ended_at'] = sp.created_at + pd.Timedelta(bucket_interval)
        sp['device_id'] = device_id
        sp = sp[['device_id','created_at','ended_at','metrics']]
        sleeps.append(sp)
        st =  pd.read_json(f"{get_dir(p)}steps.json")
        st['dt_key']=st.dateTime
        st = st[pd.to_datetime(st.dt_key) <= cut_off_date]
        print("st")
        st = st \
            .groupby(st.dt_key.dt.floor(bucket_interval)) \
            .apply(lambda g: [{
                'ts': x[0],
                'steps': x[1]
                } for x in g.values.tolist()])
        st = st.reset_index()
        st.rename({0:"metrics","dt_key":"created_at"},inplace=True,axis=1)
        st['ended_at'] = st.created_at + pd.Timedelta(bucket_interval)
        st['device_id'] = device_id
        st = st[['device_id','created_at','ended_at','metrics']]
        steps.append(st)
        users.append(device_id)
    pd.concat(hrs).to_json('hrates.json',index=False,orient='records',date_unit='s')
    pd.concat(sleeps).to_json('sleeps.json',index=False,orient='records',date_unit='s')
    pd.concat(steps).to_json('stepps.json',index=False,orient='records',date_unit='s')
    return users

devices = process_records()
df=pd.read_excel("./pmdata/participant-overview.xlsx",skiprows=1)
df=  df[['Age', 'Height', 'Gender']]
df.columns = ['age','height','gender']
df['user_id'] = [uuid.uuid4().hex for x in participants]
df['created_at'] = datetime.date(2019, 11, 1)
df['devices'] = pd.Series([[x] for x in devices])
df['email'] = pd.Series([
    "sam.smith@gmail.com",
    "alfred.boo@gmail.com",
    "tsgcesar@apkdownloadbox.com",
    "kingjaguar@echotoc.com",
    "amortan@mlgmail.top",
    "davecr13@rackabzar.com",
    "hookone@henrikoffice.us",
    "xisven@micinemail.com",
    "smith@gmail.com",
    "ed.boo@gmail.com",
    "esar@apkdownloadbox.com",
    "jaguar@echotoc.com",
    "tan@mlgmail.top",
    "cr13@rackabzar.com",
    "one@henrikoffice.us",
    "en@micinemail.com",
])
df['dob'] = df.age.apply(lambda x: (dt.now() - pd.Timedelta(days=x*365.2425)).date())
df.to_json('users.json',index=False,orient='records',date_unit='s')
