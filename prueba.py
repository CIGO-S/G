# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 08:53:40 2023

@author: SSSILVA
"""

from __future__ import print_function

from datetime import timedelta
import os.path
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
pd.options.mode.chained_assignment = None
SCOPES = ["https://www.googleapis.com/auth/gmail.send",'https://www.googleapis.com/auth/userinfo.profile','https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']


real_file_id='1OnXm_pgM-oBPWd_TAUmbaF5v_wShgMzyaD1KcGyvTgQ'
creds = None
if os.path.exists('token.json'):
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
   
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.json', 'w') as token:
        token.write(creds.to_json())

try:

    gc = gspread.authorize(creds)
    gs = gc.open_by_key(real_file_id)
    worksheet1 = gs.worksheet('Hoja 1')
    #df = pd.read_excel("Historico_Corresponsalia.xlsx")
    #df["FECHA"]=df['FECHA'].dt.strftime('%Y-%m-%d %H:%M:%S')
    #gs.values_append('paso', {'valueInputOption': 'RAW'}, {'values': df.values.tolist()})
    df1 = pd.DataFrame(worksheet1.get_all_records())
    df1["FECHA"]=pd.to_datetime(df1["FECHA"])

    fecha_min=df1["FECHA"].min()
    fecha_max=df1["FECHA"].max()
    a=pd.DataFrame({"FECHA":[fecha_min + timedelta(x) for x in range(int((fecha_max-fecha_min).days)+1)]})
    data_completa=pd.DataFrame()
    
    

    
    data_aux=pd.DataFrame(columns=list(df1.columns))
    for i in list(a["FECHA"]):
        if len(df1[df1["FECHA"]==i])!=0:
            data_completa=pd.concat([data_completa,df1[df1["FECHA"]==i]],ignore_index=True)
        else:
            print("hola")
            data_aux=df1[df1["FECHA"]==i]
            d=0
            while len(data_aux)==0:
                d+=1
                data_aux=df1[df1["FECHA"]==i+timedelta(days=d)]
            
            data_aux["FECHA"]=i
            data_completa=pd.concat([data_completa,data_aux],ignore_index=True)
    data_completa["FECHA"]=data_completa["FECHA"].apply(lambda x: x.strftime('%Y-%m-%d'))
    data_completa["IDENTIFICACION"]=data_completa["IDENTIFICACION"].apply(lambda x: str(x)[:-2])
    set_with_dataframe(worksheet=worksheet1, dataframe=data_completa, include_index=False,include_column_header=True)
except HttpError as error:
    print(F'An error occurred: {error}')
        

