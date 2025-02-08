'''
Version:
V0 serverReading basico funcionando

'''

import sys
import time
import requests;
import json;
import configparser
import paho.mqtt.publish as publish
import sched, threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from datetime import timedelta

time_token = {"time":datetime.fromisoformat('2025-02-07'),"token":"33425_8aa5cb10e7d0462b8d708a9e8eab6e51"}

''' Niveles de logging
Para obtener _TODO_ el detalle: level=logging.INFO
Para comprobar los posibles problemas level=logging.WARNINg
Para comprobar el funcionamiento: level=logging.DEBUG
'''
logging.basicConfig(
        level=logging.INFO,
        handlers=[RotatingFileHandler('./logs/log_datadis.log', maxBytes=1000000, backupCount=4)],
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')

def mqtt_tx(client,s_value):
    logging.info("__mqtt_tx")
    global mqtt_topic_prefix
    global mqtt_ip
    global mqtt_login
    global mqtt_password
    mqtt_auth = { 'username': mqtt_login, 'password': mqtt_password }
    publish.single(mqtt_topic_prefix + "/" + client, s_value, hostname=mqtt_ip, auth=mqtt_auth)
    logging.info( mqtt_ip + " -> " + mqtt_topic_prefix + "/"  + client + "  " + str(s_value))

def ask_for_key(): 
    global accesskey
    global appkey
    global sungrowDomain
    global time_token
    global password
    global account

    url_login = "https://"
    url_login += sungrowDomain
    url_login += "/login"

    headers = {
    "Content-Type": "application/json",
    "x-access-key": accesskey,
    "sys_code": "901"
    }

    data = {

        "appkey":appkey,
        "user_password":password,
        "user_account":account
    }
    logging.debug(headers)
    logging.debug(data)

    try:
        response = requests.post(url_login, json=data, headers=headers)

        logging.debug(str(response.status_code))
        logging.info(str(response)) 

        if response.status_code == 200:
            logging.debug(type(response.json()['result_data']['token']))
            time_token["time"] = datetime.now()
            time_token["token"] = response.json()['result_data']['token']


    except Exception as ex:
        logging.info ("ERROR: LA PETICION DE TOKEN NO HA SIDO CORRECTA");
        logging.info (ex)

    logging.debug("time:")
    logging.debug(type(time_token["time"]))
    logging.debug(time_token["time"])

    logging.debug("token:")
    logging.debug(type(time_token["token"]))
    logging.debug(time_token["token"])


def need_new_key():
    global time_token
    logging.debug("__need_new_key ?")
    logging.debug(str(time_token))
    # time_token = {"time":datetime.fromisoformat('2025-01-01'),"token":"firsttoken"}
    if datetime.now() > time_token["time"] + timedelta(days=1): 
        ask_for_key()
    

def serverReading(tm):    
    threading.Timer(tm, serverReading,args=[tm]).start()

    global accesskey 
    global appkey
    global sungrowDomain
    global ps_key_l
    global time_token

        
    logging.debug("_" * 2 + ' serverReading')

    need_new_key()

    urlReal = "https://"
    urlReal += sungrowDomain
    urlReal += "/getDeviceRealTimeData"

    headers = {
        "Content-Type": "application/json",
        "x-access-key": accesskey,
        "sys_code": "901"
    }

    data = {

        "appkey":appkey,
        "token": time_token["token"],

        "device_type":1,
        "point_id_list":[
            "88",    # Yield This Year
            "24"     # Total Active Power
        ],
        "ps_key_list":[
            ps_key_l
        ]
    }

    logging.debug(urlReal)
    logging.debug(str(headers))
    logging.debug(str(data))

    # La consulta al servidor
    response = requests.post(urlReal, json=data, headers=headers)
    logging.info(response.text)
    
    try:
        if response.status_code == 200:
            # 200 <class 'int'>
            logging.debug("response 200")
            if response.json()['result_code'] == "1":
                #['p88']['p24'] <class 'str'>
                s_client = response.json()['result_data']['device_point_list'][0]['device_point']['device_sn']
                s_energy = response.json()['result_data']['device_point_list'][0]['device_point']['p88']
                s_power = response.json()['result_data']['device_point_list'][0]['device_point']['p24']
                s_value = str({"power" : s_power,"energy" : s_energy})
                # print("power: ",s_power )
                # print("energy:  ", s_energy)
                mqtt_tx(s_client,s_value)

            elif response.json()['result_code'] == "E00003":
                logging.info("result_code E00003")
                ask_for_key()
            else:
                logging.info ("ERROR: result_code no contemplado")

    except Exception as ex:
        logging.info ("ERROR: LA EJECUCION NO HA TERMINADO CORRECTAMENTE")
        logging.info (ex)

def parser_sungrow():
    global accesskey 
    global appkey
    global sungrowDomain
    global password
    global account
    global ps_key_l
    global mqtt_topic_prefix
    global mqtt_ip
    global mqtt_login
    global mqtt_password


    logging.debug("__parser_sungrow")

    accesskey = parser.get('sungrow_server','s_accesskey')
    appkey = parser.get('sungrow_server','s_appkey')
    sungrowDomain = parser.get('sungrow_server','s_sungrowDomain')
    password = parser.get('sungrow_server','u_password')
    account = parser.get('sungrow_server','u_account')

    ps_key_l = parser.get('sungrow_inversor','s_ps_key_l')

    mqtt_topic_prefix = parser.get('mqtt_broker','mqtt_topic_prefix')
    mqtt_ip = parser.get('mqtt_broker','mqtt_ip')
    mqtt_login = parser.get('mqtt_broker','mqtt_login')
    mqtt_password = parser.get('mqtt_broker','mqtt_password')


parser = configparser.ConfigParser()
parser.read('config_sungrow_server.ini')
parser_sungrow()
need_new_key()
serverReading(300.0)


    
