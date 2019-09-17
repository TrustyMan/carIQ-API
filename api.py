from flask import Flask, request, jsonify, make_response
import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import math

import requests
from datetime import date

p_tunnel = 'ec2-54-148-131-210.us-west-2.compute.amazonaws.com'
p_ssh_user = 'ubuntu'
p_remote_bind_address = 'localhost'

s_tunnel = 'ec2-18-216-239-150.us-east-2.compute.amazonaws.com'
s_ssh_user = 'ubuntu'
s_remote_bind_address = 'cariq-staging.cu8ddrwc5trn.us-east-2.rds.amazonaws.com'

p_db = 'cariq'
p_db_user = 'cariq'
p_db_password = 'didyouknowcariqiscool'
p_db_host = 'localhost'

s_db = 'cariq_staging'
s_db_user = 'cariq'
s_db_password = 'RcRikcj:syWLjwM!hz4Vnez.G-hFp3Mu'
s_db_host = 'localhost'

ACCESS_TOKEN = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjYXJpcSIsImV4cCI6MTg4MzM2NzgzNCwiaWF0IjoxNTY3NzkxODQ1LCJpc3MiOiJjYXJpcSIsImp0aSI6ImMxNWM3ZTE1LWFiNzAtNDg5Yi1iYjY3LTM5YjRmNWI2MGZmMyIsIm5iZiI6MTU2Nzc5MTg0NCwic3ViIjoiMSIsInR5cCI6ImFjY2VzcyJ9.ncCc1cWNEcY5-0C--4uaOaxRLy2t2gVmPUj-IfDaRUb07J4Tu0mVpqdSymCfXCW6qBZSnWNihRvpcld7q7xexA"

# database_option = 'Production'
database_option = 'Staging'

global_warning_count = 0
reject_flag = 0

# IMEI_VIN Approach Constants
LOCAL_RISK_COUNTER = 0
IMEI_PHASE1_MIN_RECORD_THRESHOLD = 18
IMEI_PHASE1_RISK_THRESHOLD = 4
IMEI_PHASE2_RISK_THRESHOLD = 2
IMEI_PHASE2_DEFAULT_DEDUCTION = 0
IMEI_SCORE = 10000
IMEI_VIN_SCORE_WEIGHT = 1
IMEI_VIN_INITIAL_SCORE = 10000

# Fuel Level Approach Constants
FUEL_LEVEL_SCORE = 0
REASONABLE_MAX_MPH = 100
VARIANCE_FACTOR = .10
FUEL_SCORE_WEIGHT = 0.4
FUEL_LEVEL_INITIAL_SCORE = 0

# Battery Voltage Approach Constants
BATTERY_VOLTAGE_SCORE = 10000
ON_COUNT = 1
ON_INDEX = 1
ON_RECORDS_TO_SKIP = 12
ON_VOLTAGE_SUM = 0
OFF_INDEX = 1
OFF_COUNT = 1
OFF_VOLTAGE_SUM = 0
VOLTAGE_RISK_THRESHOLD = 30
BATTERY_VOLTAGE_SCORE_WEIGHT = 0.6
BATTERY_VOLTAGE_INITIAL_SCORE = 0

# Odometer Approach Constants
ODOMETER_SCORE_WEIGHT = 0.8
ODOMETER_INITIAL_SCORE = 0

# GPS Approach Constants
GPS_SCORE = 10000
NULL_COUNTER_THRESHOLD = 100
GPS_SCORE_WEIGHT = 0.7
GPS_INITIAL_SCORE = 0

app = Flask(__name__)

tunnel = None

def connectDatabase():
    global database_option
    global tunnel
    
    if database_option == 'Production':
        connection = psycopg2.connect(
            database=p_db,
            user=p_db_user,
            password=p_db_password,
            host=p_db_host,
            port=tunnel.local_bind_port
        )    
        return connection

    connection = psycopg2.connect(
        database=s_db,
        user=s_db_user,
        password=s_db_password,
        host=s_db_host,
        port=tunnel.local_bind_port
    )
    
    return connection


def getVehicles():
    """ query devices from the vehicles table """
    conn = None
    try:
        # Create a database connection
        conn = connectDatabase()
        
        query = "select vin from vehicles where vin != ''"
        devices = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    
    return devices

def get20DayData(vin):
    try:
        conn = connectDatabase()        
        query = "select distinct on (cust_table.cust_time) * from (select id, imei, vin, date_trunc('day',dml_timestamp) as cust_time from device_audits WHERE vin='" + vin + "' AND dml_timestamp between now() -interval '19 days' and now() order by dml_timestamp DESC) as cust_table order by cust_time DESC"
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return data

def get50LastData(vin):
    try:
        conn = connectDatabase()        
        query = "select id, imei, vin, dml_timestamp from device_audits WHERE vin='" + vin + "' order by dml_timestamp DESC limit 50"
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return data

def calcIMEIVINScorePhase1(data):
    global IMEI_PHASE1_MIN_RECORD_THRESHOLD
    global IMEI_SCORE
    global global_warning_count
    global LOCAL_RISK_COUNTER
    global reject_flag
    
    score = IMEI_SCORE
    if len(data) <= IMEI_PHASE1_MIN_RECORD_THRESHOLD:
        score = score - 1000
    
    old_imei = data.iloc[0]["imei"]
    old_vin = data.iloc[0]["vin"]
    for i in range(1, len(data)-1):
        if data.iloc[i]["imei"] != old_imei or data.iloc[i]["vin"] != old_vin:
            LOCAL_RISK_COUNTER = LOCAL_RISK_COUNTER + 1
            score = score - 100
            
        if LOCAL_RISK_COUNTER > IMEI_PHASE1_RISK_THRESHOLD:
            global_warning_count = global_warning_count + 1
            LOCAL_RISK_COUNTER = 0
            
        old_imei = data.iloc[i]["imei"]
        old_vin = data.iloc[i]["vin"]

    return score

def calcIMEIVINScorePhase2(data, score):
    global LOCAL_RISK_COUNTER
    global IMEI_PHASE2_RISK_THRESHOLD
    global IMEI_PHASE2_DEFAULT_DEDUCTION
    global reject_flag
    
    old_imei = data.iloc[0]["imei"]
    old_vin = data.iloc[0]["vin"]

    for i in range(1, len(data)):
        if data.iloc[i]["imei"] != old_imei or data.iloc[i]["vin"] != old_vin:
            score = score - 200
            IMEI_PHASE2_DEFAULT_DEDUCTION = 100
            LOCAL_RISK_COUNTER = LOCAL_RISK_COUNTER + 1
        
        if LOCAL_RISK_COUNTER > IMEI_PHASE2_RISK_THRESHOLD:
            global_warning_count = 0
            global_warning_count = global_warning_count + 1
            LOCAL_RISK_COUNTER = 0

    return score

def getFuelLevelData(vin):
    conn = None
    try:
        conn = connectDatabase()
        query = "SELECT fuel_level, odometer, dml_timestamp from vehicle_state_audits where ignition_state = true and vin = '" + vin + "' ORDER BY timestamp DESC LIMIT 10001"        
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return data

def calcFuelScore(data):
    global global_warning_count
    global FUEL_LEVEL_INITIAL_SCORE
    global REASONABLE_MAX_MPH
    global VARIANCE_FACTOR 

    score = FUEL_LEVEL_INITIAL_SCORE
    current_FL = data.iloc[0]['fuel_level']
    current_RO = data.iloc[0]['odometer']
    current_RT = data.iloc[0]['timestamp']

    for i in range(10, len(data.index) + 1, 10):
        previous_FL = data.iloc[i]['fuel_level']
        previous_RO = data.iloc[i]['odometer']
        previous_RT = data.iloc[i]['timestamp']

        fuel_Used = previous_FL - current_FL
        distance_Travelled = current_RO - previous_RO
        time_Travelled = current_RT - previous_RT
        estimated_Mph = distance_Travelled / time_Travelled
        estimated_Fuel_Per_Hour = fuel_Used * time_Travelled

        if fuel_Used < 0:
            global_warning_count += 1
        elif fuel_Used == 0:
            score += 3
        else:
            score += 8
            
    return score

def get15DayData(vin):    
    try:
        conn = connectDatabase()      
        query = "select battery_level, dml_timestamp, ignition_state from vehicle_state_audits where vin='" + vin + "' and dml_timestamp between now()-interval '15 days' and now()-interval '1 days' order by dml_timestamp desc"
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return data

def get12HourData(vin):    
    try:
        conn = connectDatabase()      
        query = "select battery_level, dml_timestamp, ignition_state from vehicle_state_audits where vin='" + vin + "' and dml_timestamp between now()-interval '12 hours' and now() order by dml_timestamp desc"
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close() 
    
    return data

def calcSum(data):
    global LOCAL_RISK_COUNTER
    global ON_COUNT
    global ON_INDEX
    global ON_RECORDS_TO_SKIP
    global ON_VOLTAGE_SUM
    global OFF_INDEX
    global OFF_COUNT
    global OFF_VOLTAGE_SUM
    global VOLTAGE_RISK_THRESHOLD
    global global_warning_count
   
    sum_array = []
    
    for i in range(1, len(data)):
        if data.iloc[i]["battery_level"] == "" or data.iloc[i]["ignition_state"] == "":
            LOCAL_RISK_COUNTER = LOCAL_RISK_COUNTER + 1
            ON_INDEX = 1
            if LOCAL_RISK_COUNTER > VOLTAGE_RISK_THRESHOLD:
                global_warning_count = global_warning_count + 1
                LOCAL_RISK_COUNTER = 0
            else:
                continue
                
        if data.iloc[i]["ignition_state"]:
            ON_COUNT = ON_COUNT + 1
            ON_INDEX = ON_INDEX + 1
            OFF_INDEX = 0
            if ON_INDEX < ON_RECORDS_TO_SKIP:
                continue
            else:
                if ON_INDEX<=2:
                    sum_array = []
                sum_array.append(data.iloc[i]["battery_level"])
        else:
            if len(sum_array) <= 12:
                sum_array = []
            ON_INDEX = 1
            if OFF_INDEX == 1:
                OFF_VOLTAGE_SUM = OFF_VOLTAGE_SUM + data.iloc[i]["battery_level"]
                OFF_INDEX = OFF_INDEX + 1
                OFF_COUNT = OFF_COUNT + 1
    
    if len(sum_array) >= 12:
        for i in range(0, len(sum_array)):
            ON_VOLTAGE_SUM = ON_VOLTAGE_SUM + sum_array[i]
    else:
        ON_VOLTAGE_SUM = 0
    
    return
    
def calcBatteryScore(day15_data, hour12_data):
    calcSum(day15_data)
    day15_On_Voltage_Avg = ON_VOLTAGE_SUM / ON_COUNT
    day15_Off_Voltage_Avg = OFF_VOLTAGE_SUM / OFF_COUNT

    calcSum(hour12_data)
    hour12_On_Voltage_Avg = ON_VOLTAGE_SUM / ON_COUNT
    hour12_Off_Voltage_Avg = OFF_VOLTAGE_SUM / OFF_COUNT

    on_Diff = abs(day15_On_Voltage_Avg - hour12_On_Voltage_Avg)
    off_Diff = abs(day15_Off_Voltage_Avg - hour12_Off_Voltage_Avg)

    score = BATTERY_VOLTAGE_SCORE - calcPenalty(on_Diff) - calcPenalty(off_Diff)

    return score

def calcPenalty(diff):
    penalty = 0
    if diff > 1:
        penalty = 0
    elif diff > 0.5:
        penalty = 0
    elif diff > 0.3:
        penalty = 4500
    elif diff > 0.25:
        penalty = 3000
    elif diff > 0.2:
        penalty = 1500
    elif diff > 0.15:
        penalty = 1000
    elif diff > 0.1:
        penalty = 500
    elif diff > 0.05:
        penalty = 200
    else:
        penalty = 0
    return penalty
    
def getOdometerData(vin):
    conn = None
    try:
        conn = connectDatabase()      
        query = "SELECT fuel_level, odometer, dml_timestamp from vehicle_state_audits where vin = '" + vin + "' ORDER BY timestamp DESC LIMIT 10001"        
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return data

def calcOdometerScore(data):
    global global_warning_count
    global REASONABLE_MAX_MPH
    global VARIANCE_FACTOR
    score =  ODOMETER_INITIAL_SCORE   

    current_FL = data.iloc[0]['fuel_level']
    current_RO = data.iloc[0]['odometer']
    current_RT = data.iloc[0]['timestamp']

    for i in range(10, len(data.index) + 1, 10):
        if current_RO == None:
            continue
        else:
            score += 2
        previous_FL = data.iloc[i]['fuel_level']
        previous_RO = data.iloc[i]['odometer']
        previous_RT = data.iloc[i]['timestamp']

        fuel_Used = previous_FL - current_FL
        distance_Travelled = current_RO - previous_RO
        time_Travelled = current_RT - previous_RT
        estimated_Mph = distance_Travelled / time_Travelled        

        if distance_Travelled < 0:
            global_warning_count += 1
        elif estimated_Mph <= REASONABLE_MAX_MPH:
            score += 8
        else:
            global_warning_count += 1
            
    return score

def getGPSData(vin):
    conn = None
    try:
        conn = connectDatabase()       
        query = "SELECT latitude, longitude, altitude, ignition_state, dml_timestamp FROM vehicle_state_audits WHERE vin = '" + vin + "' ORDER BY id LIMIT 10000"        
        data = pd.read_sql_query(query, conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return data

def calcGPSNullScore(data):
    global GPS_SCORE
    global NULL_COUNTER_THRESHOLD
    global global_warning_count
    
    local_risk_count = 0
    score = GPS_SCORE

    for i in data.index:
        lat = data.iloc[i]['latitude']
        lon = data.iloc[i]['longitude']
        alt = data.iloc[i]['altitude']
        
        if math.isnan(lat) or math.isnan(lon) or math.isnan(alt):
            score = score -2
            local_risk_count = local_risk_count + 1
        
        if local_risk_count > NULL_COUNTER_THRESHOLD:
            global_Warning_Count = global_Warning_Count + 1
            local_risk_count = 0
            
    return score

@app.route('/test', methods=['GET'])
def test():
	vin_data = request.get_json()
	return jsonify({'result': vin_data['vin']})

@app.route('/score', methods=['GET'])
def get_score():
    global tunnel
    if database_option == 'Production':
        tunnel = SSHTunnelForwarder(
            (p_tunnel, 22),
            ssh_username=p_ssh_user,
            ssh_private_key='vladan_open',
            remote_bind_address=(p_remote_bind_address, 5432),
            local_bind_address=('localhost', 5432)
        )
    else:
        tunnel = SSHTunnelForwarder(
            (s_tunnel, 22),
            ssh_username=s_ssh_user,
            ssh_private_key='vladan_open',
            remote_bind_address=(s_remote_bind_address, 5432),
            local_bind_address=('localhost', 5432)
        )

    # Start the tunnel
    tunnel.start()
    
    try:
        devices = getVehicles()
        vin_data = request.get_json()
        flag = 0

        if not vin_data["vin"]:
        	return jsonify({'result' : 'Unvalid'})

        for i in range(0, len(devices)):
        	if(vin_data["vin"] == devices.iloc[i]["vin"]):
        		flag = 1

        if not flag:
        	return jsonify({'result': "Unknown Vin"})

        results = []
        results.append(['vin', 'overall', 'imei_vin_score', 'fuel_level_score', 'battery_voltage_score', 'odometer_score', 'gps_score'])
        
        imei_vin_score = 10000
        fuel_score = 0
        battery_score = 0
        odometer_score = 0
        gps_null_score = 10000

        vin = vin_data["vin"]
        print(vin)
        
        # print("IMEI_VIN Score")
        try:
            imei_vin_data = get20DayData(vin)  

            if len(imei_vin_data.index) > 0:
                print("20 Days IMEI_VIN Data")
                print(imei_vin_data)
                imei_vin_score = calcIMEIVINScorePhase1(imei_vin_data)
                imei_vin_data = get50LastData(vin)

                if len(imei_vin_data.index) > 0:
                    imei_vin_score = calcIMEIVINScorePhase2(imei_vin_data, imei_vin_score)
                    print("Last 50 Days IMEI_VIN Data")
                    print(imei_vin_data)                        
                else:
                    print("No 50 Last Day IMEI_VIN data")

            else:
                print("No 20 Day IMEI_VIN data")
        except Exception as error:
            print(error)
        
        try:        
            fuel_data = getFuelLevelData(vin)   

            if len(fuel_data.index) > 0:
                fuel_score = calcFuelScore(fuel_data)
                print("Fuel Level Data")
                print(fuel_data)                    
            else:
                print("No fuel level data")
        except Exception as error:
            print(error)

        try:
            day15_battery_data = get15DayData(vin)
            hour12_battery_data = get12HourData(vin)

            if len(day15_battery_data.index) > 0 and len(hour12_battery_data.index) > 0:
                battery_score = calcBatteryScore(day15_battery_data, hour12_battery_data)
                print("15 Days Battery Data")
                print(day15_battery_data)
                print("12 Hours Battery Data")
                print(hour12_battery_data)                    
            else:
                print("No battery voltage data")
        except Exception as error:
            print(error)

        try:
            odometer_data = getOdometerData(vin)

            if len(odometer_data.index) > 0:
                odometer_score = calcOdometerScore(odometer_data)
                print("Odometer Data")
                print(odometer_data)                    
            else:
                print("No odometer data")
        except Exception as error:
            print(error)

        try:
            gps_null_data = getGPSData(vin)

            if len(gps_null_data.index) > 0:
                gps_null_score = calcGPSNullScore(gps_null_data)
                print("GPS Data")
                print(gps_null_data)                    
            else:
                print("No GPS data")
        except Exception as error:
            print(error)
        
        overall = 10000
        try:
            overall = imei_vin_score * IMEI_VIN_SCORE_WEIGHT + fuel_score * FUEL_SCORE_WEIGHT + battery_score * BATTERY_VOLTAGE_SCORE_WEIGHT + odometer_score * ODOMETER_SCORE_WEIGHT + gps_null_score * GPS_SCORE_WEIGHT
            results.append([vin, overall, imei_vin_score, fuel_score, battery_score, odometer_score, gps_null_score])
            tunnel.stop()
            return jsonify({'result': results})
        except Exception as error:
        	tunnel.stop()
        	return jsonify({'result': "error"})

        print("Global warning count: " + str(global_warning_count))
    except Exception as error:
    	tunnel.stop()
    	return jsonify({'result': "error"})

if __name__ == '__main__':
	app.run(debug=True)