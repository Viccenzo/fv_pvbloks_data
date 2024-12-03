version = 'v1.2'

import requests
import datetime
import sys
import getopt
import time
import os
import numpy as np
import pandas as pd
import json
import dotenv
import mqtt_db_service as service
from io import StringIO
from zoneinfo import ZoneInfo

#OutputFolder = ''
OutputFolder = ''
Day = ''
Hour = ''
ActiveDate = datetime.datetime.now()
Automatic = False
token = ''


EndOfLine = '\r\n'

def _url(path,ip):
    return "http://" + ip + '/v1' + path


def get_token():
    global pv_user
    global pv_password
    resp = requests.post(_url('/authentication/Login',ip), json={"username": pv_user, "password": pv_password})
    if resp.status_code != 200:
        # This means something went wrong.
        raise Exception('POST /authentication/Login {}'.format(resp.status_code))
    else:
        return resp.json()['bearer']


def get_api_version():
    resp = requests.get(_url('/info',ip))
    if resp.status_code != 200:
        # This means something went wrong.
        raise Exception('GET /info {}'.format(resp.status_code))
    else:
        return resp.json()['version']


def convert_datatime_to_query(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def get_last_meteo_data():
    global token

    endpoint = '/Meteo/last'
    resp = requests.get(_url(endpoint), headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(_url(endpoint), headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()


def get_sensor(id):
    global token
    endpoint = '/Calculation/GetSensor/'+str(id)
    resp = requests.get(_url(endpoint), headers={'Authorization': 'Bearer ' + token})
    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint), headers={'Authorization': 'Bearer ' + token})
    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()


def get_pvdevices(ip):
    global token
    endpoint = '/PvDevice'
    resp = requests.get(_url(endpoint,ip), headers={'Authorization': 'Bearer ' + token})
    if resp.status_code != 200:
        print("trying to get token")
        token = get_token()
        resp = requests.get(
            _url(endpoint,ip), headers={'Authorization': 'Bearer ' + token})
    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        pvdevices = resp.json()
        return pvdevices


def get_meteo_pvdevice_id():
    global token
    endpoint = '/Meteo/MeteoPvDeviceId'
    resp = requests.get(_url(endpoint), headers={'Authorization': 'Bearer ' + token})
    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint), headers={'Authorization': 'Bearer ' + token})
    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()


def get_measurement_data(f, u, pvdeviceId, addFullCurve,ip):
    global token

    data = {}
    curve = '?addFullCurve=false'
    if addFullCurve:
        curve = '?addFullCurve=true'

    data['loggerRequestBeginTime'] = datetime.datetime.now().isoformat()
    endpoint = '/CalibratedMeasurement/' + f + '/' + u + '/device/' + str(pvdeviceId) + curve + '&addCsvMetadataHeader=false'
    #print(endpoint)
    resp = requests.get(_url(endpoint,ip),  headers={'Authorization': 'Bearer ' + token})
    data['loggerRequestEndTime'] = datetime.datetime.now().isoformat()
    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint,ip),
            headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        data['report'] = "Request error"
        return data
    else:
        data['report'] = "Success"
        data['data_frame'] = resp.json()
        return data


def get_ivcurve_data(f, u, pvdeviceId):
    global token

    endpoint = '/CalibratedMeasurement/ivcurve/' + f + '/' + u + '/device/' + str(pvdeviceId)
    resp = requests.get(_url(endpoint),  headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint),
            headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()

def get_trigger_data(activatedate):
    global token

    start = '%d-%02d-%02d' % (activatedate.year, activatedate.month, activatedate.day)
    activatedate = activatedate + datetime.timedelta(days=1)
    stop = '%d-%02d-%02d' % (activatedate.year, activatedate.month, activatedate.day)

    endpoint = '/EventTrigger/irradiancereadings/csv?from=' + start + "&until=" + stop

    resp = requests.get(_url(endpoint),  headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint),
            headers={'Authorization': 'Bearer ' + token})



    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.text

def get_spectral_data(ip,f,u,limit):
    global token

    endpoint = '/Spectrometer/readings?limit=' + str(limit) + "&start=" + f + "&end=" + u
    resp = requests.get(_url(endpoint,ip),  headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint),
            headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()


def get_spectral_status(ip):
    global token

    endpoint = '/Spectrometer'
    resp = requests.get(_url(endpoint,ip),  headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint,ip),
            headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()

def spectral_data_exists(ip,f,u,limit):
    global token

    endpoint = '/Spectrometer/availablereadings?limit=' + str(limit) + "&start=" + f + "&end=" + u
    resp = requests.get(_url(endpoint,ip),  headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint),
            headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()
    
def get_spectral_device(ip):
    global token

    endpoint = '/Spectrometer/activespectrometers'
    resp = requests.get(_url(endpoint,ip),  headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        token = get_token()
        resp = requests.get(
            _url(endpoint,ip),
            headers={'Authorization': 'Bearer ' + token})

    if resp.status_code != 200:
        raise Exception('GET /' + endpoint + '{}'.format(resp.status_code))
    else:
        return resp.json()

def merge_sequences(input_list):
    output_list = []
    row = ''
    row_list = []
    for i in range(len(input_list) - 1):
        if row == '':
            row = input_list[i]
            row_list = input_list[i].split(',')
            continue
        next_row = input_list[i]
        next_row_list = next_row.split(',')
        if len(next_row_list) != len(row_list):
            row = ''
            continue
        if next_row_list[0] == row_list[0]:
            for x in range(len(row_list)):
                element = row_list[x]
                if element == '':
                    row_list[x] = next_row_list[x]
            continue
        output_list.append(','.join(row_list))
        row_list = next_row_list
    return output_list


def process_result(pvdev, data, remove_existing_file, extension):
    global EndOfLine
    global OutputFolder

    if len(data) < 13:
        return False

    result = data.split(EndOfLine)
    if len(result) < 2:
        if EndOfLine == '\r\n':
            result = data.split('\n')
            if len(result) > 2:
                EndOfLine = '\n'
            else:
                return False
        else:
            result = data.split('\r\n')
            if len(result) > 2:
                EndOfLine = '\r\n'
            else:
                return False

    header = result.pop(0)
    result = merge_sequences(result)
    filename = ActiveDate.strftime("%Y%m%d") + "_%s_%d%s.csv" % (pvdev['name'], pvdev['id'], extension)

    if remove_existing_file:
        if os.path.isfile(OutputFolder + filename):
            os.remove(OutputFolder + filename)

    if not os.path.isfile(OutputFolder + filename):
        with open(OutputFolder + filename, "w") as text_file:
            np.savetxt(text_file, [header], delimiter=" ", newline="\n", fmt="%s")

    with open(OutputFolder + filename, "a") as text_file:
        np.savetxt(text_file, result, delimiter=" ", newline="\n", fmt="%s")

    print('written: ' + OutputFolder + filename)


def store_spectral_csv(data):
    filename = ActiveDate.strftime("%Y%m%d") + "_spectrum.csv"
    if os.path.isfile(OutputFolder + filename):
        os.remove(OutputFolder + filename)

    with open(OutputFolder + filename, "w", encoding='utf-8') as text_file:
        text_file.write(data)

    print('written: ' + OutputFolder + filename)

def store_trigger_csv(data):
    filename = ActiveDate.strftime("%Y%m%d") + "_trigger.csv"
    if os.path.isfile(OutputFolder + filename):
        os.remove(OutputFolder + filename)

    with open(OutputFolder + filename, "w") as text_file:
        text_file.write(data)

    print('written: ' + OutputFolder + filename)


def usage():
    print('pvblocks_collect.py -d <yyyymmdd> -H <hour> -o <outputfolder> --auto -s')
    print('when auto is selected, all other parameters are ignored')
    print('all times are in the same timezone as set in the PVBlocks application')


def main(argv):
    global OutputFolder
    global Hour
    global Day
    global Automatic
    global ActiveDate
    global CurveOnly
    global Spectrometer
    global NoPvDevices
    global RepeatDay
    global Trigger

    CurveOnly = False
    found_day = False
    Spectrometer = False
    NoPvDevices = False
    RepeatDay = 0
    Trigger = False


    try:
        opts, args = getopt.getopt(argv, "hd:o:a:c:sptr:", ["day=", "ofolder=", "auto", "curve", "spectrometer", "pvdevskip", "trigger", "repeat="])
    except getopt.GetoptError:
        usage()
        sys.exit(1)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-d", "--day"):
            Day = arg
            found_day = True

        elif opt in ("-o", "--ofolder"):
            OutputFolder = arg + os.sep
        elif opt in ("-a", "--auto"):
            Automatic = True
        elif opt in ("-c", "--curve"):
            CurveOnly = True
        elif opt in ("-s", "--spectrometer"):
            Spectrometer = True
        elif opt in ("-p", "--pvdevskip"):
            NoPvDevices = True
        elif opt in ("-t", "--trigger"):
            Trigger = True
        elif opt in ("-r", "--repeat"):
            RepeatDay = int(arg)


    if not Automatic:
        if not found_day:
            print('The date of interest is required! For example:')
            print('For example: pvblocks_collect.py -d <20201124>')
            sys.exit(1)
        else:
            if len(Day) != 8:
                print('The date of interest is required! For example:')
                print('For example: pvblocks_collect.py -d <20201124>')
                sys.exit(1)
            else:
                year = int(Day[0:4])
                month = int(Day[4:6])
                day = int(Day[6:8])
                ActiveDate = datetime.datetime(year, month, day)


main(sys.argv[1:])
print('PVBlocks collect (%s)' % (version))
#print('API key: ' + APIkey)
#print('API Version: ' + get_api_version())

#pvdevices = get_pvdevices()
#print('Available modules: ')
#for name in [x['name'] for x in pvdevices]:
#    print(name)

#spectroradiometers_enabled = get_spectral_status(ip)['enabled']
#if spectroradiometers_enabled:
#    print('Spectrometer(s) found enabled')

## Server Service functions

def getDataloggerData(ip,table,queryStepTime,queryEndTime):
    try:
        url = f'http://{ip}/?command=dataquery&uri=dl:{table}&format=json&mode=date-range&p1={queryStepTime.strftime("%Y-%m-%dT%H:%M:%S")}&p2={queryEndTime.strftime("%Y-%m-%dT%H:%M:%S")}'
        print(url)
        data = {
            'report' : "Success",
            'loggerRequestBeginTime' : datetime.datetime.now().isoformat()
        }
        getData = requests.get(url,timeout=3) # Caso ocorra um timeout o que fazer? (fazer hoje)
        data['loggerRequestEndTime'] = datetime.datetime.now().isoformat()
        data_json = json.loads(getData.text)
        if data_json["data"] == []: #No data recevied continue search
            data['df_data'] = pd.DataFrame()
            data['report'] = "No data in packet"
            return data
        if "more" in data_json: # Datalogger did not return all asked data
            data['report'] = "missing query lines"
        df_data = pd.DataFrame(data_json['data'])
        cols = pd.DataFrame(data_json['head']['fields'])['name'].to_list()
        df_data[cols] = pd.DataFrame(df_data['vals'].to_list(), index=df_data.index)
        df_data = df_data.drop(columns=['vals'])
        df_data = df_data.rename(columns={'time': 'TIMESTAMP', 'no': 'RecNbr'})
        df_data['TIMESTAMP'] = pd.to_datetime(df_data['TIMESTAMP'])
        df_data = fixing(df_data)
        data['df_data'] = df_data
        data['loggerProcessingEndTime'] = datetime.datetime.now().isoformat()
        
    except requests.Timeout:
        print("Timeout occurred when trying to fetch data.")
        data['report'] = "Timeout occurred"
        return data

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        data['report'] = "Request error"
        return data

    except json.JSONDecodeError:
        print("Error decoding JSON response.")
        data['report'] = "Invalid JSON response"
        return data

    return data

def fixing(d_frame):
    names = []
    mydict = {'Datetime': 'TIMESTAMP'}
    for current, column in enumerate(d_frame):
        names.append(str(column))
        if names[current][0] == 'b':
            names[current] = names[current][1:]
            names[current] = names[current].strip("''")
            mydict[column] = names[current]
    d_frame.rename(columns=mydict, inplace=True)
    d_frame = d_frame.fillna(-9999)

    return d_frame

def getLoggerTabeNames(ip):
    #get datalogger tables
    
    #try to get data from DataTableInfo
    print("trying DataTabelInfo")
    url = f'http://{ip}/?command=dataquery&uri=dl:DataTableInfo.DataTableName&format=json&mode=most-recent'
    request_data = requests.get(url,timeout=3)
    #print(request_data.text)
    if ("Unrecognized request" in request_data.text):
        #try to get data from Status
        print("trying Status")
        url = f'http://{ip}/?command=dataquery&uri=dl:Status.DataTableName&format=json&mode=most-recent'
        request_data = requests.get(url,timeout=3)

    #print(request_data.text)
    value = json.loads(request_data.text)
    tables = value['data'][0]['vals']
    return tables

def getLoggerCurrentTime(ip):
    url = f'http://{ip}/v1/Info/status'    
    request_data = requests.get(url,timeout=3)
    value = json.loads(request_data.text)
    date_str = value["time"]
    date_str = date_str.split(".")[0]
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    queryEndTime = date_obj - datetime.timedelta(minutes=1)
    return queryEndTime

def healthCheck():
    # Código crítico
    with open('/tmp/heartbeat.txt', 'w') as f:
        f.write(str(time.time()))  # Escreve o timestamp

def get_table_name(device):
    table = f"{device['serial']}_{device['name']}"
    return table

dotenv.load_dotenv()
brokers = os.getenv("MQTT_BROKER").split(',')
pv_user = os.getenv("PV_USER")
pv_password = os.getenv("PV_PASSWORD")
groupName = os.getenv("SERVICE_NAME")
service.initDBService(user=os.getenv("USER"), service=groupName , server1=brokers[0], server2=brokers[1])
ips = os.getenv("IPS").split(',')
gmt_0 = ZoneInfo("UTC")

#fazer while de tempo
while  True:
    for ip in ips:
        datalogger = {"ip":ip,"tables":[]}
        print(" ")
        print(f'trying to reatch datalogger on ip: {ip}')
        try:
            devices = get_pvdevices(ip)
        except:
            # Error trying to get tables jump to next ip
            print("Error trying to get tables jump to next ip")
            continue
        queryEndTime = getLoggerCurrentTime(ip)
        
        # get spectro data
        spectral_status = get_spectral_status(ip)
        print(spectral_status)
        spectral_devices = get_spectral_device(ip)
        tablesNames = []
        for i, device in enumerate(spectral_devices):
            tableName = "shell_" + device["serial"] + "_" + device["sensorType"] + "_spectrometer"
            print(tableName)
            tablesNames.append(tableName)  # Adiciona o nome da tabela à lista

        if spectral_status["enabled"] == True:
            minTimestamp = None
            limit = 1
            for tableName in tablesNames: # busca o timestamp mais antigo de todos os dados de spectrometro
                lastTime = service.getLastTimestamp(tableName, groupName)
                if lastTime == "mqtt timeout":
                    print("getting table last timestamp timeout")
                    continue
                if lastTime == None: # table dont exist
                    print(f'This table: {tableName} dont exist on server. It will be created and Added to the next measurement loop') # Criar automáticamente no futuro?
                    continue
                
                # Atualiza o menor timestamp
                if minTimestamp is None or lastTime < minTimestamp:
                    minTimestamp = lastTime

            lastTime = minTimestamp
       
            queryStartTime = lastTime + datetime.timedelta(seconds=1)
            queryStepTime = queryStartTime
            while queryStepTime < queryEndTime:
                print(" ")
                print(f'Query start time: {queryStepTime}')
                print(f'Query end time: {queryEndTime}')
                #if timeoutCount >= 3: resolver no futuro
                #    print("timeout acheived max number of atempts")
                #    continue
                queryStepEnd = queryStepTime + datetime.timedelta(minutes=1)
                
                if queryStepEnd>queryEndTime: # na chamada mais recente utilizar o valor final do logger
                    queryStepEnd=queryEndTime

                data_exist = spectral_data_exists(ip,queryStepTime.strftime('%Y-%m-%d %H:%M:%S'),queryStepEnd.strftime('%Y-%m-%d %H:%M:%S'),limit)
                if not data_exist:
                    print("No data found, jumping to next attempt")
                    queryStepTime = queryStepEnd
                    continue
            
                data = {}
                data['loggerRequestBeginTime'] = datetime.datetime.now().isoformat()
                spectral_data = get_spectral_data(ip,queryStepTime.strftime('%Y-%m-%d %H:%M:%S'),queryStepEnd.strftime('%Y-%m-%d %H:%M:%S'),limit)
                data['loggerRequestEndTime'] = datetime.datetime.now().isoformat()
                #Spectral_data_JSON = json.loads(spectral_data[0]["readings"][0])
                #print(spectral_data[0]["readings"][1]["serial"])
                for device in spectral_data[0]["readings"]:
                    #Corrigindo o timezone
                    time_logger = device['timestamp']

                    # Truncar para 6 dígitos na fração de segundos
                    if "." in time_logger:
                        base, tz = time_logger.split("+")
                        time_part, fraction = base.split(".")
                        fraction = (fraction + "000000")[:6]
                        time_logger = f"{time_part}.{fraction}+{tz}"

                    parsed_datetime = datetime.datetime.fromisoformat(time_logger)
                    time_server = parsed_datetime.replace(tzinfo=gmt_0)
                    tableName = "shell_" + device["serial"] + "_" + device["sensorType"] + "_spectrometer"
                    #lastTime = service.getLastTimestamp(tableName,groupName)
                    spectral_pairs = [(pair['wavelength'], pair['reading']) for pair in device['spectralPairs']]
                    df = pd.DataFrame({
                        'TIMESTAMP': [time_server],
                        'spectralPairs': [spectral_pairs]
                    })
                    data['df_data'] = df
                    #print(data)
                    data['loggerProcessingEndTime'] = datetime.datetime.now().isoformat()
                    data["report"] = "Success"
                    
                    if data["report"] == "Success":
                        response = service.sendDF(data, tableName, groupName)
                        print(response)
                        if response == "mqtt timeout":
                            print("Sending data to mqtt timeout")
                            continue
                        #queryStepTime = queryStepEnd + datetime.timedelta(seconds=1)
                        
                        time.sleep(0.2)
                        healthCheck()
                        continue
                queryStepTime = queryStepEnd
                
        #get device data
        for device in devices:
            table = get_table_name(device)
            print(f"Table found: {table} with id {device['id']}")
            if "_OLD" in table:
                print("_OLD table, skipping")
                continue
            lastTime = service.getLastTimestamp(table,groupName) # está dando crash caso o servidor não tenha a tabela
            if lastTime == "mqtt timeout":
                print("getting table last timestamp timeout")
                continue
            if lastTime == None: # table dont exist
                print(f'This table: {table} dont exist on server. It will be created and Added to the next measurement loop') # Criar automáticamente no futuro?
                continue
            #print(lastTime)
            queryStartTime = lastTime + datetime.timedelta(seconds=1)
            #queryStartTime = datetime.datetime.strptime("2024-08-10T00:00:00", '%Y-%m-%dT%H:%M:%S') #Para recuperar dados
            queryStepTime = queryStartTime

            #df_data = pd.DataFrame()
            timeoutCount = 0
            while queryStepTime < queryEndTime:
                print(" ")
                print(f'Query start time: {queryStepTime}')
                print(f'Query end time: {queryEndTime}')
                if timeoutCount >= 3:
                    print("timeout acheived max number of atempts")
                    continue
                queryStepEnd = queryStepTime + datetime.timedelta(minutes=5)
                
                if queryStepEnd>queryEndTime: # na chamada mais recente utilizar o valor final do logger
                    queryStepEnd=queryEndTime
                
                data = get_measurement_data(queryStepTime.strftime('%Y-%m-%d %H:%M:%S'), queryStepEnd.strftime('%Y-%m-%d %H:%M:%S'), device['id'], True, ip)


                # Ler os dados do campo 'data_frame' usando StringIO para interpretar como CSV
                df = pd.read_csv(StringIO(data['data_frame']))

                # Renomear a coluna 'timestamp' para 'TIMESTAMP'
                df.rename(columns={'timestamp': 'TIMESTAMP'}, inplace=True)

                # Imprimir os headers (nomes das colunas)
                print("Headers:", list(df.columns))
                # Imprimir a quantidade de linhas e colunas
                print("Linhas e colunas:", df.shape)  # df.shape retorna uma tupla (n_linhas, n_colunas)

                if data["report"] == "Request error":
                    continue

                #if data["report"] == "Invalid JSON response":
                #    continue

                #if data["report"] == "Timeout occurred":
                #    timeoutCount += 1
                #    continue

                if df.empty: # caso o logger nao tenha retornado data !!!(verificar essa implementação)!!!
                    queryStepTime = queryStepEnd + datetime.timedelta(seconds=1)
                    print("empty data")
                    healthCheck()
                    break

                #if data["report"] == "missing query lines": # caso o logger tenha enviado menos dados do que solicitado
                #    #print(data)
                #    lastLoggerTimestamp = data["df_data"].iloc[-1]["TIMESTAMP"].to_pydatetime()
                #    response = service.sendDF(data,table,groupName)
                #    print(response)
                #    if response == "mqtt timeout":
                #        print("Sending data to mqtt timeout")
                #        continue
                #    queryStepTime = lastLoggerTimestamp + datetime.timedelta(seconds=1) #if datalogger did not send all data make new query follow the last sended data
                #    time.sleep(0.2)
                #    healthCheck()
                #    continue
                data['df_data'] = df
                #print(data)
                data['loggerProcessingEndTime'] = datetime.datetime.now().isoformat()
                if data["report"] == "Success":
                    response = service.sendDF(data, table, groupName)
                    print(response)
                    if response == "mqtt timeout":
                        print("Sending data to mqtt timeout")
                        continue
                    queryStepTime = queryStepEnd + datetime.timedelta(seconds=1)
                    time.sleep(0.2)
                    healthCheck()
                    continue

                print(f'unknown error ocurred: {data["report"]}')

    print("waiting 15 min to get new measurements")
    time.sleep(900)

### use this to update the code above


data['loggerRequestBeginTime'] : datetime.datetime.now().isoformat()
data['loggerRequestEndTime'] = datetime.datetime.now().isoformat()
data['loggerProcessingEndTime'] = datetime.datetime.now().isoformat()

while True:
    if Automatic:
        ActiveDate = datetime.datetime.now()
        Spectrometer = spectroradiometers_enabled

    f = '%d-%02d-%02dT%02d:%02d' % (ActiveDate.year, ActiveDate.month, ActiveDate.day, 0, 0)
    u = '%d-%02d-%02dT%02d:%02d' % (ActiveDate.year, ActiveDate.month, ActiveDate.day, 23, 59)
    d = '%d-%02d-%02d' % (ActiveDate.year, ActiveDate.month, ActiveDate.day)

    if not NoPvDevices:
        for pvdev in pvdevices:
            data = get_measurement_data(f, u, pvdev['id'], False)
            process_result(pvdev, data, True, "")
            data = get_ivcurve_data(f, u, pvdev['id'])
            process_result(pvdev, data, True, "_IVCURVE")

    if Spectrometer:
        spectralData = get_spectral_data(d)
        if spectralData.count('\n') > 1:
            store_spectral_csv(spectralData)

    if Trigger:
        triggerData = get_trigger_data(ActiveDate)
        if triggerData.count('\n') > 1:
            store_trigger_csv(triggerData)

    if RepeatDay > 0:
        ActiveDate = ActiveDate + datetime.timedelta(days=1)
        RepeatDay = RepeatDay - 1
        continue
    if not Automatic:
        break

    print('Waiting for next hour...')

    currentHour = datetime.datetime.now().hour
    while currentHour == datetime.datetime.now().hour:
        time.sleep(60)

    print('Another hour passed, retrieving data at  ' + datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

### old Code for check only

"""

data = {}

def convert_to_numeric(df):
    for column in df.columns:
        if column != 'TIMESTAMP':  # Ignorar a coluna TIMESTAMP
            try:
                # Tenta converter a coluna para numérico
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                # Se houver erro, mantém a coluna como está
                pass

# Function to map types from pandas to SQLalchey
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float 
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    else:
        return VARCHAR

def check_for_changes(old_df, new_df):
    return not old_df.equals(new_df)

def table_check(ip,lines):
    
    # Removing the first line that contains the ESN
    lines = lines[1:]

    # Processing the data to separate by equipment
    current_equipment_name = None

    tables = []

    for line in lines:
        if line.startswith('#INV'):
            # Capture the name of the new equipment (inverter)
            current_equipment_name = line.strip().split(':')[1]
            #print(f"New equipment detected: {current_equipment_name}")
            tables.append(current_equipment_name)
    
    return tables

# Função auxiliar para converter o formato de timestamp
def parse_time(value):
    try:
        # Verifica o comprimento da string para decidir o formato
        if len(value) == 19:  # Formato: '2024-11-07 11:34:00'
            return pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S')
        elif len(value) == 17:  # Formato: '24-11-07 09:18:00'
            return pd.to_datetime(value, format='%y-%m-%d %H:%M:%S')
    except ValueError:
        print("TIMESTAMP format not supported")
        return pd.NaT

def healthCheck():
    # Código crítico
    with open('/tmp/heartbeat.txt', 'w') as f:
        f.write(str(t.time()))  # Escreve o timestamp

# Coisas adicionadas para mandar pro servidor do Lucas
#engine2 = create_engine(f'postgresql://fotovoltaica:TSAL6ujJn8pD7Nq@150.162.142.79/fotovoltaica', echo=False) # tirar depois

dotenv.load_dotenv()
brokers = os.getenv("MQTT_BROKER").split(',')
service.initDBService(user=os.getenv("USER"), server1=brokers[0], server2=brokers[1])
ips = os.getenv("IPS").split(',')

def main():
    global data

    print("start")

    while(1):
        for ip in ips:
            
            print(f'Checking logger on ip: {ip}')
            # read current csv
            folder_terminator = ip.split(".")[3]
            today = datetime.now().date()
            today = datetime.combine(today, time(23, 59, 59))
            print(today)
            try:
                file_path = f'../ftp/data/HW{folder_terminator}/min{today.strftime("%Y%m%d")}.csv'
                print(file_path)
                with open(file_path, 'r') as file:
                    lines = file.readlines()
            except:
                print("Newest File not found")
                continue

            #Utilizar a verificação se o arquivo mudou, caso não tenha mudado não tem pq fazer o processo (futuro)
            #tomorrow = today + timedelta(days=1)
            tables = table_check(ip,lines)
            print(f'tables found: {tables}')
            
            ###
            for table in tables:
                #check for the most recent stored data timestamp
                server_timestamp = service.getLastTimestamp(table)
                
                if server_timestamp is None:
                    server_timestamp = today - timedelta(days=30)
                
                #print(f'server_timestap_get: {server_timestamp}')
                # try to read file from server tome to today
                while today>=server_timestamp:
                    #print(server_timestamp)
                    #print(f'../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv')
                    try:
                        file_path = f'../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv'
                        with open(file_path, 'r') as file:
                            lines = file.readlines()
                    except:
                        print("file not found on this date, jumping to next day")
                        server_timestamp += timedelta(days=1)
                        continue
                    
                    # finding on CSV file the correct table chunk
                    init_found = 0
                    for index,line in enumerate(lines):
                        if table in line:
                            line_init = index + 1
                            logger_name = line.strip().split(':')[1]
                            init_found = 1
                            #print(f'line_init_{line_init}: {lines[line_init]}')
                        elif init_found and "#" in line and "#Time" not in line: # IQ teste here to test your logic
                            line_end = index -1
                            #print(f'line_end_{line_end}: {lines[line_end]}')
                            break
                        elif index == len(lines)-1:
                            line_end = index
                            #print(f'line_end_{line_end}: {lines[line_end]}')
                            break
                    #print(lines[line_init:line_end])
                    
                    #extrai o chunck de dados referente a um data logger
                    try:
                        data_chunk=lines[line_init:line_end]
                    except:
                        print(f'missing {table} on file: ../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv')
                        server_timestamp += timedelta(days=1)
                        continue
                    #extai os headers
                    headers = data_chunk[0].strip("#").strip().split(";")
                    # Remover o caractere de nova linha e separador extra (se houver) nas linhas de 
                    data_rows = [line.strip().rstrip(";").split(";") for line in data_chunk[1:]]
                    # Criar o DataFrame
                    df = pd.DataFrame(data_rows, columns=headers)

                    # Aplica a função de conversão na coluna 'Time'
                    df['Time'] = df['Time'].apply(parse_time)
                    # Renomeando a coluna "Time" para "TIMESTAMP"
                    df.rename(columns={'Time': 'TIMESTAMP'}, inplace=True)
                    #print(df)

                    # Converting datatype
                    convert_to_numeric(df)
                    #column_types = {name: map_dtype(dtype) for name, dtype in df.dtypes.items()}

                    print(logger_name)
                    print(f'../ftp/data/HW{folder_terminator}/min{server_timestamp.strftime("%Y%m%d")}.csv')
                    print(f'today : {today}')
                    print(f'server_timestamp : {server_timestamp}')

                    #print("uploading data to database")
                    data = {}
                    data["df_data"] = df
                    data['loggerRequestBeginTime'] = datetime.now().isoformat()
                    data['loggerRequestEndTime'] = data['loggerRequestBeginTime']
                    data['report'] = "Success"
                    if data["report"] == "Success":
                        response = service.sendDF(data, table=logger_name)
                        print(response)
                        if response == "mqtt timeout":
                            print("Sending data to mqtt timeout")
                        t.sleep(0.2)
                        healthCheck()
                    
                    print(" ")
                    server_timestamp += timedelta(days=1)

                ######

        print("Waiting 900s")
        t.sleep(900)  

main()

"""