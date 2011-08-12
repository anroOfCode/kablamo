from pymongo import Connection
from pymongo.objectid import ObjectId
import datetime
import time

conn = Connection()
db = conn.data
acTk = None

def prepareDatabase():
    db.devices.remove()
    device = {"name": "Device 1", "serial": "12345A"}
    devKey1 = db.devices.insert(device)
    device['name'] = "Device 2"
    devKey2 = db.devices.insert(device)

    db.sensors.remove()
    sensor = {"measure": "Atmospheric CO2", "units":"ppm", "scaling": 1}
    sensorKey1 = db.sensors.insert(sensor)

    db.accessKeys.remove()
    accessKey = {"em": "andrew.robinson@gmail.com",
             "d": [devKey1, devKey2],
             "s": [sensorKey1],
             "e": True}
    acTk = db.accessKeys.insert(accessKey)
    db.data.remove()
    db.create_collection("stream", {"capped":True, "size":100})
    print ObjectId(acTk)


def testInsert():
    timeStampValue = int(round(time.time()*1000))
    sD = {"sensorId": 0, "deviceId": 1, "timeStamp": timeStampValue, "value": 12345, "loc": [123, 456] }
    success = insertRecord(acTk, sD)

def insertRecord(accessToken, sensorData):
    res = db.accessKeys.find_one(accessToken)
    print res
    print 'hi'
    if res != None:
        dataRow = {}
        dataRow['s'] = res['s'][sensorData['sensorId']]
        dataRow['d'] = res['d'][sensorData['deviceId']]
        dataRow['l'] = sensorData['loc']
        dataRow['aK'] = accessToken
        dataRow['t'] = sensorData['timeStamp'] 
        db.data.insert(dataRow)
        db.stream.insert(dataRow)
    else:
        return False

def printData():
    print db.accessKeys.find_one()
    print db.devices.find_one()
    print db.sensors.find_one()
    print db.data.find_one()

prepareDatabase()
testInsert()
printData()

