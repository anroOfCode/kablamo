from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
import struct
import txmongo
from twisted.internet.defer import inlineCallbacks, returnValue 
from twisted.internet import defer
import time
from txmongo._pymongo.objectid import ObjectId

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class DataStruct():
    pass

class DataServ(Protocol):
    _sful_data = None, None, 0

    def connectionMade(self):
        #self.transport.write(self.factory.quote+'\r\n')
        self._sful_data = self.getInitialState(), StringIO(), 0
        #self.transport.loseConnection()

    def setVersionAccess(self, data):
	self.dataStruct = DataStruct()
        fields = struct.unpack('<B12s',data )
        self.dataStruct.version, self.dataStruct.accessKey = fields 
	print fields 
        return self.processFixedFields, 14
 
    def processFixedFields(self, data):
        fields = struct.unpack('<BIIHHB', data)
        self.dataStruct.transferMode, self.dataStruct.lat, self.dataStruct.log, self.dataStruct.deviceId, self.dataStruct.timeSlots, self.dataStruct.sensorSlots = fields
        print fields

        allowToContinue = False
        if self.dataStruct.transferMode == 0 and self.dataStruct.timeSlots > 0:
            allowToContinue = True
        if self.dataStruct.transferMode == 1 and self.dataStruct.timeSlots == 1:
            allowToContinue = True
        if self.dataStruct.sensorSlots > 16:
            allowToContinue = False

        if allowToContinue:
            if self.dataStruct.transferMode == 0:
                return self.processSensorData, self.dataStruct.timeSlots * 8 + self.dataStruct.sensorSlots * 2 + self.dataStruct.timeSlots * self.dataStruct.sensorSlots * 4
            else:
                return self.processSensorData, self.dataStruct.sensorSlots * 2 + self.dataStruct.sensorSlots * 4
        else:
            self.transport.loseConnection()
            return None

    def processSensorData(self, data):
        self.dataStruct.data = []
        self.dataStruct.sensors = []
        offset = 0
        for i in range(0, self.dataStruct.sensorSlots):
            self.dataStruct.sensors.append(struct.unpack('<H', data[offset: offset+2])[0])
            offset += 2

        if self.dataStruct.transferMode == 0:
            for i in range(0, self.dataStruct.timeSlots):
                self.dataStruct.data.append( DataStruct())
                self.dataStruct.data[i].timeStamp = struct.unpack('<Q', data[offset: offset+8])[0]
                self.dataStruct.data[i].sensorValues = [] 
                offset += 8
        else:
            self.dataStruct.data.append(DataStruct())
            self.dataStruct.data[i].timeStamp = int(round(time.time()*1000))
            self.dataStruct.data[i].sensorValues = []

        for i in range(0, self.dataStruct.timeSlots):
            for j in range(0, self.dataStruct.sensorSlots):
                self.dataStruct.data[i].sensorValues.append(struct.unpack('<I', data[offset: offset+4])[0])
                offset += 4
        d  = defer.maybeDeferred(self.factory.processData, self.dataStruct)
        d.addCallback(self.handleWriteCallback)
        return None
 
    def handleWriteCallback(self, result):
        self.transport.write(result)
        self.transport.loseConnection()

    def getInitialState(self):
        return self.setVersionAccess, 13

    def dataReceived(self, data):
        state, buffer, offset = self._sful_data
        buffer.seek(0, 2)
        buffer.write(data)
        blen = buffer.tell() # how many bytes total is in the buffer
        buffer.seek(offset)
        while blen - offset >= state[1]:
            d = buffer.read(state[1])
            offset += state[1]
            next = state[0](d)
            if self.transport.disconnecting: # XXX: argh stupid hack borrowed right from LineReceiver
                return # dataReceived won't be called again, so who cares about consistent state
            if next:
                state = next
        if offset != 0:
            b = buffer.read()
            buffer.seek(0)
            buffer.truncate()
            buffer.write(b)
            offset = 0
        self._sful_data = state, buffer, offset

class DataFactory(Factory):
 
    protocol = DataServ

    def __init__(self):
        pass
 
    @inlineCallbacks
    def insertDataRow(self, mDb, accessKeyRow, deviceId, dataRow, sensorMatrix, location):
        for i in range(0, len(sensorMatrix)):
            insertObject = {}
            insertObject["aK"] = accessKeyRow['_id']
            insertObject["d"] = accessKeyRow['d'][deviceId]
            insertObject["s"] = accessKeyRow['s'][sensorMatrix[i]]
            insertObject["t"] = dataRow.timeStamp
            insertObject["v"] = dataRow.sensorValues[i]
            insertObject["l"] = location
            yield mDb.data.insert(insertObject)
            yield mDb.stream.insert(insertObject) 

    @inlineCallbacks
    def processData(self, data):
        db = yield txmongo.MongoConnection()
        mDb = db.data 
        accessToken =  yield mDb.accessKeys.find_one({"_id" : ObjectId(data.accessKey)})
        print 'Do we have a result?'
	print str(accessToken)
        if accessToken != None: # do we have results?
            if accessToken['e'] == True:
                for dR in data.data:
                    self.insertDataRow(mDb, accessToken, data.deviceId, dR, data.sensors, [data.lat, data.log])
                yield 2
            else:
                yield 1
        else:
            print 'awh'
            yield 0

reactor.listenTCP(8007, DataFactory())
reactor.run()
