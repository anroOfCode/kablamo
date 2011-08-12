import socket
import array
import binascii
import struct

HOST = 'localhost'
PORT = 8007
for i in range(0, 50):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    accessKey = binascii.unhexlify('4e4480e3990aef1ba9000002')
    dTs = struct.pack('<B12sBIIHHBHI', 1, accessKey, 1, 0, 0, 0, 1, 1, 0, 50)
    a = array.array('B', [1, 0x39, 0x30, 0, 0, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    s.send(dTs)
    s.close()
