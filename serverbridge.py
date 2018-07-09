import socket
from SimpleWebSocketServer import SimpleWebSocketServer, WebSocket
from threading import Thread

wsclients = []

class SimpleEcho(WebSocket):

    def handleMessage(self):
        pass

    def handleConnected(self):
       self.sendMessage("ws connected")
       wsclients.append(self)

    def handleClose(self):
        print(self.address, 'closed')


server = SimpleWebSocketServer('', 8000, SimpleEcho)
Thread(target=server.serveforever).start()
print("Listening on websocket")

s = socket.socket()
s.bind(("localhost",5555))
s.listen(1)
rec, addr = s.accept()

buf = ""
while True:
    buf += rec.recv(1024).decode("ascii")
    while "\n" in buf:
        line, rest = buf.split("\n", maxsplit=1) 
        for client in wsclients:
            print(line)
            client.sendMessage(line)
        buf = rest
        
