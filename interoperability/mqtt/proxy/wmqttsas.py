# Trace MQTT traffic

# Author: Ian Craggs

import mqtt.formats.MQTTV311 as MQTTV3

import socket, sys, select, socketserver, traceback, datetime, os

import wx, threading
import wx.lib.mixins.listctrl  as  listmix

logging = True
myWindow = None

myhost = 'localhost'
if len(sys.argv) > 1:
  brokerhost = sys.argv[1]
else:
  brokerhost = 'localhost'

if len(sys.argv) > 2:
  brokerport = int(sys.argv[2])
else:
  brokerport = 1883

if brokerhost == myhost:
  myport = brokerport + 1
else:
  myport = brokerport
  myport = 1883

def timestamp():
  now = datetime.datetime.now()
  return now.strftime('%Y%m%d %H%M%S')+str(float("."+str(now.microsecond)))[1:]

class MyHandler(socketserver.StreamRequestHandler):

  def handle(self):
    if not hasattr(self, "ids"):
      self.ids = {}
    if not hasattr(self, "versions"):
      self.versions = {}
    inbuf = True
    i = o = e = None
    try:
      clients = self.request
      brokers = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      brokers.connect((brokerhost, brokerport))
      while inbuf != None:
        (i, o, e) = select.select([clients, brokers], [], [])
        for s in i:
          if s == clients:
            inbuf = MQTTV3.getPacket(clients) # get one packet
            if inbuf == None:
              break
            try:
              packet = MQTTV3.unpackPacket(inbuf)
              if packet.fh.MessageType == MQTTV3.PUBLISH and \
                packet.topicName == "MQTTSAS topic" and \
                packet.data == "TERMINATE":
                print("Terminating client", self.ids[id(clients)])
                brokers.close()
                clients.close()
                break
              else:
                if packet.fh.MessageType == MQTTV3.CONNECT:
                  self.ids[id(clients)] = packet.ClientIdentifier
                  self.versions[id(clients)] = 3
              wx.CallAfter(myWindow.log, timestamp() , "C to S",
                        self.ids[id(clients)], repr(packet))
            except:
              traceback.print_exc()
              # next line should not be needed once things work properly
              print("C to S", timestamp(), repr(inbuf))
              sys.exit()
            #print "C to S", timestamp(), repr(inbuf)
            brokers.send(inbuf)       # pass it on
          elif s == brokers:
            inbuf = MQTTV3.getPacket(brokers) # get one packet
            if inbuf == None:
              break
            try:
              wx.CallAfter(myWindow.log, timestamp(), "S to C", self.ids[id(clients)],
                               repr(MQTTV3.unpackPacket(inbuf)))
            except:
              traceback.print_exc()
              # next line should not be needed once things work properly
              print("S to C", timestamp(), repr(inbuf))
              sys.exit()
            #print "S to C", timestamp(), repr(inbuf)
            clients.send(inbuf)
      wx.CallAfter(myWindow.status, timestamp()+" client "+self.ids[id(clients)]+\
                   " connection closing")
    except:
      print(repr((i, o, e)), repr(inbuf))
      traceback.print_exc()
    if self.ids.has_key(id(clients)):
      del self.ids[id(clients)]
    elif self.versions.has_key(id(clients)):
      del self.versions[id(clients)]

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass

class WorkerThread(threading.Thread):

  def __init__(self):
    threading.Thread.__init__(self)
    self.s = ThreadingTCPServer(("", myport), MyHandler)
    self.toRun = True

  def run(self):
    while self.toRun:
      self.s.handle_request()

  def stop(self):
    self.toRun = False

cols = ["No", "Timestamp", "Direction", "ClientID", "Packet"]
widths = [60, 100, 80, 100, 400]

class Frame(wx.Frame, listmix.ColumnSorterMixin):

  def __init__(self):
    global myWindow
    wx.Frame.__init__(self, None, -1, "MQTT Protocol Trace", size=(600, 400))
    self.Bind(wx.EVT_CLOSE, self.OnCloseWindow)
    self.Bind(wx.EVT_LIST_ITEM_SELECTED, self.OpenDetails)

    self.list = wx.ListCtrl(self, -1, style=wx.LC_REPORT)
    self.statusBar = self.CreateStatusBar()
    menubar = wx.MenuBar()
    menu1 = wx.Menu()
    clear = menu1.Append(wx.NewId(), "&Clear")
    self.Bind(wx.EVT_MENU, self.OnClear, clear)
    saveas = menu1.Append(wx.NewId(), "&Save as")
    self.Bind(wx.EVT_MENU, self.OnSaveAs, saveas)
    menubar.Append(menu1, "&File")
    self.SetMenuBar(menubar)

    for index, title in enumerate(cols):
      self.list.InsertColumn(index, title)
      self.list.SetColumnWidth(index, widths[index])

    self.listitem = 0
    listmix.ColumnSorterMixin.__init__(self, len(cols))

    self.itemDataMap = {}

    myWindow = self
    self.thread = WorkerThread()
    self.thread.start()

  def log(self, timestamp, direction, clientid, packet):
    self.list.InsertStringItem(self.listitem, str(self.listitem))

    if direction.startswith("C"):
      self.list.SetItemTextColour(self.listitem, wx.RED)
    else:
      self.list.SetItemTextColour(self.listitem, wx.BLUE)

    self.list.SetStringItem(self.listitem, 1, timestamp)
    self.list.SetStringItem(self.listitem, 2, direction)
    self.list.SetStringItem(self.listitem, 3, clientid)
    self.list.SetStringItem(self.listitem, 4, packet)

    self.list.SetItemData(self.listitem, self.listitem)
    self.itemDataMap[self.listitem] = (self.listitem, timestamp, direction, clientid, packet)

    self.list.EnsureVisible(self.listitem)
    self.listitem += 1

  def status(self, text):
    self.statusBar.SetStatusText(text)

  def GetListCtrl(self):
    return self.list

  def OnCloseWindow(self, evt):
    self.thread.stop()
    self.Destroy()

  def OpenDetails(self, evt):
    item = evt.GetItem()
    print("item is", item)
    self.status(item.GetText())
    self.status(item.GetColumn(3))

  def OnClear(self, evt):
    self.list.DeleteAllItems()
    self.itemDataMap = {}
    self.listitem = 0

  def OnSaveAs(self, evt):
    dialog = wx.FileDialog(None, "Log File to Save to", defaultDir=".",
                  defaultFile="MQTT.log", style=wx.SAVE)
    if dialog.ShowModal() == wx.ID_OK:
      filename = dialog.GetPath()
    dialog.Destroy()
    file = open(filename, "w+")
    sortedKeys = self.itemDataMap.keys()
    sortedKeys.sort()
    for i in sortedKeys:
      curitem = self.itemDataMap[i]
      file.write(curitem[1]+" "+curitem[2]+" "+curitem[3]+" "+curitem[4]+"\n\n")
    file.close()

app = wx.PySimpleApp(redirect=0)
frame = Frame()
frame.status("Listening on port "+str(myport))
frame.Show()
app.MainLoop()
