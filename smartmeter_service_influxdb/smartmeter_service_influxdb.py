#!/usr/bin/env python3

# system
import datetime
import json
import select
import socket
import sys
import time
# library
import paho.mqtt.client as mqtt
# internal
import influxSender

# Configuration options.
Config = {
  'Server': {
    'Host': '192.168.1.28',
    'Log.Filename': 'smartmeter.log',
    'Print': True,
    'Influx': True,
    'Debug': {
      'Dummy' : True,
    },
  },
  'Mqtt': {
    'Host': 'localhost',
    'Username': '',
    'Password': '',
    'ListenOnly': False
  },
  'Mqtt2': {
    'Host': '',
    'Username': '',
    'Password': '',
    'ListenOnly': True
  },
  'Influx': {
    # Host/Database options.
    'Host': 'localhost',
    'Port': 8086,
    'Database': 'power',
    #'Database': 'demo',
    # Record options.
    'TimeStamp': 'host',
    #'TimeStamp': 'device'
    # Debug.
    'Debug': True,
  },
}

def SmartmeterDebugPrint(string):
  now = datetime.datetime.utcnow()
  msg = str(now) + ": Smartmeter: " + str(string)
  #print >>sys.stderr, 'LOGTRIP Rx[%dB]: \'%s\'' % (len(data), data)
  if Config['Server']['Print']:
    print(msg)
  if(flog):
    flog.write(msg + "\n")
    flog.flush()
  return

def SmartmeterCountRxPacket():
  global count_rx
  count_rx += 1
  return

def smartmeter_handle_report(sock, my_address, peer_address, devid, data):
  SmartmeterDebugPrint("REPORT Rx[{:d}B]: \'{:s}\'".format(len(data), data))
  #fields = csv.reader([data], delimiter=',')
  #print(list(fields))
  # Set database tags based on device id.
  #deviceId = lp.DeviceDataDict.get('Device.Id', None)
  #deviceTags = devdb.GetTags(deviceId)
  deviceTags = None
  # Send to database.
  if (db):
  #  db.sendDict(measurement='meter', tags=deviceTags, recorddict=lp.DeviceDataDict, time=None)
    db.sendPoints(measurement='meter', tags=deviceTags, linestring=data, time=None)
  return

def smartmeter_handle_misc(sock, my_address, peer_address, devid, data):
  SmartmeterDebugPrint('MISC Rx[{:d}B]: \'{:s}\''.format(len(data), data))
  # Send to database.
  #if (db):
  #  db.sendDict(measurement='device', tags=deviceTags, recorddict=ls.DeviceDataDict, time=None)
  return

MQTT_HANDLERS = {
  'smartmeter/report' : smartmeter_handle_report,
  #'smartmeter/#'      : smartmeter_handle_misc,
}

def mqtt_handle_traffic(mqttc, topic, data):
  # Capture timestamp.
  t = time.process_time()
  # Convert to string
  try:
    data_str = data.decode('utf-8')
    # Handle it
    #SmartmeterDebugPrint('Received %d bytes from %s' % (len(data), topic))
    #SmartmeterDebugPrint(data)
    SmartmeterCountRxPacket()
  except:
    SmartmeterDebugPrint("MQTT Malformed data, skipping packet: Data=" + str(data))
  #try:
  if True:
    # Get handler (plain)
    handler = MQTT_HANDLERS.get(topic)
    devid = data_str.split(',')[0]
    # Get handler (with MQTT wildcard)
    if handler is None:
      topicroot = topic.split('/')[0]
      devid = topic.split('/')[1]
      handler = MQTT_HANDLERS.get(topicroot+'/#')
    # Run handler.
    if handler:
      handler(mqttc, None, None, devid, data_str)
  #except:
  #  SmartmeterDebugPrint("MQTT Handler failed, skipping packet: Data=" + str(data))
  # Measure handling time.
  t = time.process_time() - t
  SmartmeterDebugPrint("MQTT Handled packet #{:d} in {:.4} msec".format(count_rx, t*1000))

# MQTT server setup
def mqtt_server_connect(config):
  if config['Host']:
    SmartmeterDebugPrint("MQTT connecting to '{}'".format(config['Host']))
  else:
    return None
  mqttc = mqtt.Client("server-{}".format(socket.gethostname()))
  if config['Username']:
    mqttc.username_pw_set(username=config['Username'],password=config['Password'])
  mqttc.on_connect = mqtt_on_connect
  mqttc.on_disconnect = mqtt_on_disconnect
  mqttc.on_message = mqtt_on_message
  mqttc.listen_only = config['ListenOnly']   # Custom field
  mqttc.connect(config['Host'], port=1883, keepalive=60)
  SmartmeterDebugPrint("MQTT Starting loop")
  mqttc.loop_start()
  return mqttc

def mqtt_server_disconnect(config, mqttc):
  if config['Host']:
    SmartmeterDebugPrint("MQTT Stopping loop for '{}'".format(config['Host']))
  if hasattr(mqttc, 'loop_stop'):
    mqttc.loop_stop()
    mqttc.disconnect()
  return

# The callback for when the client receives a CONNACK response from the server.
def mqtt_on_connect(mqttc, obj, flags, rc):
  SmartmeterDebugPrint("MQTT Connected to server with result code "+str(rc))
  if rc == 5:
    SmartmeterDebugPrint("MQTT Authentication Error")
  # Subscribing in on_connect() means that if we lose the connection and
  # reconnect then subscriptions will be renewed.
  SmartmeterDebugPrint("MQTT Subscribing to topics")
  #mqttc.subscribe("$SYS/#")
  mqttc.subscribe("smartmeter/report")
  #mqttc.subscribe("smartmeter/#")

# The callback for when the client is disconnected.
def mqtt_on_disconnect(mqttc, obj, rc):
  SmartmeterDebugPrint("MQTT Disconnected from server with result code "+str(rc))

# The callback for when a PUBLISH message is received from the server.
def mqtt_on_message(mqttc, obj, msg):
  SmartmeterDebugPrint("MQTT Rx: "+msg.topic+" "+str(msg.payload))
  mqtt_handle_traffic(mqttc, msg.topic, msg.payload)


def main(argv):
  global Config
  global flog
  global db
  global count_rx

  print("Smartmeter Server")
  print("---------------------")

  # Load config
  print("Loading config...")
  try:
    with open('smartmeter.conf') as infile:
      Config = json.load(infile)
  except:
    with open('smartmeter.conf', 'w') as outfile:
      print("No config file, writing defaults")
      json.dump(Config, outfile, indent=2)

  # Log file
  print("Open log file '{}'".format(Config['Server']['Log.Filename']))
  try:
    flog = open(Config['Server']['Log.Filename'], "a+")
  except:
    SmartmeterDebugPrint("Cannot open log -- using /dev/null")
    flog = open("/dev/null", "w+")
  flog.write("Smartmeter: Start Log\n")

  # Influx database connection
  if Config['Server']['Influx']:
    SmartmeterDebugPrint("Initializing InfluxDB Sender")
    # Create instance.
    db = influxSender.InfluxSender(debugflag=Config['Influx']['Debug'])
    db.open(host=Config['Influx']['Host'], port=Config['Influx']['Port'], database=Config['Influx']['Database'])
  else:
    db = None

  # Device MQTT connections
  mqttc = mqtt_server_connect(Config['Mqtt'])
  mqttc2 = mqtt_server_connect(Config['Mqtt2'])

  # Main loop
  SmartmeterDebugPrint("Starting receive message loop -- press CTRL-C to exit")
  count_rx = 0
  try:
    while True:
      # MQTT traffic comes in automatically by callback.
      time.sleep(0.01)
  except KeyboardInterrupt:
    SmartmeterDebugPrint("Terminated by user")

  # Device MQTT disconnections
  SmartmeterDebugPrint("MQTT disconnecting")
  mqtt_server_disconnect(Config['Mqtt'], mqttc)
  mqtt_server_disconnect(Config['Mqtt2'], mqttc2)

  #sock.close()

  SmartmeterDebugPrint("Packets processed {:d}".format(count_rx))

  SmartmeterDebugPrint("Close log file")
  flog.close()

if __name__ == "__main__":
  main(sys.argv)

#raise InfluxDBClientError(response.content, response.status_code)
#influxdb.exceptions.InfluxDBClientError: 400: {"error":"unable to parse 'meter Timestamp_s=1632673041 Interval_s=4.894069 Iac_A=-0.900000 MeterSum=1553041271.466667 SolarSum=-1752860.258421 1632673041553136128': bad timestamp"}
# 1632673041553136128
# 1465839830100400200