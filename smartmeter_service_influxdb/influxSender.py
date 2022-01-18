#!/usr/bin/python3

import datetime
from influxdb import InfluxDBClient

class InfluxSender:
  def __init__(self, debugflag=False):
    # Initialize state.
    self._DEBUG = debugflag
    self.database = ''

  def debugPrint(self, s):
    if self._DEBUG:
      print("InfluxSender: " + s)
    return

  def open(self, host, port, database):
    self.debugPrint("Initializing InfluxDB client: Host={} Port={} Database=\'{}\'".format(host, port, database))
    # Create instance.
    self.dbclient = InfluxDBClient(host=host, port=port, database=database)
    #client = InfluxDBClient(host='mydomain.com', port=8086, username='myuser', password='mypass' ssl=True, verify_ssl=True)
    self.database = database

  def close(self):
    return

  def sendPoints(self, measurement, tags, linestring, time=None):
    # Formulate influxDB line protocol.
    tagstring = InfluxSender.dict2lineformat(tags, string_use_quotes=False)
    if tagstring is None:
      dbstring = "{:s},{:s} {:s} {:d}".format(measurement, tagstring, linestring, int(InfluxSender.timestamp(time)*1e9))
    else:
      dbstring = "{:s} {:s} {:d}".format(measurement, linestring, int(InfluxSender.timestamp(time)*1e9))
    self.debugPrint(dbstring)
    # Send it.
    return self.dbclient.write_points(dbstring, database=self.database, protocol=u'line')

  def sendDict(self, measurement, tags, recorddict, time=None):
    #print(recorddict)
    #print(InfluxSender.dict2lineformat(recorddict))
    return self.sendPoints(measurement=measurement, tags=tags, linestring=InfluxSender.dict2lineformat(recorddict), time=time)

  def timestamp(t):
    # Generate influxdb timestamp from datetime object.
    if t == None:
      t = datetime.datetime.utcnow()
    return (t - datetime.datetime.utcfromtimestamp(0)).total_seconds()

  def dict2lineformat(d, string_use_quotes=True):
    report = ""
    if d is None:
      return report
    for key in sorted(d):
      if isinstance(d[key], float):
        report = report + "{:s}={:0.7f},".format(key, d[key])
      elif isinstance(d[key], int):
        report = report + "{:s}={:d},".format(key, d[key])
      elif isinstance(d[key], str):
        if string_use_quotes:
          report = report + "{:s}=\"{:s}\",".format(key, d[key])
        else:
          report = report + "{:s}={:s},".format(key, d[key])
      elif d[key] is not None:
        report = report + "{:s}={:},".format(key, d[key])
      else:
        # d[key] is None so leave it out of dataset.
        pass
    # Trim trailing comma.
    report = report[0:-1]
    return report

def main(argv):
  # test operation
  print("Initialize object")
  ifs = InfluxSender(debugflag = True)

if __name__ == "__main__":
  main(sys.argv)
