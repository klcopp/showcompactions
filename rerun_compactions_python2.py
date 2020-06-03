#!/usr/bin/python2
import datetime
import fileinput

def parse_file():
  global activeWorkers
  global waitingCompactions
  '''
split line looks like:
['', ' 1440816       ', ' dbname       ', ' tblname           ', '  partname                         ', ' MAJOR  ', ' failed     ', ' hostname  ', ' 1583272973000  ', ' 0         ', ' None                      ', '\n']
'''
  for line in fileinput.input():
    if "|" not in line or "compactionid" in line or "Start Time" in line:
      continue
    lineContents = line.split("|")
    dbname = lineContents[2].strip()
    tablename= lineContents[3].strip()
    partition = lineContents[4].strip()
    compactionType = lineContents[5].strip()
    status = lineContents[6].strip()
    host = lineContents[7].strip()
    if status != "initiated":
      timestamp = int(lineContents[8].strip())
      humanreadabletimestamp = datetime.datetime.fromtimestamp(timestamp/1000).strftime("%Y-%m-%d %H:%M:%S")
      if status == "working":
        activeWorkers += 1
    else:
      timestamp = "---"
      humanreadabletimestamp = "---"
      waitingCompactions += 1

    partitionid = dbname + "," + tablename + "," + partition

    compactioninfo = [dbname, tablename, partition, timestamp, humanreadabletimestamp, compactionType, status, host]
    if partitionid not in partitionMap.keys() or timestamp > partitionMap[partitionid][3]:
      partitionMap[partitionid] = compactioninfo

def print_rerun_compaction():
  for partitionid in partitionMap:
    status = partitionMap[partitionid][6]
    if status == "failed" or status == "attempted":
      if partitionMap[partitionid][2] == "---":
        print "alter table " + partitionMap[partitionid][0] + "." + partitionMap[partitionid][1] + " compact '" + partitionMap[partitionid][5] + "';"
      else:
        print "alter table " + partitionMap[partitionid][0] + "." + partitionMap[partitionid][1] + " partition (" + partitionMap[partitionid][2].replace('/',',') + ") compact '" + partitionMap[partitionid][5] + "';"

partitionMap = {}
activeWorkers = 0
waitingCompactions = 0
parse_file()

print_rerun_compaction()
