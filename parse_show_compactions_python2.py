#!/usr/bin/python2
import datetime
import sys


def parse_file():
  global activeWorkers
  global waitingCompactions
  file = open(fileName, 'r')
  '''
split line looks like:
['', ' 1440816       ', ' dbname       ', ' tblname           ', '  partname                         ', ' MAJOR  ', ' failed     ', ' hostname  ', ' 1583272973000  ', ' 0         ', ' None                      ', '\n']
'''
  for line in file:
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

  file.close()

def print_last_compaction():
  header = ["Database", "Table", "Partition", "UTC start", "Local time of start", "Compaction Type", "Status", "Host"]
  for element in header:
    print '{:>20}'.format(element),
  print ""
  for partitionid in partitionMap:
    status = partitionMap[partitionid][6]
    if status == "failed" or status == "attempted":
      for element in partitionMap[partitionid]:
        print '{:>20}'.format(element),
      print ""

fileName = sys.argv[1]
print "Parsing file:", fileName
partitionMap = {}
activeWorkers = 0
waitingCompactions = 0
parse_file()

print "Active workers:", activeWorkers
print "Waiting compactions:", waitingCompactions
print "The following tables/partitions' most recent compaction is in a failed or attempted state:"
print_last_compaction()
