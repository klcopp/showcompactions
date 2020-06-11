# showcompactions
parse hive 3.1 "show compactions" output to find partitions where last compaction was failed/attempted

Usage:
```
$ python3 parse_show_compactions.py <show_compactions_output>
```

or 
```
$ python2 parse_show_compactions_python2.py <show_compactions_output>
$ ./parse_show_compactions_python2.py <show_compactions_output>
$ beeline -u hive -p hive -e "show compactions" | python2 parse_show_compactions_python2.py
$ beeline -u hive -p hive -e "show compactions" | ./parse_show_compactions_python2.py
```

# reruncompactions
parse hive 3.1 "show compactions" output to find tables/partitions where the last compaction was failed/attempted and creates an sql script to rerun the compactions for them. For example:

```
alter table <dbname>.<tablename> compact 'MINOR';
alter table <dbname>.<tablename> partition (<partition>) compact 'MAJOR';
```

Be aware that if the partition value contains '/' there might be a problem with the command.

Usage:
```
$ python2 rerun_compactions_python2.py <show_compactions_output>
$ ./rerun_compactions_python2.py <show_compactions_output>
$ beeline -u hive -p hive -e "show compactions" | python2 rerun_compactions_python2.py
$ beeline -u hive -p hive -e "show compactions" | ./python2 rerun_compactions_python2.py
```
