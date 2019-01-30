#! /bin/bash

# $1 : java code name without ".java"
# $2 : data path in HDFS

if [[ -z "$2" ]]
then
        exit 0
fi

javac -classpath `hadoop classpath` -d classes $1.java
jar -cvf $1.jar -C classes/ .
hadoop fs -rm -r -f /$2.out
hadoop jar $1.jar $1 /$2 /$2.out
hadoop fs -cat /$2.out/* > result
