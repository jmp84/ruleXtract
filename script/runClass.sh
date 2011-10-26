#!/bin/sh

JAVA=/home/blue1/jmp84/phd/code/java/jdk1.6.0_17/bin/java
HADOOPBASE=/home/blue7/aaw35/libs/hadoop-0.21.0
CLASSPATH=/home/blue1/jmp84/workspace/ruleXtract/bin
for jarfile in `ls $HADOOPBASE/*.jar`; do
    CLASSPATH=$CLASSPATH:$jarfile
done
for jarfile in `ls $HADOOPBASE/lib/*.jar`; do
    CLASSPATH=$CLASSPATH:$jarfile
done
# for debugging run in standalone
#CLASSPATH=$CLASSPATH:/home/blue1/jmp84/workspace/ruleXtract/debug/conf

$JAVA -cp $CLASSPATH $@
