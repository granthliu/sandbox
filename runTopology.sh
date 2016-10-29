#!/bin/bash

#Usage
#  ./runTopology <storm-yarn appid> <name of topology> <nimbus server> <number of workers> 
# Rest of Arguments (in order): 
# $DEPLOYMODE - cluster or local
# $EXECTUOR - executor number applied to each bolt/spout
# $SPOUTRATIO - executor ration for spout
# $NORMALIZATIONRATIO  - executor ration for normalization bolt
# $JMSPUBRATIO - executor ration for jms repub bolt 
# $HDFSRATIO - executor ration for hdfs bolt 
# $INDEXRATIO - executor ration for index bolt 
# $PARALLELPATHS - how many parallel spout=>bolt=>bolt paths to run, up to 3
# $TRUNCATENORMALIZE - for tuning, truncate from normalize onwards
# $TRUNCATEREPUB - for tuning, truncate repub
# $TRUNCATEHDFS - for tuning, truncate hdfs sink
# $TRUNCATEINDEX - for tuning, truncate index

#

APPID=$1
TOPOLOGYNAME=$2
NIMBUS=$3

NUMWORKER=$4
DEPLOYMODE=$5
EXECUTOR=$6
SPOUTRATIO=$7
NORMALIZATIONRATIO=$8
JMSPUBRATIO=$9
HDFSRATIO=$10
INDEXRATIO=$11
PARALLEL=$12
TRUNCATENORM=$13
TRUNCATREPUB=$14
TRUNCATEHDFS=$15
TRUNCATEINDEX=$16

#build
mvn package

#This is out of date, check out TransactionProcessorTopology for usage (comments at top of class)
#deploy topology
storm jar target/poc-1.0-SNAPSHOT-jar-with-dependencies.jar com.hortonworks.wfbs.topologies.TransactionProcessorTopology $TOPOLOGYNAME $NUMWORKER $DEPLOYMODE $EXECUTOR $SPOUTRATIO $NORMALIZATIONRATIO $JMSPUBRATIO $HDFSRATIO $INDEXRATIO $PARALLEL $TRUNCATENORM $TRUNCATREPUB $TRUNCATEHDFS $TRUNCATEINDEX -c nimbus.host=$NIMBUS
