#!/bin/sh

JAVA=java
JAR=$(pwd)/bg-generate-cassandra-sstables-1.0-SNAPSHOT-uber.jar
KEYSPACE=biggraphite
SOURCE=$1

echo "Reading from ${SOURCE}"
echo "======================"
echo

cd ${SOURCE}

INPUT='INPUT'
rm -f ${INPUT}

for schema in *.cql; do
    stage=${schema%.cql}
    points=$(echo $stage | cut -d '*' -f1)p
    resolution=$(echo $stage | cut -d '*' -f2)
    echo ${KEYSPACE} datapoints_${points}_${resolution} "$stage.cql" "$stage.csv" >> ${INPUT}
done

echo parallel -j4 --progress --bar --colsep -- ${JAVA} -jar ${JAR}
cat ${INPUT} | parallel --no-notice -j4 --progress --bar --colsep ' ' -- ${JAVA} -jar ${JAR}
