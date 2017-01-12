#!/bin/sh

SOURCE=$1
DESTINATION=$2

echo "Reading from ${SOURCE}"
echo "======================"
echo

for shard in $SOURCE/*; do
    for schema in ${shard}/*.cql; do
        stage=$(basename ${schema})
        stage=${stage%.cql}
        echo ${shard}/${stage}
        cp ${shard}/${stage}.cql ${DESTINATION}/${stage}.cql
        cat ${shard}/${stage}.csv >> ${DESTINATION}/${stage}.csv
    done
done

echo
echo 'Results:'
echo '========'
echo

ls -lh ${DESTINATION}/
