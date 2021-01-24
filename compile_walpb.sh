#!/bin/bash

if [ -z ${GOMODULEPATH} ]; then
	echo "no GOMODULEPATH provided!!!"
	exit 1
fi

cd ${GOMODULEPATH}
for i in $(ls ${GOMODULEPATH}/github.com/amazingchow/photon-dance-wal/walpb/*.proto); do
	fn=github.com/amazingchow/photon-dance-wal/walpb/$(basename "$i")
	echo "compile" $fn
	/usr/local/bin/protoc --go_out=plugins=grpc:. "$fn"
done