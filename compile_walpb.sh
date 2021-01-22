#!/bin/bash

cd $GOMODULEPATH
for i in $(ls $GOMODULEPATH/github.com/amazingchow/photon-dance-wal/walpb/*.proto); do
	fn=github.com/amazingchow/photon-dance-wal/walpb/$(basename "$i")
	echo "compile" $fn
	/usr/local/bin/protoc -I/usr/local/include -I . --go_out=. "$fn"
done
