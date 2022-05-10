#!/bin/sh

mkdir -p /build

cp -r ./docker/twins/config/. /build

cp ./iothub.crt /build

go build -o /build/twins ./cmd/twins

cd /build

exec ./twins -configFile=config.json