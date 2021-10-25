#!/usr/bin/env bash

# CERT_FOLDER=/home/mathieu/mgdev/certs

# export HOST=`uname -n`

#export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
#export MG_MQ_CERTFILE=$CERT_FOLDER/pki.media.cert
#export MG_MQ_KEYFILE=$CERT_FOLDER/pki.media.key
#export MG_MQ_URL=amqps://$HOST:5673
#export MG_REDIS_HOST=$HOST

#export MG_ELASTICSEARCH_URL=http://192.168.2.131:9200
# export IPFS_HOST=http://192.168.2.131:5001

# export PORT=3021

#export DEBUG=millegrilles:common:server4

# export SERVER_TYPE=https

export ENV=DEV

npm start
