#!/usr/bin/env bash

CERT_FOLDER=/home/mathieu/mgdev/certs

export HOST=`uname -n`

export MG_MQ_CAFILE=$CERT_FOLDER/pki.millegrille.cert
export MG_MQ_CERTFILE=$CERT_FOLDER/pki.fichiers.cert
export MG_MQ_KEYFILE=$CERT_FOLDER/pki.fichiers.key
export SFTP_ED25519_KEY=$CERT_FOLDER/pki.fichiers.sftp.ed25519
export SFTP_RSA_KEY=$CERT_FOLDER/pki.fichiers.sftp.rsa
export WEB_CERT=$MG_MQ_CERTFILE
export WEB_KEY=$MG_MQ_KEYFILE
export MG_MQ_URL=amqps://$HOST:5673
export IPFS_HOST=http://192.168.2.131:5001
export MG_SERVEUR_INDEX_URL=http://192.168.2.131:9200
export MG_REDIS_HOST=$HOST

# Path ou les apps web (Vitrine, Place) sont copiees
export WEBAPPS_SRC_FOLDER=/var/opt/millegrilles/nginx/html

export PORT=3021

export DEBUG=millegrilles:common:server4,\
millegrilles:routes:backup,millegrilles:util:backup,millegrilles:util:processFichiersBackup,\
millegrilles:util:restaurationBackup,millegrilles:fichiers:uploadFichier,\
millegrilles:messages:media,millegrilles:fichiers:traitementMedia,millegrilles:fichiers:transformationsVideo,\
millegrilles:fichiers:cryptoUtils,millegrilles:utilpki

# export SERVER_TYPE=https

# Desactiver validation usager locale
# export IDMG=z2xMUPJHXDgkLEgdziA21EuA4BCvtWtKLuSnyqVJyvexgxj8yPsMRW
# export DISABLE_SSL_AUTH=1

npm start
