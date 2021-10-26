#!/bin/bash
set -e

# Faire lien vers package.json de consignationfichiers
ln -f ../package.json
ln -f ../package-lock.json

# Nettoyager package existants
#rm -rf node_modules

# Installer dependances
#npm i --production
