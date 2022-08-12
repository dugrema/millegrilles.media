// Effectue le transfert avec le serveur de consignation
const debug = require('debug')('millegrilles:transfertConsignation')
const { pipeline } = require('node:stream');
const axios = require('axios')
const https = require('https')
const fs = require('fs')
const fsPromises = require('fs/promises')
// const tmp = require('tmp-promise')
const {v4: uuidv4} = require('uuid')
// const FormData = require('form-data')
const path = require('path')

const readdirp = require('readdirp')

const MIMETYPE_EXT_MAP = require('@dugrema/millegrilles.utiljs/res/mimetype_ext.json')

const {getDecipherPipe4fuuid, decipherTransform, creerOutputstreamChiffrage} = require('./cryptoUtils')

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle',
      UPLOAD_TAILLE_BLOCK = 5 * 1024 * 1024  // 5 MB,
      PATH_MEDIA_STAGING = '/var/opt/millegrilles/consignation/staging/media',
      PATH_MEDIA_DECHIFFRE_STAGING = '/var/opt/millegrilles/consignation/staging/mediaDechiffre',
      EXPIRATION_DECHIFFRE = 30 * 60000,
      INTERVALLE_ENTRETIEN = 5 * 60000

const downloadCache = {}

var _urlServeurConsignation = null,
    _httpsAgent = null,
    _storeConsignation = null,
    _intervalEntretien = setInterval(entretien, INTERVALLE_ENTRETIEN)

function init(urlServeurConsignation, amqpdao, storeConsignation) {
  debug("Initialiser transfertConsignation avec url %s", urlServeurConsignation)
  
  _urlServeurConsignation = new URL(''+urlServeurConsignation).href  // Cleanup URL

  _storeConsignation = storeConsignation

  fsPromises.mkdir(PATH_MEDIA_STAGING, {recursive: true}).catch(err=>console.error("ERROR Erreur mkdir %s", PATH_MEDIA_STAGING))
  fsPromises.mkdir(PATH_MEDIA_DECHIFFRE_STAGING, {recursive: true}).catch(err=>console.error("ERROR Erreur mkdir %s", PATH_MEDIA_DECHIFFRE_STAGING))

  const pki = amqpdao.pki
  // Configurer httpsAgent avec les certificats/cles
  // Note: pas de verif CA, la destination peut etre un serveur public 
  //       (Host Gator, Amazon, CloudFlare, etc.). C'est safe, le contenu 
  //       est chiffre et la cle est separee et signee.
  _httpsAgent = new https.Agent({
    rejectUnauthorized: false,
    // ca: pki.ca,
    cert: pki.chainePEM,
    key: pki.cle,
  })

}

function entretien() {
  debug("run Entretien")
  cleanupStagingDechiffre()
    .catch(err=>console.error("ERROR transfertConsignation.entretien Erreur cleanupStagingDechiffre : %O", err))
}

async function cleanupStagingDechiffre() {
  debug("Entretien cleanupStagingDechiffre")
  const dateCourante = new Date().getTime(),
        dateExpiree = dateCourante - EXPIRATION_DECHIFFRE
  
  for await (const entry of readdirp(PATH_MEDIA_DECHIFFRE_STAGING, {type: 'files', alwaysStat: true})) {
    const { basename, fullPath, stats } = entry
    debug("Entry fichier staging dechiffre : %s", fullPath)
    const fichierParse = path.parse(basename)
    const hachage_bytes = fichierParse.name
    const { mtimeMs } = stats
    if(!downloadCache[hachage_bytes] && mtimeMs < dateExpiree) {
      debug("cleanupStagingDechiffre Suppression fichier dechifre expire : %s", basename)
      try {
        await fsPromises.rm(fullPath)
      } catch(err) {
        debug("cleanupStagingDechiffre Erreur suppression fichier expire %s : %O", fullPath, err)
      }
    }
  }

}

// Download et dechiffre un fichier protege pour traitement local
async function downloaderFichierProtege(hachage_bytes, mimetype, cleFichier, opts) {
  opts = opts || {}
  let downloadCacheFichier = getDownloadCacheFichier(hachage_bytes, mimetype, cleFichier, opts)
  const pathFichier = await downloadCacheFichier.ready
  return { path: pathFichier, cleanup: downloadCacheFichier.clean, cacheEntry: downloadCacheFichier }
}

function getCacheItem(hachage_bytes) {
  let downloadCacheFichier = downloadCache[hachage_bytes]
  
  // Touch
  if(downloadCacheFichier) {
    downloadCacheFichier.lastAccess = new Date()
    if(downloadCacheFichier.timeout) {
      downloadCacheFichier.activerTimer()  // Reset le timer
    }
  }

  return downloadCacheFichier
}

function getDownloadCacheFichier(hachage_bytes, mimetype, cleFichier, opts) {
  opts = opts || {}

  let downloadCacheFichier = downloadCache[hachage_bytes]
  if(!downloadCacheFichier) {
    const url = new URL(''+_urlServeurConsignation)
    url.pathname = path.join(url.pathname, hachage_bytes)
    debug("Url download fichier : %O", url)
  
    const extension = MIMETYPE_EXT_MAP[mimetype] || opts.extension || 'bin'
  
    const decryptedPath = path.join(PATH_MEDIA_DECHIFFRE_STAGING, hachage_bytes + '.' + extension)
    debug("Fichier temporaire pour dechiffrage : %s", decryptedPath)

    downloadCacheFichier = {
      creation: new Date(),
      hachage_bytes,
      mimetype,
      decryptedPath,
      ready: null,    // Promise, resolve quand fichier prete (err sinon)
      clean: null,    // Fonction qui supprime le fichier dechiffre
      timeout: null,  // timeout qui va appeler cleanup(), doit etre resette/cleare si le fichier est utilise
      activerTimer: null,  // Activer timer
    }

    if(opts.metadata) downloadCacheFichier.metadata = opts.metadata

    downloadCache[hachage_bytes] = downloadCacheFichier
    
    downloadCacheFichier.clean = () => {
      delete downloadCache[hachage_bytes]
      return fsPromises.rm(decryptedPath)  // function pour nettoyer le fichier
    }
    
    downloadCacheFichier.activerTimer = delai => {
      delai = delai || EXPIRATION_DECHIFFRE

      if(downloadCacheFichier.timeout) clearTimeout(downloadCacheFichier.timeout)

      downloadCacheFichier.timeout = setTimeout(()=>{
        downloadCacheFichier.clean()
          .catch(err=>console.error("ERROR Erreur autoclean fichier dechiffre %s : %O", hachage_bytes, err))
      }, delai)
    }

    // Lancer le download (promise)
    downloadCacheFichier.ready = new Promise(async (resolve, reject) => {

      try {
        await fsPromises.stat(decryptedPath)
        debug("getDownloadCacheFichier Le fichier %s existe deja, on l'utilise", decryptedPath)
      } catch(err) {
        debug("getDownloadCacheFichier Err : %O", err)
        debug("Le fichier %s n'existe pas, on le download", decryptedPath)

        const decryptedWorkPath = decryptedPath + '.work'
        try {
          try {
            // Verifier si le fichier work existe
            debug("Verifier si le fichier %s existe", decryptedWorkPath)
            await fsPromises.stat(decryptedWorkPath)
            debug("Le fichier de download existe deja, on va attendre que le fichier soit libere")
            await new Promise((resolve, reject) => {
              try {
                const timeout = setTimeout(()=>{
                  // Abandonner le caching du fichier (echec)
                  delete downloadCache[hachage_bytes]
                  reject('timeout')
                }, 60000)
                fs.watchFile(decryptedWorkPath, (curr, prev) => {
                  debug("getDownloadCacheFichier curr : %O, prev: %O", curr, prev)
                  if(curr.mtimeMs === 0) {  // Fichier est supprime
                    clearTimeout(timeout)
                    return resolve()
                  }
                })
              } catch(err) {
                return reject(err)
              }
            })
          } catch(err) {
            debug("Le fichier de download n'existe pas, on commence un nouveau download")
            const reponseFichier = await axios({
              method: 'GET',
              url: url.href,
              httpsAgent: _httpsAgent,
              responseType: 'stream',
              timeout: 7500,
            })
    
            try {
              debug("Reponse download fichier : %O", reponseFichier.status)
              const writeStream = fs.createWriteStream(decryptedWorkPath)
              await dechiffrerStream(reponseFichier.data, cleFichier, writeStream)
    
              await fsPromises.rename(decryptedWorkPath, decryptedPath)
            } finally {
              fsPromises.rm(decryptedWorkPath).catch(err => { 
                // Ok, fichier avait deja ete traite
              })
            }
          } finally {
            fs.unwatchFile(decryptedWorkPath)
          }
  
        } catch(err) {
          debug("Erreur download fichier %s : %O", hachage_bytes, err)
          downloadCacheFichier.clean().catch(err=>debug("Erreur nettoyage fichier dechiffre %s : %O", decryptedPath, err))
          delete downloadCache[hachage_bytes]
          return reject(err)
        }

      } // fin catch, download/dechiffrage fichier

      try {

        // Creer un timer de cleanup automatique
        downloadCacheFichier.activerTimer()

        return resolve(decryptedPath)

      } catch(err) {
        debug("Erreur download fichier %s : %O", hachage_bytes, err)
        downloadCacheFichier.clean().catch(err=>debug("Erreur nettoyage fichier dechiffre %s : %O", decryptedPath, err))
        delete downloadCache[hachage_bytes]
        return reject(err)
      }
    })
  }

  downloadCacheFichier.lastAccess = new Date()

  if(downloadCacheFichier.timeout) {
    downloadCacheFichier.activerTimer()  // Reset le timer
  }

  return downloadCacheFichier
}

// Chiffre et upload un fichier cree localement
// Supprime les fichiers source et chiffres
async function stagerFichier(mq, pathFichier, clesPubliques, identificateurs_document) {
  // debug("Upload fichier traite : %s", pathFichier)
  // debug("CLES PUBLIQUES : %O", clesPubliques)
  const pathStr = pathFichier.path || pathFichier
  const cleanup = pathFichier.cleanup

  const uuidCorrelation = ''+uuidv4()
  const url = new URL(_urlServeurConsignation)
  try {
    const readStream = fs.createReadStream(pathStr)

    // Creer stream chiffrage
    const infoCertCa = {cert: mq.pki.caForge, fingerprint: mq.pki.fingerprintCa}
    const chiffrageStream = await creerOutputstreamChiffrage(clesPubliques, identificateurs_document, 'GrosFichiers', infoCertCa)
    readStream.pipe(chiffrageStream)

    await _storeConsignation.stagingStream(
      chiffrageStream, uuidCorrelation, 
      {TAILLE_SPLIT: UPLOAD_TAILLE_BLOCK, PATH_STAGING: PATH_MEDIA_STAGING}
    )

    // Signer commande maitre des cles
    var commandeMaitrecles = chiffrageStream.commandeMaitredescles

    const partition = commandeMaitrecles._partition
    delete commandeMaitrecles['_partition']
    commandeMaitrecles = await mq.pki.formatterMessage(
      commandeMaitrecles, DOMAINE_MAITREDESCLES, {action: ACTION_SAUVEGARDERCLE, partition})

    debug("Commande maitre des cles signee : %O", commandeMaitrecles)

    return {uuidCorrelation, commandeMaitrecles, hachage: commandeMaitrecles.hachage_bytes, taille: chiffrageStream.byteCount}

  } catch(e) {
    debug("Erreur upload fichier traite %s, DELETE tmp serveur. Erreur : %O", uuidCorrelation, e)
    url.pathname = '/fichiers_transfert/' + uuidCorrelation
    await _storeConsignation.stagingDelete(uuidCorrelation, {PATH_STAGING: PATH_MEDIA_STAGING})
    // await axios({method: 'DELETE', httpsAgent: _httpsAgent, url: url.href})
    throw e  // Rethrow
  } finally {
    // Supprimer fichier temporaire
    if(cleanup) cleanup()
  }

}

async function dechiffrerStream(stream, cleFichier, writeStream) {
  const decipherTransformStream = await decipherTransform(cleFichier.cleSymmetrique, {...cleFichier.metaCle})

  const promiseTraitement = new Promise((resolve, reject)=>{
    try {
      decipherTransformStream.on('end', ()=>{
        resolve()
      });
      decipherTransformStream.on('error', err=>{
        debug("dechiffrerStream Erreur : %O", err)
        reject(err)
      });
    } catch(err) {
      debug("Erreur preparation events readable : %O", err)
      reject(err)
    }
  })

  // Pipe dechiffrage -> writer
  decipherTransformStream.pipe(writeStream)

  // Pipe la reponse chiffree dans le dechiffreur
  stream.pipe(decipherTransformStream)

  return promiseTraitement
}

module.exports = {init, downloaderFichierProtege, stagerFichier, getCacheItem, getDownloadCacheFichier}
