// Effectue le transfert avec le serveur de consignation
const debug = require('debug')('millegrilles:transfertConsignation')
const axios = require('axios')
const https = require('https')
const fs = require('fs')
const tmp = require('tmp-promise')
const {v4: uuidv4} = require('uuid')
// const FormData = require('form-data')
const path = require('path')

const MIMETYPE_EXT_MAP = require('@dugrema/millegrilles.utiljs/res/mimetype_ext.json')

const {getDecipherPipe4fuuid, creerOutputstreamChiffrage} = require('./cryptoUtils')

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle',
      UPLOAD_TAILLE_BLOCK = 5 * 1024 * 1024  // 5 MB,
      PATH_MEDIA_STAGING = '/var/opt/millegrilles/consignation/staging/media'

var _urlServeurConsignation = null,
    _httpsAgent = null,
    _storeConsignation = null

function init(urlServeurConsignation, amqpdao, storeConsignation) {
  debug("Initialiser transfertConsignation avec url %s", urlServeurConsignation)
  
  _urlServeurConsignation = new URL(''+urlServeurConsignation).href  // Cleanup URL

  _storeConsignation = storeConsignation

  const pki = amqpdao.pki
  // Configurer httpsAgent avec les certificats/cles
  _httpsAgent = new https.Agent({
    rejectUnauthorized: true,
    ca: pki.ca,
    cert: pki.chainePEM,
    key: pki.cle,
  })

}

// Download et dechiffre un fichier protege pour traitement local
async function downloaderFichierProtege(hachage_bytes, mimetype, cleFichier) {

  const url = new URL(_urlServeurConsignation)
  url.pathname = path.join('/fichiers_transfert', hachage_bytes)
  debug("Url download fichier : %O", url)

  const extension = MIMETYPE_EXT_MAP[mimetype] || '.bin'

  const tmpDecrypted = await tmp.file({ mode: 0o600, postfix: '.' + extension })
  const decryptedPath = tmpDecrypted.path
  debug("Fichier temporaire pour dechiffrage : %s", decryptedPath)

  const reponseFichier = await axios({
    method: 'GET',
    url: url.href,
    httpsAgent: _httpsAgent,
    responseType: 'stream',
    timeout: 7500,
  })

  debug("Reponse download fichier : %O", reponseFichier.status)
  await dechiffrerStream(reponseFichier.data, cleFichier, decryptedPath)

  return tmpDecrypted
}

// Chiffre et upload un fichier cree localement
// Supprime les fichiers source et chiffres
async function uploaderFichierTraite(mq, pathFichier, clesPubliques, identificateurs_document, transactionContenu, backingStore) {
  // debug("Upload fichier traite : %s", pathFichier)
  // debug("CLES PUBLIQUES : %O", clesPubliques)
  const pathStr = pathFichier.path || pathFichier
  const cleanup = pathFichier.cleanup

  const uuidCorrelation = ''+uuidv4()
  const url = new URL(_urlServeurConsignation)
  try {
    const readStream = fs.createReadStream(pathStr)
    //url.pathname = '/fichiers_transfert/' + uuidCorrelation + '/0'
    //debug("Url upload fichier : %O", url)

    // Creer stream chiffrage
    const infoCertCa = {cert: mq.pki.caForge, fingerprint: mq.pki.fingerprintCa}
    const chiffrageStream = await creerOutputstreamChiffrage(clesPubliques, identificateurs_document, 'GrosFichiers', infoCertCa)
    readStream.pipe(chiffrageStream)

    // const form = new FormData()
    // form.append("fichier", readStream, "fichier.jpg")

    let dataBuffer = [],
        position = 0

    await new Promise(async (resolve, reject)=>{

      chiffrageStream.on('data', async data => {
        dataBuffer = [...dataBuffer, ...data]

        if(dataBuffer.length > UPLOAD_TAILLE_BLOCK) {
          // Upload split
          try {
            chiffrageStream.pause()
            while(dataBuffer.length > UPLOAD_TAILLE_BLOCK) {
              const buffer = Uint8Array.from(dataBuffer.slice(0, UPLOAD_TAILLE_BLOCK))
              // debug("UPLOAD DATA %s bytes position %s", buffer.length, position)
              //await backingStore.stagingPut(buffer, uuidCorrelation, position, {PATH_STAGING: PATH_MEDIA_STAGING})
              await putAxios(url, uuidCorrelation, position, buffer)
              position += buffer.length
              dataBuffer = dataBuffer.slice(UPLOAD_TAILLE_BLOCK)
            }
            chiffrageStream.resume()
          } catch(err) {
            chiffrageStream.destroy([err])
            reject()
          }
        }
      })

      chiffrageStream.on('error', err => reject(err))

      chiffrageStream.on('end', async () => {
        try {
          if(dataBuffer.length > 0) {
            const buffer = Uint8Array.from(dataBuffer)
            // debug("DATA BUFFER UPLOAD position %s Uint8 Array: %O", position, buffer)
            await putAxios(url, uuidCorrelation, position, buffer)
            //await backingStore.stagingPut(buffer, uuidCorrelation, position, {PATH_STAGING: PATH_MEDIA_STAGING})
                // uuidCorrelation, position, buffer
          }
          resolve()
        } catch(err) {
          reject(err)
        }
      })

      chiffrageStream.read()  // Lancer la lecture

      // const reponsePut = await axios({
      //   method: 'PUT',
      //   httpsAgent: _httpsAgent,
      //   url: url.href,
      //   // headers: form.getHeaders(),
      //   headers: {'content-type': 'application/stream'},
      //   data: chiffrageStream,
      // })
      // debug("Reponse put : %s, commande maitre des cles: %O", reponsePut.status, chiffrageStream.commandeMaitredescles)
    })

    // Emettre POST avec info maitredescles
    url.pathname = '/fichiers_transfert/' + uuidCorrelation

    // Signer commande maitre des cles
    var commandeMaitrecles = chiffrageStream.commandeMaitredescles
    const partition = commandeMaitrecles._partition
    delete commandeMaitrecles['_partition']
    commandeMaitrecles = await mq.pki.formatterMessage(
      commandeMaitrecles, DOMAINE_MAITREDESCLES, {action: ACTION_SAUVEGARDERCLE, partition})

    debug("Commande maitre des cles signee : %O", commandeMaitrecles)

    const commandePost = { cles: commandeMaitrecles }
    //await backingStore.stagingReady(mq, transactionContenu, commandeMaitrecles, correlation, {PATH_STAGING: PATH_MEDIA_STAGING})
      //uuidCorrelation, commandeMaitrecles, {PATH_STAGING: PATH_MEDIA_STAGING})

    const reponsePost = await axios({
      method: 'POST',
      httpsAgent: _httpsAgent,
      headers: { 'content-type': 'application/json;charset=utf-8' },
      url: url.href,
      data: commandePost,
    })
    debug("Reponse POST : %s", reponsePost.status)

    return {hachage: commandeMaitrecles.hachage_bytes, taille: chiffrageStream.byteCount}

  } catch(e) {
    debug("Erreur upload fichier traite %s, DELETE tmp serveur. Erreur : %O", uuidCorrelation, e)
    url.pathname = '/fichiers_transfert/' + uuidCorrelation
    //await backingStore.stagingDelete(uuidCorrelation, {PATH_STAGING: PATH_MEDIA_STAGING})
    await axios({method: 'DELETE', httpsAgent: _httpsAgent, url: url.href})
    throw e  // Rethrow
  } finally {
    // Supprimer fichier temporaire
    if(cleanup) cleanup()
  }

}

async function putAxios(url, uuidCorrelation, position, dataBuffer) {
  // Emettre POST avec info maitredescles
  const urlPosition = new URL(url.href)
  urlPosition.pathname = path.join('/fichiers_transfert', uuidCorrelation, ''+position)

  debug("putAxios url %s taille %s", urlPosition, dataBuffer.length)

  const reponsePut = await axios({
    method: 'PUT',
    httpsAgent: _httpsAgent,
    url: urlPosition.href,
    headers: {'content-type': 'application/stream'},
    data: dataBuffer,
  })

  debug("Reponse put %s : %s", urlPosition.href, reponsePut.status)
}

async function dechiffrerStream(stream, cleFichier, pathDestination) {
  const decipherPipe = await getDecipherPipe4fuuid(
    cleFichier.cleSymmetrique,
    cleFichier.metaCle.iv,
    {tag: cleFichier.metaCle.tag}
  )

  // Pipe la reponse chiffree dans le dechiffreur
  stream.pipe(decipherPipe.reader)

  let writeStream = fs.createWriteStream(pathDestination)
  const promiseTraitement = new Promise((resolve, reject)=>{
    decipherPipe.writer.on('end', ()=>{
      resolve()
    });
    decipherPipe.writer.on('error', err=>{
      debug("Erreur : %O", err)
      reject(err)
    });
  })

  // Pipe dechiffrage -> writer
  decipherPipe.writer.pipe(writeStream)

  return promiseTraitement
}

module.exports = {init, downloaderFichierProtege, uploaderFichierTraite}
