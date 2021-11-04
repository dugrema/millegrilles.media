// Effectue le transfert avec le serveur de consignation
const debug = require('debug')('millegrilles:transfertConsignation')
const axios = require('axios')
const https = require('https')
const fs = require('fs')
const tmp = require('tmp-promise')
const {v4: uuidv4} = require('uuid')
const FormData = require('form-data')

const MIMETYPE_EXT_MAP = require('@dugrema/millegrilles.common/lib/mimetype_ext.json')

const {getDecipherPipe4fuuid, creerOutputstreamChiffrage} = require('./cryptoUtils')

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle'

var _urlServeurConsignation = null,
    _httpsAgent = null

function setUrlServeurConsignation(url, pki) {
  _urlServeurConsignation = url

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
  url.pathname = '/fichiers/' + hachage_bytes
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
async function uploaderFichierTraite(mq, pathFichier, clesPubliques, identificateurs_document) {
  debug("Upload fichier traite : %s", pathFichier)
  debug("CLES PUBLIQUES : %O", clesPubliques)
  const pathStr = pathFichier.path || pathFichier
  const cleanup = pathFichier.cleanup

  const uuidCorrelation = ''+uuidv4()
  const url = new URL(_urlServeurConsignation)
  try {
    const readStream = fs.createReadStream(pathStr)
    url.pathname = '/fichiers/' + uuidCorrelation + '/0'
    debug("Url upload fichier : %O", url)

    // Creer stream chiffrage
    const chiffrageStream = await creerOutputstreamChiffrage(clesPubliques, identificateurs_document, 'GrosFichiers')
    readStream.pipe(chiffrageStream)

    // const form = new FormData()
    // form.append("fichier", readStream, "fichier.jpg")

    const reponsePut = await axios({
      method: 'PUT',
      httpsAgent: _httpsAgent,
      url: url.href,
      // headers: form.getHeaders(),
      headers: {'content-type': 'application/stream'},
      data: chiffrageStream,
    })
    debug("Reponse put : %s, commande maitre des cles: %O", reponsePut.status, chiffrageStream.commandeMaitredescles)

    // Emettre POST avec info maitredescles
    url.pathname = '/fichiers/' + uuidCorrelation

    // Signer commande maitre des cles
    var commandeMaitrecles = chiffrageStream.commandeMaitredescles
    const partition = commandeMaitrecles._partition
    delete commandeMaitrecles['_partition']
    commandeMaitrecles = await mq.pki.formatterMessage(
      commandeMaitrecles, DOMAINE_MAITREDESCLES, {action: ACTION_SAUVEGARDERCLE, partition})

    debug("Commande maitre des cles signee : %O", commandeMaitrecles)

    const commandePost = { commandeMaitrecles }
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
    url.pathname = '/fichiers/' + uuidCorrelation
    //await axios({method: 'DELETE', httpsAgent: _httpsAgent, url: url.href})
    throw e  // Rethrow
  } finally {
    // Supprimer fichier temporaire
    if(cleanup) cleanup()
  }

}

function dechiffrerStream(stream, cleFichier, pathDestination) {
  const decipherPipe = getDecipherPipe4fuuid(
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

module.exports = {setUrlServeurConsignation, downloaderFichierProtege, uploaderFichierTraite}
