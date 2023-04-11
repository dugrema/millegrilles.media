// Prepare le fichier a uploader
const debug = require('debug')('transfertConsignation')
const fs = require('fs')
const fsPromises = require('fs/promises')
const {v4: uuidv4} = require('uuid')
const path = require('path')
const readdirp = require('readdirp')

const { creerOutputstreamChiffrageParSecret } = require('./cryptoUtils')

const PATH_MEDIA_STAGING = '/var/opt/millegrilles/consignation/staging/media',
      DIR_MEDIA_PREPARATION = 'prep',
      PATH_MEDIA_DECHIFFRE_STAGING = '/var/opt/millegrilles/consignation/staging/mediaDechiffre',
      FILE_TRANSACTION_CONTENU = 'transactionContenu.json',
      EXPIRATION_PREP = 3_600_000 * 8

var _fichiersTransfert = null

function init(fichiersTransfert) {
  debug("Initialiser transfertConsignation")

  _fichiersTransfert = fichiersTransfert

  // Preparer repertoires
  fsPromises.mkdir(PATH_MEDIA_STAGING, {recursive: true}).catch(err=>console.error("ERROR Erreur mkdir %s", PATH_MEDIA_STAGING))
  fsPromises.mkdir(PATH_MEDIA_DECHIFFRE_STAGING, {recursive: true}).catch(err=>console.error("ERROR Erreur mkdir %s", PATH_MEDIA_DECHIFFRE_STAGING))

  // Faire un entretien initial, ceduler a toutes les 30 minutes
  entretien().catch(err=>console.error(new Date() + " transfertConsignation.entretien ERROR ", err))
  setInterval(entretien, 60_000 * 30)
}

/** Supprime les batch abandonnees */
async function entretien() {

  try {
    const pathPrep = path.join(PATH_MEDIA_STAGING, DIR_MEDIA_PREPARATION)
    const dateExpiration = new Date().getTime() - EXPIRATION_PREP
    debug("entretien Date expiration prep : ", dateExpiration)

    for await (const entry of readdirp(pathPrep, {type: 'directories', alwaysStat: true, depth: 1})) {
      const { fullPath, stats } = entry
      const { mtimeMs } = stats
      if(mtimeMs < dateExpiration) {
        try {
          await fsPromises.rm(fullPath, {recursive: true})
          debug("Suppression entry fichier work : %s, mod time %s", fullPath, mtimeMs)
        } catch(err) {
          console.error(new Date() + " transfertConsignation.entretien ERROR Suppression %s : %O", fullPath, err)
        }
      } else {
        debug("Entry fichier work : %s, mod time %s", fullPath, mtimeMs)
      }
    }
  } catch(err) {
    console.error(new Date() + " transfertConsignation.entretien ERROR ", err)
  }
}

// Chiffre et upload un fichier cree localement
// Supprime les fichiers source et chiffres
async function stagerFichier(fichier, cleSecrete, opts) {
  opts = opts || {}
  const pathStr = fichier.path || fichier
  const cleanup = fichier.cleanup
  const readStream = fs.createReadStream(pathStr)
  try {
    return await stagerStream(readStream, cleSecrete, opts)
  } finally {
    // Supprimer fichier temporaire
    if(cleanup) cleanup()
  }
}

async function stagerStream(readStream, cleSecrete, opts) {
  opts = opts || {}

  const generateurTransaction = opts.generateurTransaction

  // Generer identificateurs, paths
  const uuidCorrelation = ''+uuidv4()
  const batchId = uuidCorrelation  // On a juste besoin d'un batchId unique pour le repertoire
  const pathBatch = path.join(PATH_MEDIA_STAGING, DIR_MEDIA_PREPARATION, batchId)
  const pathDirFichier = path.join(pathBatch, uuidCorrelation)
  const pathFichierTmp = path.join(pathDirFichier, 'output.tmp')

  try {
    // Preparer repertoire de batch/fichier
    await fsPromises.mkdir(pathDirFichier, {recursive: true})
    
    // Creer stream chiffrage
    const chiffrageStream = await creerOutputstreamChiffrageParSecret(cleSecrete)
    readStream.pipe(chiffrageStream)

    const writeStream = fs.createWriteStream(pathFichierTmp)
    const promiseWrite = new Promise((resolve, reject)=>{
      writeStream.on('close', resolve)
      writeStream.on('error', reject)
    })
    chiffrageStream.pipe(writeStream)
    await promiseWrite

    // Signer commande maitre des cles
    // {taille, hachage, header, format} = resultatChiffrage
    const resultatChiffrage = chiffrageStream.resultatChiffrage || {}
    debug("Fin Chiffrage Stream : ", resultatChiffrage)

    // Renommer fichier en utilisant son hachage (fuuid)
    const pathFichierFuuid = path.join(pathDirFichier, resultatChiffrage.hachage)
    await fsPromises.rename(pathFichierTmp, pathFichierFuuid)

    // Creer fichier etat.json (requis pour transfert de fichier)
    await creerEtat(pathDirFichier, resultatChiffrage.hachage)

    let transaction = null
    if(generateurTransaction) {
      const pathTransaction = path.join(pathDirFichier, FILE_TRANSACTION_CONTENU)
      transaction = await generateurTransaction(resultatChiffrage)
      await fsPromises.writeFile(pathTransaction, JSON.stringify(transaction))
    }

    // Declencher le transfert
    await _fichiersTransfert.takeTransfertBatch(batchId, pathBatch)
    await _fichiersTransfert.ajouterFichierConsignation(batchId)

    return { batchId, uuidCorrelation, ...resultatChiffrage, transaction }

  } catch(e) {
    debug("Erreur staging fichier traite %s, DELETE tmp. Erreur : %O", uuidCorrelation, e)
    await fsPromises.mkdir(pathBatch, {recursive: true})
    throw e  // Rethrow
  }

}

async function creerEtat(pathFichier, hachage) {
  const etat = {
    hachage,
    created: new Date().getTime(),
    lastProcessed: new Date().getTime(),
    retryCount: 0,
  }

  const pathEtat = path.join(pathFichier, 'etat.json')
  await fsPromises.writeFile(pathEtat, JSON.stringify(etat))
}

function getIdConsignation() {
  if(_fichiersTransfert) return _fichiersTransfert.getIdConsignation()
}

function ajouterListener(listener) {
  _fichiersTransfert.ajouterListener(listener)
}

function retirerListener(listener) {
  _fichiersTransfert.retirerListener
}

module.exports = { init, stagerFichier, stagerStream, getIdConsignation, ajouterListener, retirerListener }
