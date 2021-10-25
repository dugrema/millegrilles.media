const debug = require('debug')('millegrilles:fichiers:traitementMedia')
const tmp = require('tmp-promise')
const {v1: uuidv1} = require('uuid')
const path = require('path')
const fs = require('fs')
const fsPromises = require('fs/promises')
const pdfParse = require('pdf-parse')
const axios = require('axios')

const { decrypterGCM } = require('./cryptoUtils.js')
const { creerCipher, creerDecipher } = require('@dugrema/millegrilles.common/lib/chiffrage')
const { hacher } = require('@dugrema/millegrilles.common/lib/hachage')
const transformationImages = require('./transformationImages')
const MIMETYPE_EXT_MAP = require('@dugrema/millegrilles.common/lib/mimetype_ext.json')

const { calculerHachageFichier } = require('./utilitairesHachage')

function genererPreviewImage(mq, pathConsignation, message, opts) {
  if(!opts) opts = {}
  const fctConversion = traiterImage
  return _genererPreview(mq, pathConsignation, message, opts, fctConversion)
}

function genererPreviewVideo(mq, pathConsignation, message, opts) {
  if(!opts) opts = {}
  const fctConversion = traiterVideo
  return _genererPreview(mq, pathConsignation, message, opts, fctConversion)
}

async function _genererPreview(mq, pathConsignation, message, opts, fctConversion) {

  const uuidDocument = message.tuuid,
        fuuid = message.fuuid,
        mimetype = message.mimetype

  // Determiner extension fichier original en fonction du mimetype
  const extension = MIMETYPE_EXT_MAP[mimetype] || '.mov'

  debug("Message genererPreviewImage uuid:%s/fuuid:%s, chiffre:%s", uuidDocument, fuuid, opts.iv?true:false);
  // debug("Parametres dechiffrage : %O", opts)

  var fichierSource = null, fichierDestination = null,
      fichierSrcTmp = null, fichierDstTmp = null,
      pathPreviewImageTmp = null
  try {

    // Trouver fichier original crypte    const pathFichierChiffre = this.pathConsignation.trouverPathLocal(fuuid, true);
    fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, extension, opts.cleSymmetrique, opts.metaCle)
    fichierSource = fichierSrcTmp.path

    debug("Fichier (dechiffre) %s pour generer preview image", fichierSource)
    var resultatConversion = await fctConversion(
      fichierSource, {...opts, mq, chiffrerTemporaire, deplacerVersStorage: _deplacerVersStorage, pathConsignation, fuuid})
    debug("Resultat conversion : %O", resultatConversion)

    return resultatConversion

  } finally {
    // Effacer le fichier temporaire
    const fichiersTmp = [fichierSrcTmp, fichierDstTmp, pathPreviewImageTmp]
    fichiersTmp.forEach(item=>{
      try {
        if(item) {
          debug("Nettoyage fichier tmp %s", item.path)
          item.cleanup()
        }
      } catch(err) {
        console.error("Erreur suppression fichier temp %s: %O", item.path, err)
      }
    })
  }

}

async function dechiffrerTemporaire(pathConsignation, fuuid, extension, cleSymmetrique, metaCle, opts) {
  opts = opts || {}

  const pathFichierChiffre = pathConsignation.trouverPathLocal(fuuid, true);

  const tmpDecrypted = await tmp.file({ mode: 0o600, postfix: '.' + extension })
  const decryptedPath = tmpDecrypted.path

  // Decrypter
  await decrypterGCM(pathFichierChiffre, decryptedPath, cleSymmetrique, metaCle.iv, metaCle.tag, opts)

  return tmpDecrypted
}

async function chiffrerTemporaire(mq, fichierSrc, fichierDst, clesPubliques, opts) {
  opts = opts || {}

  const writeStream = fs.createWriteStream(fichierDst);

  const identificateurs_document = opts.identificateurs_document || {}

  // Creer cipher
  debug("Cles publiques pour cipher : %O", clesPubliques)
  const cipher = await mq.pki.creerCipherChiffrageAsymmetrique(
    clesPubliques, 'GrosFichiers', identificateurs_document
  )

  return new Promise((resolve, reject)=>{
    const s = fs.ReadStream(fichierSrc)
    var tailleFichier = 0
    s.on('data', data => {
      const contenuCrypte = cipher.update(data);
      tailleFichier += contenuCrypte.length
      writeStream.write(contenuCrypte)
    })
    s.on('end', async _ => {
      const informationChiffrage = await cipher.finish()
      console.debug("Information chiffrage fichier : %O", informationChiffrage)
      writeStream.close()
      return resolve({
        tailleFichier,
        meta: informationChiffrage.meta,
        commandeMaitreCles: informationChiffrage.commandeMaitreCles
      })
    })
  })

}

async function transcoderVideo(mq, pathConsignation, message, opts) {
  opts = opts || {}

  const {cleSymmetrique, metaCle, clesPubliques} = opts

  const fuuid = message.fuuid;
  const extension = message.extension;
  // const securite = message.securite;

  // Requete info fichier
  const infoFichier = await mq.transmettreRequete('GrosFichiers.documentsParFuuid', {fuuid})
  debug("Recue detail fichiers pour transcodage : %O", infoFichier)
  const infoVersion = infoFichier.versions[fuuid]

  let mimetype = infoVersion.mimetype.split('/')[0];
  if(mimetype !== 'video') {
    throw new Error("Erreur, type n'est pas video: " + mimetype)
  }

  mq.emettreEvenement({fuuid, progres: 1}, 'evenement.fichiers.transcodageEnCours')
  var fichierSrcTmp = '', fichierDstTmp = '', pathVideoDestTmp = ''
  try {
    fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, 'vid', cleSymmetrique, metaCle)
    fichierDstTmp = await tmp.file({ mode: 0o600, postfix: '.mp4'})
    var fichierSource = fichierSrcTmp.path
    debug("Fichier transcodage video, source dechiffree : %s", fichierSource)

    debug("Decryptage video, re-encoder en MP4, source %s sous %s", fuuid, fichierDstTmp.path);
    mq.emettreEvenement({fuuid, progres: 5}, 'evenement.fichiers.transcodageEnCours')
    var resultatMp4 = await transformationImages.genererVideoMp4_480p(fichierSource, fichierDstTmp.path);
    mq.emettreEvenement({fuuid, progres: 95}, 'evenement.fichiers.transcodageEnCours')

    // Chiffrer le video transcode
    // const fuuidVideo480p = uuidv1()
    // const pathVideo480p = pathConsignation.trouverPathLocal(fuuidVideo480p, true)

    pathVideoDestTmp = await tmp.file({
      mode: 0o600,
      postfix: '.mp4',
      // dir: pathConsignation.consignationPathUploadStaging, // Meme drive que storage pour rename
    })

    await new Promise((resolve, reject)=>{
      fs.mkdir(path.dirname(pathVideoDestTmp.path), {recursive: true}, e => {
        if(e) return reject(e)
        resolve()
      })
    })

    debug("Chiffrer le fichier transcode (%s) vers %s", fichierDstTmp.path, pathVideoDestTmp.path)
    const resultatChiffrage = await chiffrerTemporaire(mq, fichierDstTmp.path, pathVideoDestTmp.path, clesPubliques)
    debug("Resultat chiffrage fichier transcode : %O", resultatChiffrage)

    // Calculer hachage
    // const hachage = await calculerHachageFichier(pathVideo480p)
    // debug("Hachage nouveau fichier transcode : %s", hachage)

    // Deplacer le fichier vers le repertoire de storage
    await _deplacerVersStorage(pathConsignation, resultatChiffrage, pathVideoDestTmp)
    const hachage = resultatChiffrage.meta.hachage_bytes

    const resultat = {
      uuid: infoFichier.uuid,
      height: resultatMp4.height,
      bitrate: resultatMp4.bitrate,
      fuuidVideo: hachage,
      mimetypeVideo: 'video/mp4',
      hachage,
      ...resultatChiffrage,
    }
    // resultat.fuuidVideo480p = fuuidVideo480p
    // resultat.mimetypeVideo480p = 'video/mp4'
    // resultat.tailleVideo480p = resultatMp4.tailleFichier
    // resultat.sha256Video480p = resultatMp4.sha256
    // resultat.hachage = hachage

    console.debug("Fichier converti : %O", resultat);
    // console.debug(convertedFile);
    // this._transmettreTransactionVideoTranscode(fuuid, resultat)
    return resultat
  } catch(err) {
    console.error("Erreur conversion video");
    console.error(err);
  } finally {
    // S'assurer que les fichiers temporaires sont supprimes
    [fichierSrcTmp, fichierDstTmp, pathVideoDestTmp].forEach(fichier=>{
      console.debug("Cleanup fichier tmp : %O", fichier)
      fichier.cleanup()
    })
  }

}

function _transmettreTransactionPreview(mq, transaction) {
  const domaine = 'GrosFichiers', action = 'associerThumbnail'
  mq.transmettreCommande(domaineTransaction, transaction, {action});
}

function _transmettreTransactionVideoTranscode(mq, transaction) {
  const domaineTransaction = 'GrosFichiers', action = 'associerVideo'
  mq.transmettreTransactionFormattee(domaineTransaction, transaction, {action});
}

// Extraction de thumbnail et preview pour images
//   - pathImageSrc : path de l'image source dechiffree
function traiterImage(pathImageSrc, opts) {
  return transformationImages.genererConversionsImage(pathImageSrc, opts)
}

// Extraction de thumbnail, preview et recodage des videos pour le web
function traiterVideo(pathImageSrc, opts) {
  return transformationImages.genererPosterVideo(pathImageSrc, opts)
}

async function indexerDocument(mq, pathConsignation, message, optsConversion) {
  const documentFichier = message.doc
  const {urlServeurIndex, cleSymmetrique, metaCle} = optsConversion
  const {fuuid, tuuid} = message

  const fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, 'pdf', cleSymmetrique, metaCle)
  try {
    debug("indexerDocument fichier tmp : %O", fichierSrcTmp)
    const dataBuffer = await fsPromises.readFile(fichierSrcTmp.path)
    const data = await pdfParse(dataBuffer)

    const docIndex = {
      ...documentFichier,
      contenu: data.text,
    }

    debug("indexerDocument contenu : %O", docIndex)

    const rep = await axios({
      method: 'PUT',
      url: urlServeurIndex + '/grosfichiers/_doc/' + fuuid,
      data: docIndex,
    })

    debug("Reponse indexation fichier %s, %d = %O", fuuid, rep.status, rep.data)
    if([200, 201].includes(rep.status)) {
      // Emettre evenement pour indiquer la date de l'indexation
      const info = {
        tuuid, fuuid,
        dateModification: documentFichier.modification,
        result: rep.data.result,
      }
      mq.emettreEvenement(info, 'evenement.fichiers.indexationFichier')
        .catch(err=>{
          console.error('ERROR traitementMedia.indexerDocument Erreur emission evenement confirmation %O', err)
        })
    } else {
      console.error("ERROR traitementMedia.indexerDocument traitement indexation PDF pour fichier %s %O", rep.status, rep.data)
    }

  } finally {
    fichierSrcTmp.cleanup()
  }
}

async function _deplacerVersStorage(pathConsignation, resultatChiffrage, pathPreviewImageTmp) {
  const hachage = resultatChiffrage.meta.hachage_bytes
  const pathPreviewImage = pathConsignation.trouverPathLocal(hachage)

  // Makedir consignation
  const pathRepertoire = path.dirname(pathPreviewImage)
  await new Promise((resolve, reject) => {
    fs.mkdir(pathRepertoire, { recursive: true }, async (err)=>{
      if(err) return reject(err)
      resolve()
    })
  })

  // Changer extension fichier destination
  debug("Renommer fichier dest pour ajouter extension : %s", pathPreviewImage)
  try {
    await fsPromises.rename(pathPreviewImageTmp.path, pathPreviewImage)
  } catch(err) {
    console.warn("WARN traitementMedia._deplacerVersStorage: move (rename) echec, on fait copy")
    await fsPromises.copyFile(pathPreviewImageTmp.path, pathPreviewImage)
    await fsPromises.unlink(pathPreviewImageTmp.path)
  }
}

module.exports = {
  genererPreviewImage, genererPreviewVideo, transcoderVideo, dechiffrerTemporaire, indexerDocument,
}
