const debug = require('debug')('media:traitementMedia')
const fsPromises = require('fs/promises')
const pdfParse = require('pdf-parse')
const axios = require('axios')

const transformationImages = require('./transformationImages')

function genererPreviewImage(mq, fichierDechiffre, message, opts) {
  if(!opts) opts = {}
  const fctConversion = traiterImage
  return _genererPreview(mq, fichierDechiffre, message, opts, fctConversion)
}

function genererPreviewVideo(mq, fichierDechiffre, message, opts) {
  if(!opts) opts = {}
  const fctConversion = traiterVideo
  return _genererPreview(mq, fichierDechiffre, message, opts, fctConversion)
}

async function _genererPreview(mq, fichierDechiffre, message, opts, fctConversion) {

  const uuidDocument = message.tuuid,
        fuuid = message.fuuid

  debug("Message genererPreviewImage tuuid:%s/fuuid:%s, chiffre:%s", uuidDocument, fuuid);

  debug("Fichier (dechiffre) %s pour generer preview image", fichierDechiffre)
  var resultatConversion = await fctConversion(
    fichierDechiffre, {...opts, mq})
  debug("Resultat conversion : %O", resultatConversion)

  return resultatConversion

}

// Extraction de thumbnail et preview pour images
//   - pathImageSrc : path de l'image source dechiffree
function traiterImage(pathImageSrc, opts) {
  return transformationImages.genererConversionsImage(pathImageSrc, opts)
}

// Extraction de thumbnail, preview et recodage des videos pour le web
async function traiterVideo(pathImageSrc, opts) {
  const resultat = await transformationImages.genererPosterVideo(pathImageSrc, opts)
  return resultat
}

async function indexerDocument(mq, fichierDechiffre, message, optsConversion) {
  const documentFichier = message.doc
  const {urlServeurIndex} = optsConversion
  const {fuuid, tuuid} = message

  // const fichierSrcTmp = await dechiffrerTemporaire(pathConsignation, fuuid, 'pdf', cleSymmetrique, metaCle)
  debug("indexerDocument fichier tmp : %s", fichierDechiffre)
  const dataBuffer = await fsPromises.readFile(fichierDechiffre)
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

}

module.exports = {
  genererPreviewImage, 
  genererPreviewVideo, 
  indexerDocument,
}
