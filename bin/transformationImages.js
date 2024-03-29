const debug = require('debug')('media:transformationImages')
const fs = require('fs')
const path = require('path')
const tmp = require('tmp-promise')
const im = require('@dugrema/imagemagick')
const FFmpeg = require('fluent-ffmpeg')

const {chiffrerMemoireSecret} = require('./cryptoUtils')
const {probeVideo} = require('./transformationsVideo')

async function genererPosterVideo(sourcePath, opts) {
  // Preparer fichier destination decrypte
  // Aussi preparer un fichier tmp pour le thumbnail
  const {mq, clesPubliques, fuuid, cleSecrete} = opts

  const tmpFile = await tmp.file({ mode: 0o600, postfix: '.jpg' })

  try {
    const snapshotPath = tmpFile.path

    const probeVideoResult = await probeVideo(sourcePath, {utiliserTailleOriginale: true})
    debug("genererPosterVideo Probe video : %O", probeVideoResult)

    // Extraire une image du video
    const metadata = await genererSnapshotVideoPromise(sourcePath, snapshotPath)
    debug("Metadata video : %O", metadata)

    // Prendre le snapshot genere et creer les images converties en formats
    const {metadataImage, conversions} = await determinerConversionsPoster(snapshotPath)
    debug("Information de conversion d'images du video : medataImage %O\nconversions %O", metadataImage, conversions)

    const sourceImage = metadataImage.filename || sourcePath

    // Effectuer les conversions pour tous les formats
    const promisesConversions = await convertir(
      mq,
      cleSecrete,
      sourceImage,
      fuuid,
      conversions
    )

    // Recuperer l'information de chaque image convertie
    const resultatConversions = await Promise.all(promisesConversions)
    debug("Information de conversions completees : %O", resultatConversions)

    return {
      probeVideo: probeVideoResult, conversions: resultatConversions
    }

  } catch(err) {
    console.error("ERROR transformationImages.genererPosterVideo Erreur creation thumbnail/poster video : %O", err)
  }

}

async function genererConversionsImage(sourcePath, opts) {
  debug("genererConversionsImage avec %s", sourcePath)
  const {
    mq, clesPubliques, fuuid, cleSecrete,
  } = opts

  //debug("Thumbnail genere en base64\n%s", b64Thumbnail)
  const {metadataImage, nbFrames, conversions} = await determinerConversionsImages(sourcePath)
  debug("Information de conversion d'images : %O", conversions)

  // Effectuer les conversions pour tous les formats
  const promisesConversions = await convertir(
    mq,
    cleSecrete,
    sourcePath,
    fuuid,
    conversions
  )

  // Recuperer l'information de chaque image convertie
  // Note : un echec sur une promise indique que la cle de chiffrage
  //        n'a PAS ete conservee.
  const resultatsConversions = await Promise.all(promisesConversions)
  debug("Information de conversions completees : %O", resultatsConversions)

  return {metadataImage, nbFrames, conversions: resultatsConversions}
}

async function convertir(
  mq,
  cleSecrete,
  sourcePath,
  fuuid,
  conversions
) {
  // NOTE : bloque sur les conversions mais retourne une liste de promises
  //        qui servent a confirmer la reception des cles de chiffrage de chaque
  //        image convertie.
  const resultatsConversions = []

  for(let cle in conversions) {
    const cp = conversions[cle]
    debug("Executer conversion %s, %O", cle, cp)
    // Creer fichier temporaire avec la bonne extension
    const fichierTmp = await tmp.file({ mode: 0o600, postfix: '.' + cp.ext })
    let paramsConversion = [sourcePath+'[0]', ...cp.params, fichierTmp.path]
    try {
      await _imConvertPromise(paramsConversion)
    } catch(err) {
      console.error("ERROR transfomationImages.convertir fichierTmp %O", err)
      if(cp.paramsFallback) {
        paramsConversion = [sourcePath+'[0]', ...cp.paramsFallback, fichierTmp.path]
        try {
          await _imConvertPromise(paramsConversion)
        } catch(err) {
          console.error("ERROR transfomationImages.convertir Echec fallback %O", err)
          try {
            fichierTmp.cleanup()  // Supprimer fichier tmp non chiffre
          } catch(err) {console.error("ERROR transfomationImages.convertir fichierTmp %O", err)}
        }
      } else {
        try {
          fichierTmp.cleanup()  // Supprimer fichier tmp non chiffre
        } catch(err) {console.error("ERROR transfomationImages.convertir fichierTmp %O", err)}
      }
      throw err  // Abandonner conversion
    }

    // Creer promise pour continuer le traitement de chiffrage
    // const fichierChiffreTmp = await tmp.file({ mode: 0o600, postfix: '.mgs3' })
    const promiseChiffrage = readIdentify(fichierTmp.path).then(async metaConversion=>{
      // Recuperer information image convertie
      debug("Information meta image convertie params %O : %O", cp, metaConversion)

      var resultat = {
        cle,
        informationImage: {
          width: metaConversion.width,
          height: metaConversion.height,
          mimetype: metaConversion['mime type'],
          resolution: cp.resolution,
        }
      }

      if(cle === 'thumb') {
        // Le thumbnail est extrait et conserve dans la base de donnees
        // Chiffrer le resultat, conserver information pour transactions maitre des cles
        const resultatChiffrage = await chiffrerMemoireSecret(fichierTmp.path, cleSecrete, { base64: true })

        // Supprimer fichiers tmp
        fichierTmp.cleanup()

        resultat = {
          ...resultat,
          informationImage: {
            ...resultat.informationImage,
            hachage: resultatChiffrage.hachage,
            taille: resultatChiffrage.tailleFichier,
            data_chiffre: resultatChiffrage.data,
            header: resultatChiffrage.meta.header,
            format: resultatChiffrage.meta.format,
          },
        }

      }
      else {
        // Copier path du fichier temporaire, il va etre chiffrer a l'upload
        resultat.fichierTmp = fichierTmp
      }

      debug("Resultat preparation image : %O", resultat)
      return resultat

    })
    .catch(err => {
      throw err
    })

    resultatsConversions.push(promiseChiffrage)
  }

  return resultatsConversions
}

async function determinerConversionsImages(sourcePath) {
  let ratioInverse = false,
      operationResize = '>',
      quality = '86',
      valRef = null

  let metadataImage = null, nbFrames = null, estPdf = null
  try {
    nbFrames = await readIdentifyFrames(sourcePath)
    debug("Image nb frames : %d", nbFrames)

    metadataImage = await readIdentify(sourcePath)
    debug("Metadata image chargee : %O", metadataImage)

    const {width, height} = metadataImage
    const mimetype = metadataImage['mime type']
    quality = metadataImage.quality
    if(quality && quality < 0.86) {
      quality = ''+Math.round(quality * 100)
    } else {
      quality = '86'
    }

    // Flag si c'est un PDF - conversions plus simples
    estPdf = mimetype === 'application/pdf'

    let ratio = null
    if(width < height) {
      // Ratio inverse
      ratioInverse = true
      ratio = width / height
      if(ratio < (9/16)) {
        // Image tres longue, on va inverser le resize (^ plutot que >) pour garder suffisamment de detail
        operationResize = '^'
      }
    }

    // Definir valeur de reference pour la resolution (selon le ratio)
    valRef = ratioInverse?width:height

    debug("Image mimetype %s, resolution %s, ratio %s, quality %s", mimetype, valRef, ratio, quality)

    if(!valRef) {
      debug("determinerConversionsImages Erreur detection resolution image - on met 720 par defaut.\n%s", JSON.stringify(metadataImage))
      valRef = 720
    }

  } catch(err) {
    debug("Erreur preparation image, aucune meta-information : %O", err)
  }

  const conversions = {
    // Thumbnail : L'image est ramenee sur 128px, et croppee au milieu pour ratio 1:1
    'thumb': {
      ext: 'jpg', resolution: 128,
      params: ['-strip', '-resize', '128x128^', '-gravity', 'center', '-extent', '128x128', '-quality', '25']
    },
    // Poster, utilise pour afficher dans un coin d'ecran/preview
    'small': {
      ext: 'jpg', resolution: 200,
      params: ['-strip', '-resize', '200x200^', '-gravity', 'center', '-extent', '200x200', '-quality', '80']
    },
  }

  // Grandeur standard la plus pres de l'originale
  if(estPdf) {
    // Ajouter format standard d'exportation des PDF. Resolution par defaut (auto) est 612x792 de large.
    conversions['image/webp;612'] = {
      ext: 'webp',
      resolution: 612,
      params: ['-strip', '-quality', quality],
      paramsFallback: ['-strip', '-resize', '612x792', '-quality', quality],
    }

    // Changer gravity pour north (haut de la page, avec titre)
    conversions['thumb'] = {ext: 'jpg', resolution: 128, params: ['-strip', '-resize', '128x128^', '-gravity', 'north', '-extent', '128x128', '-quality', '25']}
  } else {
    // Image standard
    // if(valRef >= 2160) {
    //   conversions['image/webp;2160'] = {ext: 'webp', resolution: 2160, params: ['-strip', '-resize', ratioInverse?'2160x3840'+operationResize:'3840x2160'+operationResize, '-quality', quality]}
    // } else 
    if(valRef >= 1440) {
      conversions['image/webp;1440'] = {ext: 'webp', resolution: 1440, params: ['-strip', '-resize', ratioInverse?'1440x2560'+operationResize:'2560x1440'+operationResize, '-quality', quality]}
    } else if(valRef >= 1080) {
      conversions['image/webp;1080'] = {ext: 'webp', resolution: 1080, params: ['-strip', '-resize', ratioInverse?'1080x1920'+operationResize:'1920x1080'+operationResize, '-quality', quality]}
    } else if(valRef > 200) {
      // L'image est plus petite que 1080, generer une version avec grandeur originale.
      // Couvre les case entre 200 et 1080.
      let valAutre = Math.floor(valRef * 16 / 9)
      conversions['image/webp;' + valRef] = {ext: 'webp', resolution: valRef, params: ['-strip', '-resize', ratioInverse?''+valRef+'x'+valAutre+operationResize:''+valAutre+'x'+valRef+operationResize, '-quality', quality]}
    }

    // if(valRef >= 720) {
    //   conversions['image/webp;720'] = {ext: 'webp', resolution: 720, params: ['-strip', '-resize', ratioInverse?'720x1280'+operationResize:'1280x720'+operationResize, '-quality', quality]}
    // }

    // Default fallback
    // if(valRef >= 480) {
    //   // conversions['image/webp;480'] = {ext: 'webp', resolution: 480, params: ['-strip', '-resize', ratioInverse?'480x854'+operationResize:'854x480'+operationResize, '-quality', quality]}
    //   conversions['image/jpeg;480'] = {ext: 'jpg', resolution: 480, params: ['-strip', '-resize', ratioInverse?'480x854'+operationResize:'854x480'+operationResize, '-quality', quality]}
    // }
  }

  return {metadataImage, nbFrames, conversions}
}

async function determinerConversionsPoster(sourcePath, opts) {
  opts = opts || {}

  let ratioInverse = false,
      operationResize = '>',
      quality = '60',
      valRef = null,
      valAutre = null

  let metadataImage = null, nbFrames = null
  try {
    nbFrames = await readIdentifyFrames(sourcePath)
    debug("Image nb frames : %d", nbFrames)

    metadataImage = await readIdentify(sourcePath)
    debug("Metadata image chargee : %O", metadataImage)

    const {width, height} = metadataImage
    const mimetype = metadataImage['mime type']
    quality = metadataImage.quality
    if(quality && quality < 0.60) {
      quality = ''+Math.round(quality * 100)
    } else {
      quality = '60'
    }

    if(width < height) {
      // Ratio inverse
      ratioInverse = true
    }

    // Definir valeur de reference pour la resolution (selon le ratio)
    valRef = ratioInverse?width:height
    valAutre = Math.round(valRef * (16/9))

  } catch(err) {
    debug("Erreur preparation image, aucune meta-information : %O", err)
    //  const info = await readMetadata(sourcePath)
    // debug("Information metadata exif : %O", info)
  }

  const conversions = {
    // Thumbnail : L'image est ramenee sur 128px, et croppee au milieu pour ratio 1:1
    'thumb': {ext: 'jpg', resolution: 128, params: ['-strip', '-resize', '128x128^', '-gravity', 'center', '-extent', '128x128', '-quality', '25']},
    'small': {ext: 'jpg', resolution: 200, params: ['-strip', '-resize', '200x200^', '-gravity', 'center', '-extent', '200x200', '-quality', '80']},
    // Poster, utilise pour afficher dans un coin d'ecran/preview
    // 'poster': {ext: 'jpg', resolution: 240, params: ['-strip', '-resize', ratioInverse?'240x420>':'420x240>', '-quality', '60']},
  }

  // Generer une version "pleine grandeur" en jpeg et webp
  // Peut agir comme poster avant le demarrage du video
  //if(valRef >= 360) {
    var geometrie = null
    if(ratioInverse) {
      geometrie = valRef + 'x' + valAutre + operationResize
    } else {
      geometrie = valAutre + 'x' + valRef + operationResize
    }

    conversions['image/webp;' + valRef] = {
      ext: 'webp',
      resolution: valRef,
      params: ['-strip', '-resize', geometrie, '-quality', quality]
    }
    // conversions['image/jpeg;' + valRef] = {
    //   ext: 'jpg',
    //   resolution: valRef,
    //   params: ['-strip', '-resize', geometrie, '-quality', quality]
    // }
  //}

  return {metadataImage, nbFrames, conversions}
}


function _imConvertPromise(params) {
  return new Promise((resolve, reject) => {
    console.debug("Conversion params : %O", params)
    im.convert(params,
      function(err, stdout){
        if (err) reject(err)
        resolve()
      })
  })
}

function genererSnapshotVideoPromise(sourcePath, previewPath) {
  return new Promise((resolve, reject) => {
    debug("Extraire preview du video %s vers %s", sourcePath, previewPath)

    // S'assurer d'avoir un .jpg, c'est ce qui indique au convertisseur le format de sortie
    var nomFichierDemande = path.basename(previewPath)
    var nomFichierPreview = nomFichierDemande
    if( ! nomFichierPreview.endsWith('.jpg') ) {
      nomFichierPreview += '.jpg'
    }
    var folderPreview = path.dirname(previewPath)

    debug("Fichier preview demande %s, temporaire : %s", nomFichierDemande, nomFichierPreview)

    var dataVideo = null
    new FFmpeg({ source: sourcePath, priority: 10, })
      .on('error', function(err) {
          console.error('An error occurred: ' + err.message);
          reject(err);
      })
      .on('codecData', data => {
        dataVideo = data;
      })
      .on('progress', progress=>{
        debug("Progress : %O", progress)
      })
      .on('end', filenames => {
        debug('Successfully generated thumbnail %s, filenames : %O ', previewPath, filenames);

        debug("Copie de %s", nomFichierPreview)
        if(nomFichierPreview !== nomFichierDemande) {
          // Rename
          fs.rename(path.join(folderPreview, nomFichierPreview), previewPath, err=>{
            if(err) return reject(err)
            resolve(dataVideo)
          })
        } else {
          resolve(dataVideo)
        }
      })
      .takeScreenshots(
        {
          count: 1,
          timestamps: ['25%'],
          filename: nomFichierPreview,
          folder: folderPreview,
          // size: '640x?',
        },
        '/'
      );
  });
}

// async function genererVideoMp4_480p(sourcePath, destinationPath, opts) {
//   if(!opts) opts = {}
//   const bitrate = opts.bitrate || '1800k'
//         height = opts.height || '480'
//   return await new Promise((resolve, reject) => {
//     new FFmpeg({source: sourcePath})
//       .withVideoBitrate(bitrate)
//       .withSize('?x' + height)
//       .on('error', function(err) {
//           console.error('An error occurred: ' + err.message);
//           reject(err);
//       })
//       .on('end', function(filenames) {

//         // let shasum = crypto.createHash('sha256');
//         try {
//           let s = fs.ReadStream(destinationPath)
//           let tailleFichier = 0;
//           s.on('data', data => {
//             // shasum.update(data)
//             tailleFichier += data.length;
//           })
//           s.on('end', function () {
//             // const sha256 = shasum.digest('hex')
//             // console.debug('Successfully generated 480p mp4 ' + destinationPath + ", taille " + tailleFichier + ", sha256 " + sha256);
//             // return resolve({tailleFichier, sha256});
//             return resolve({tailleFichier, bitrate, height})
//           })
//         } catch (error) {
//           return reject(error);
//         }

//       })
//       .saveToFile(destinationPath);
//   });
// }

async function readIdentify(filepath) {

  // Tenter identify avec methode custom
  let infoImage = null
  try {
    infoImage = await new Promise((resolve, reject)=>{
      try {
        const commande = [
          '-format',
          '{\"width\":%w,\"height\":%h,\"format\":\"%m\",\"orientation\":\"%[orientation]\"}',
          filepath + '[0]',
        ]

        im.identify(commande, (err, metadata)=>{
          if(err) return reject(err)
          try {
            debug("Resultat commande custom\n%s", metadata)
            resolve(JSON.parse(metadata))
          } catch(err) {
            reject(err)
          }
        })
      } catch(err) {
        reject(err)
      }
    })
  } catch(err) {
    debug("readIdentify Erreur identify custom, utiliser metadata default : ", err)
  }

  return new Promise((resolve, reject)=>{
    try {
      im.identify(filepath + '[0]', (err, metadata)=>{
        if(err) return reject(err)

        if(!infoImage) {
          infoImage = metadata
        } else {
          infoImage = {...metadata, ...infoImage}  // Donner preseance aux attributs custom
        }

        resolve(infoImage)
      })
    } catch(err) {
      reject(err)
    }
  })
}

function readIdentifyFrames(filepath) {
  /* Detecter nombre de frames (images animees) */
  return new Promise((resolve, reject)=>{
    try {
      im.identify(['-format', '%n;', filepath], (err, info)=>{
        if(err) return reject(err)
        try {
          resolve(Number(info.split(';')[0]))
        } catch(err) {
          return resolve(1)
        }
      })
    } catch(err) {
      reject(err)
    }
  })
}

module.exports = {
  genererConversionsImage, genererPosterVideo, 
  // genererVideoMp4_480p
}
