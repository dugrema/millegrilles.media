const debug = require('debug')('millegrilles:messages:media')
const traitementMedia = require('../traitementMedia.js')
const { traiterCommandeTranscodage, progressUpdate } = require('../transformationsVideo')
// const transfertConsignation = require('../transfertConsignation')
const { recupererCle } = require('../pki')

const urlServeurIndex = process.env.MG_ELASTICSEARCH_URL || 'http://elasticsearch:9200'

const EXPIRATION_MESSAGE_DEFAUT = 15 * 60 * 1000,  // 15 minutes en millisec
      EXPIRATION_COMMANDE_TRANSCODAGE = 30 * 60 * 1000  // 30 minutes en millisec

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle',
      DOMAINE_GROSFICHIERS = 'GrosFichiers'

// Variables globales
var _mq = null,
    _idmg = null,
    _storeConsignation = null  // injecte dans www,
    _transfertConsignation = null

function setMq(mq) {
  _mq = mq
  _idmg = mq.pki.idmg
  debug("IDMG RabbitMQ %s", this.idmg)
}

function setStoreConsignation(mq, storeConsignation, transfertConsignation) {
  // Config upload consignation
  debug("Set store consignation : %O", storeConsignation)
  _storeConsignation = storeConsignation
  if(!_mq) setMq(mq)
  //transfertConsignation.init(urlConsignationFichiers, mq, storeConsignation)
  _transfertConsignation = transfertConsignation
}

// Appele lors d'une reconnexion MQ
function on_connecter() {
  enregistrerChannel()
}

function enregistrerChannel() {

  // Exchanges 2.prive et 3.protege
  ['2.prive', '3.protege'].map(exchange=>{
    _mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return _traiterCommandeTranscodage(message)},
      ['commande.fichiers.jobConversionVideoDisponible'],
      {
        // operationLongue: true,
        qCustom: 'video',
        exchange,
      }
    )

    _mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewImage(message)},
      ['commande.fichiers.genererPosterImage'],
      {
        // operationLongue: true,
        qCustom: 'image',
        exchange,
      }
    )

    _mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewVideo(message)},
      ['commande.fichiers.genererPosterVideo'],
      {
        // operationLongue: true,
        qCustom: 'image',
        exchange,
      }
    )

  })

  // Exchange 3.protege (default) uniquement
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{
      debug("indexerContenu : rk (%s) = %O", routingKey, message)
      return _indexerDocumentContenu(message)
    },
    ['commande.fichiers.indexerContenu'],
    {
      // operationLongue: true,
      qCustom: 'indexation',
    }
  )

}

async function genererPreviewImage(message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}

  debug("GenererPreviewImage message recu : %O", message)

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
    console.warn("WARN media.genererPreviewImage Commande expiree, on l'ignore : %O", message)
    return
  }

  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  let hachageFichier = message.hachage || message.fuuid
  let mimetype = message.mimetype
  let extension = message.extension
  if(message.version_courante) {
    // C'est une retransmission
    const version_courante = message.version_courante
    hachageFichier = version_courante.hachage || version_courante.fuuid
    mimetype = mimetype || version_courante.mimetype
    if(!extension) {
      const nom = version_courante.nom
      if(nom) extension = nom.split('.').pop()
    }
  }

  try {
    var cleFichier = await recupererCle(_mq, hachageFichier)
    debug("Cle pour %s est dechiffree, info : %O", hachageFichier, cleFichier.metaCle)
  } catch(err) {
    debug("genererPreviewImage Erreur cles fichier %s non disponible : %O", hachageFichier, err)
    return {ok: false, err: 'Cles non disponibles : '+err}
  }

  // Downloader et dechiffrer le fichier
  try {
    var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
      hachageFichier, mimetype, cleFichier, {extension})
  } catch(err) {
    debug("genererPreviewImage Erreur download fichier avec downloaderFichierProtege : %O", err)
    return {ok: false, err: ''+err}
  }

  const {clesPubliques} = cleFichier
  var resultatConversion = null
  try {
    debug("Debut generation preview %O", message)
    resultatConversion = await traitementMedia.genererPreviewImage(
      _mq, fichierDechiffre, message, {clesPubliques, fuuid: hachageFichier, extension})
    debug("Fin traitement thumbnails/posters, resultat : %O", resultatConversion)
  } catch(err) {
    debug("genererPreviewImage Erreur creation preview image %s : %O", hachageFichier, err)
    return {ok: false, err: 'Erreur creation preview image : '+err}
  } finally {
    // Note: pour pdf, on utilise autoclean (indexation survient en meme temps)
    if(mimetype !== 'application/pdf') {
      // Nettoyer fichier dechiffre temporaire
      cleanup().catch(err=>console.debug("Erreur suppression fichier dechiffre %s : %O", hachageFichier, err))
    }
  }

  try {
    const {nbFrames, conversions} = resultatConversion
    const metadataImage = resultatConversion.metadataImage || {}

    // Transmettre transaction preview
    const transactionAssocier = {
      tuuid: message.tuuid,
      fuuid: hachageFichier,  // message.fuuid,
      // images,
      width: metadataImage.width,
      height: metadataImage.height,
      mimetype: metadataImage['mime type'],
    }
    // Determiner si on a une image animee (fichier avec plusieurs frames, sauf PDF (plusieurs pages))
    const estPdf = transactionAssocier.mimetype === 'application/pdf'
    if(!estPdf && nbFrames > 1) transactionAssocier.anime = true

    const images = await traiterConversions(hachageFichier, conversions, clesPubliques, transactionAssocier)
    // transactionAssocier.images = images

    // debug("Transaction associer images converties : %O", transactionAssocier)
    // _mq.transmettreTransactionFormattee(
    //   transactionAssocier, 'GrosFichiers', {action: 'associerConversions', exchange: '4.secure', ajouterCertificat: true}
    // ).catch(err=>{
    //     console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
    //   })
  } catch(err) {
    debug("genererPreviewImage Erreur preparation resultat conversion fichier %s : %O", hachageFichier, err)
    return {ok: false, err: 'Erreur preparation resultat conversion : '+err}
  }

}

async function traiterConversions(fuuid, conversions, clesPubliques, transactionAssocier) {
  // Extraire information d'images converties sous un dict
  const images = {}

  const identificateurs_document = {type: 'image', fuuid_reference: fuuid}
  // Conserver dict de labels d'images (e.g. image.thumb, image.small)
  const thumbnails = conversions.filter(item=>['thumb', 'small'].includes(item.cle)).reduce((acc, item)=>{
    acc[item.cle] = item
    return acc
  }, {})
  const imagesLarges = conversions.filter(item=>!['thumb', 'small'].includes(item.cle))

  debug("Images thumbnails: %O\nImages larges: %O", thumbnails, imagesLarges)

  // Uploader thumbnails
  {
    const thumb = {...thumbnails.thumb.informationImage},
          small = {...thumbnails.small.informationImage}
    const {uuidCorrelation, commandeMaitrecles, hachage, taille} = await _transfertConsignation.stagerFichier(
      _mq, thumbnails.small.fichierTmp, clesPubliques, identificateurs_document, _storeConsignation)

    try {
      // Ajouter information pour image small
      small.hachage = hachage
      small.taille = taille
     
      let transactionContenu = {...transactionAssocier}
      transactionContenu.images = {thumb, small}
      // Cleanup transaction
      const transactionMetadata = transactionContenu.metadata
      if(transactionContenu.duration === 'N/A') delete transactionContenu.duration
      if(transactionMetadata.nbFrames === 'N/A') delete transactionMetadata.nbFrames

      transactionContenu = await _mq.pki.formatterMessage(
        transactionContenu, DOMAINE_GROSFICHIERS, {action: 'associerConversions', ajouterCertificat: true})
      debug("Transaction thumbnails : %O", transactionContenu)

      // Emettre la commande de maitre des cles du thumbnail. Les autres cles sont transferees au store de consignation.
      const commandeClesThumbnail = thumbnails.thumb.commandeMaitreCles
      const partition = commandeClesThumbnail._partition
      delete commandeClesThumbnail._partition
      await _mq.transmettreCommande(DOMAINE_MAITREDESCLES, commandeClesThumbnail, {action: ACTION_SAUVEGARDERCLE, partition})

      await _storeConsignation.stagingReady(_mq, transactionContenu, commandeMaitrecles, uuidCorrelation, {PATH_STAGING: PATH_MEDIA_STAGING})

    } catch(err) {
      await _storeConsignation.stagingDelete(uuidCorrelation, {PATH_STAGING: PATH_MEDIA_STAGING})
      throw err
    }
  }

  // Uploader images larges (individuellement)
  for await (let conversion of imagesLarges) {
    // Preparer la transaction de contenu
    const resultat = {...conversion.informationImage}
    const cle = conversion.cle
    images[cle] = resultat

    // Chiffrer et conserver image dans staging local pour upload vers consignation
    const {uuidCorrelation, commandeMaitrecles, hachage, taille} = await _transfertConsignation.stagerFichier(
      _mq, conversion.fichierTmp, clesPubliques, identificateurs_document, _storeConsignation)
    try {
      resultat.hachage = hachage
      resultat.taille = taille

      let transactionContenu = {...transactionAssocier}
      transactionContenu.images = {[cle]: resultat}

      // Cleanup transaction
      const transactionMetadata = transactionContenu.metadata
      if(transactionContenu.duration === 'N/A') delete transactionContenu.duration
      if(transactionMetadata.nbFrames === 'N/A') delete transactionMetadata.nbFrames

      transactionContenu = await _mq.pki.formatterMessage(
        transactionContenu, DOMAINE_GROSFICHIERS, {action: 'associerConversions', ajouterCertificat: true})
      debug("Transaction contenu image : %O", transactionContenu)
      await _storeConsignation.stagingReady(_mq, transactionContenu, commandeMaitrecles, uuidCorrelation, {PATH_STAGING: PATH_MEDIA_STAGING})
    } catch(err) {
      await _storeConsignation.stagingDelete(uuidCorrelation, {PATH_STAGING: PATH_MEDIA_STAGING})
      throw err
    }
  }

  // for(let idx in conversions) {
  //   const conversion = conversions[idx]
  //   const resultat = {...conversion.informationImage}
  //   const cle = conversion.cle
  //   images[cle] = resultat

  //   if(conversion.fichierTmp) {
  //     // Chiffrer et uploader le fichier tmp
  //     const {hachage, taille} = await _transfertConsignation.uploaderFichierTraite(
  //       _mq, conversion.fichierTmp, clesPubliques, identificateurs_document)
  //     // Ajouter nouveau hachage (fuuid fichier converti)
  //     resultat.hachage = hachage
  //     resultat.taille = taille
  //   }

  //   if (conversion.commandeMaitreCles) {
  //     // Emettre la commande de maitre des cles
  //     const commandeMaitreCles = conversion.commandeMaitreCles
  //     const partition = commandeMaitreCles._partition
  //     delete commandeMaitreCles._partition
  //     await _mq.transmettreCommande(DOMAINE_MAITREDESCLES, commandeMaitreCles, {action: ACTION_SAUVEGARDERCLE, partition})
  //   }
  // }

  return images
}

async function genererPreviewVideo(message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
    console.warn("WARN media.genererPreviewVideo Commande expiree, on l'ignore : %O", message)
    return
  }

  debug("Traitement genererPreviewVideo : %O", message)

  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  const versionCourante = message.version_courante || {},
        hachageFichier = message.fuuid || message.hachage || message.fuuid_v_courante || versionCourante.fuuid || versionCourante.hachage,
        mimetype = message.mimetype

  let extension = message.extension
  if(!extension && versionCourante.nom) {
    extension = versionCourante.nom.split('.').pop()
  }

  if(!hachageFichier) {
    console.error("ERROR media.genererPreviewVideo Aucune information de fichier dans le message : %O", message)
    return
  }
  const cleFichier = await recupererCle(_mq, hachageFichier)
  const {cleDechiffree, informationCle, clesPubliques} = cleFichier

  // Downloader et dechiffrer le fichier
  try {
    var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
      hachageFichier, mimetype, cleFichier, {extension})
  } catch(err) {
    debug("genererPreviewVideo Erreur download fichier avec downloaderFichierProtege : %O", err)
    return {ok: false, err: ''+err}
  }

  // Transmettre transaction preview
  // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  var resultatConversion = null
  try {
    const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}
    debug("Debut generation preview")
    resultatConversion = await traitementMedia.genererPreviewVideo(_mq, fichierDechiffre, message, optsConversion)
    debug("Fin traitement preview, resultat : %O", resultatConversion)

    if(!resultatConversion) {
      console.error("genererPreviewVideo Erreur conversion (aucuns resultats) video %s", message.fuuid)
      return
    }

    const {probeVideo, conversions} = resultatConversion

    const metadata = {
      // videoCodec: probeVideo.raw.codec_name,
      nbFrames: probeVideo.raw.nb_frames,
      // duration: probeVideo.raw.duration,
      videoBitrate:  probeVideo.raw.bitrate,
    }

    const transactionAssocier = {
      tuuid: message.tuuid,
      fuuid: message.fuuid,
      width: probeVideo.width,
      height: probeVideo.height,
      mimetype: mimetype,
      videoCodec: probeVideo.raw.codec_name,
      duration: probeVideo.raw.duration,
      metadata,
    }
    transactionAssocier.anime = true
  
    // Extraire information d'images converties sous un dict
    const images = await traiterConversions(hachageFichier, conversions, clesPubliques, transactionAssocier)
    // transactionAssocier.images = images
  
    // debug("Transaction associer images converties : %O", transactionAssocier)
  
    // _mq.transmettreTransactionFormattee(
    //   transactionAssocier, 'GrosFichiers', {action: 'associerConversions', exchange: '4.secure', ajouterCertificat: true}
    // )
    // .catch(err=>{
    //   console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
    //   debug("ERROR media.genererPreviewImage Erreur association conversions d'image message %O", message)
    // })

    // Probleme - transcodage video ici bloque la Q de traitement des images
    // // Conversion videos 240p en mp4 et vp9
    // const tuuid = message.tuuid
    // const commandeMp4 = {
    //   tuuid: tuuid, fuuid: hachageFichier, mimetype: 'video/mp4', videoBitrate: 250000, audioBitrate: '64k', height: 240,
    // }
    // const commandeVp9 = {
    //   tuuid: tuuid, fuuid: hachageFichier, mimetype: 'video/webm', videoBitrate: 250000, audioBitrate: '64k', height: 320,
    // }
    // await traiterCommandeTranscodage(_mq, fichierDechiffre, clesPubliques, commandeMp4)
    //   .catch(err=>console.error("media._traiterCommandeTranscodage ERROR mp4 %s: %O", message.fuuid, err))
    // await traiterCommandeTranscodage(_mq, fichierDechiffre, clesPubliques, commandeVp9)
    //   .catch(err=>console.error("media._traiterCommandeTranscodage ERROR webm %s: %O", message.fuuid, err))

  } finally {
    // maintenant autoclean
    // cleanup()  // Supprimer fichier dechiffre temporaire
  }

}

async function _traiterCommandeTranscodage(message) {

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_COMMANDE_TRANSCODAGE})) {
    console.warn("WARN media.traiterCommandeTranscodage Commande expiree, on l'ignore : %O", message)
    return
  }

  debug("_traiterCommandeTranscodage Traitement genererPreviewVideo : %O", message)

  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  {
    const fuuid = message.fuuid, 
          cleVideo = message.cleVideo
    const commandeGetJob = {fuuid, cle_video: cleVideo}
    var reponseGetJob = await _mq.transmettreCommande(
      'GrosFichiers', commandeGetJob, {action: 'getJobVideo', exchange: '2.prive', attacherCertificat: true})
    
    debug("_traiterCommandeTranscodage Reponse getJob : %O", reponseGetJob)

    if(reponseGetJob.ok === false) {
      debug("_traiterCommandeTranscodage Erreur demande job, abort : %O", reponseGetJob.err)
      return {ok: false, err: ''+reponseGetJob.err}
    } else if(reponseGetJob.ok === true && !reponseGetJob.fuuid) {
      debug("_traiterCommandeTranscodage Aucune job disponible")
      return {ok: true, message: 'Aucune job disponible'}
    }

  }

  const fuuid = reponseGetJob.fuuid,
        mimetype = reponseGetJob.mimetype
  if(!fuuid) {
    console.error("ERROR media.genererPreviewVideo Aucune information de fichier dans le message : %O", reponseGetJob)
    return
  }

  const cleFichier = await recupererCle(_mq, fuuid)
  const {cleDechiffree, informationCle, clesPubliques} = cleFichier

  debug("_traiterCommandeTranscodage fuuid: %s, cle: %O", fuuid, informationCle)

  // Creer un progress cb regulier - utilise comme healthcheck pour eviter la reallocation de la conversion de fichier
  const progressDownload = () => {
    const {fuuid, cle_conversion } = message
    const [mimetype, videoCodec, heightStr, videoQuality ] = cle_conversion.split(';')
    const height = Number.parseInt(heightStr.replace('p', ''))
    const paramsVideo = {fuuid, mimetype, videoCodec, height, videoQuality, etat: 'downloading'}
    progressUpdate(_mq, paramsVideo, {percent: 0})
  }
  let timeoutProgressDownload = setTimeout(progressDownload, 20000)
  // Downloader et dechiffrer le fichier
  try {
    progressDownload()
    var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
      fuuid, mimetype, cleFichier)
  } catch(err) {
    debug("_traiterCommandeTranscodage Erreur download fichier avec downloaderFichierProtege : %O", err)
    return {ok: false, err: ''+err}
  } finally {
    clearTimeout(timeoutProgressDownload)
  }

  try {
    debug("_traiterCommandeTranscodage fichier temporaire: %s", fichierDechiffre)

    await traiterCommandeTranscodage(_mq, fichierDechiffre, clesPubliques, reponseGetJob, _storeConsignation)
      .catch(err=>{
        debug("media._traiterCommandeTranscodage ERROR Erreur transcodage  %s: %O", fuuid, err)
        return {ok: false, err: 'Erreur transcodage '+err}
      })
  } catch(err) {
    debug("media._traiterCommandeTranscodage ERROR Erreur transcodage %s: %O", fuuid, err)
    return {ok: false, err: 'Erreur transcodage '+err}
}
  finally {
    // maintenant autoclean
    //if(cleanup) cleanup()
  }
}

async function _indexerDocumentContenu(message) {
  debug("Traitement _indexerDocumentContenu : %O", message)

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
    console.warn("WARN media.indexerDocumentContenu Commande expiree, on l'ignore : %O", message)
    return
  }

  const fuuid = message.fuuid
  var mimetype = message.mimetype || message.doc?message.doc.mimetype:null

  debug("Indexer %s type %s", fuuid, mimetype)

  const cleFichier = await recupererCle(_mq, fuuid)
  // const {cleDechiffree, informationCle, clesPubliques} = cleFichier

  // Downloader et dechiffrer le fichier
  try {
    var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
      fuuid, mimetype, cleFichier)
  } catch(err) {
    debug("_indexerDocumentContenu Erreur download fichier avec downloaderFichierProtege : %O", err)
    return {ok: false, err: ''+err}
  }

  try {
    debug("Fichier dechiffre pour indexation : %s", fichierDechiffre)
    const optsConversion = {urlServeurIndex}

    await traitementMedia.indexerDocument(_mq, fichierDechiffre, message, optsConversion)

    const commandeResultat = { ok: true, fuuid }

    debug("Commande resultat indexation : %O", commandeResultat)

    await _mq.transmettreCommande(
      "GrosFichiers",
      commandeResultat,
      {action: 'confirmerFichierIndexe', ajouterCertificat: true, nowait: true}
    )
  } finally {
    // Autoclean
    //if(cleanup) cleanup()
  }
}

module.exports = {setMq, setStoreConsignation, on_connecter, genererPreviewImage}
