const debug = require('debug')('messages:media')
const traitementMedia = require('../traitementMedia.js')
const { traiterCommandeTranscodage, progressUpdate } = require('../transformationsVideo')
// const transfertConsignation = require('../transfertConsignation')
const { recupererCle } = require('../pki')

const urlServeurIndex = process.env.MG_ELASTICSEARCH_URL || 'http://elasticsearch:9200'
// const activerQueuesProcessing = process.env.DISABLE_Q_PROCESSING?false:true

const EXPIRATION_MESSAGE_DEFAUT = 15 * 60 * 1000,  // 15 minutes en millisec
      EXPIRATION_COMMANDE_TRANSCODAGE = 30 * 60 * 1000  // 30 minutes en millisec

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle',
      DOMAINE_GROSFICHIERS = 'GrosFichiers'

// Variables globales
var _mq = null,
    _transfertConsignation = null,
    _downloadManager = null,
    activerQueuesProcessing = true,
    _consignationId = null,
    _consignationIdQueues = null

function setMq(mq, opts) {
  opts = opts || {}
  activerQueuesProcessing = opts.activerQueuesProcessing !== false
  _mq = mq
  _idmg = mq.pki.idmg
  debug("IDMG RabbitMQ %s", this.idmg)
}

function setHandlers(mq, transfertConsignation, downloadManager) {
  // Config upload consignation
  if(!_mq) setMq(mq)
  _transfertConsignation = transfertConsignation
  _downloadManager = downloadManager

  const idTransfert = _transfertConsignation.getIdConsignation()
  _transfertConsignation.ajouterListener(changementConsignation)
}

// Appele lors d'une reconnexion MQ
function on_connecter() {
  if(activerQueuesProcessing) enregistrerChannel()
}

function changementConsignation(consignationTransfert) {
  const consignationId = consignationTransfert.getIdConsignation()
  if(consignationId && _consignationId !== consignationId) {
    _consignationId = consignationId
    debug("Changement de consignation pour ID ", consignationId)
    enregistrerChannel()
  }
}

function enregistrerChannel() {
  if(!_consignationId || _consignationIdQueues === _consignationId) return   // Rien a faire

  debug("enregistrerChannel Transfert Consignation ", _transfertConsignation)

  if(_consignationIdQueues && _consignationIdQueues !== _consignationId) {
    debug("!!! TODO - retirer vieilles queues")
  }

  const exchange = '2.prive'
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return _traiterCommandeTranscodage(message)},
    [`commande.media.${_consignationId}.jobConversionVideoDisponible`],
    {
      qCustom: 'video',
      exchange,
    }
  )

  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return genererPreviewImage(message)},
    [`commande.media.${_consignationId}.genererPosterImage`],
    {
      qCustom: 'image',
      exchange,
    }
  )

  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return genererPreviewImage(message)},
    [`commande.media.${_consignationId}.genererPosterPdf`],
    {
      qCustom: 'pdf',
      exchange,
    }
  )

  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return genererPreviewVideo(message)},
    [`commande.media.${_consignationId}.genererPosterVideo`],
    {
      // operationLongue: true,
      qCustom: 'image',
      exchange,
    }
  )

  // Exchange 3.protege (default) uniquement
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{
      debug("indexerContenu : rk (%s) = %O", routingKey, message)
      return _indexerDocumentContenu(message)
    },
    [`commande.media.${_consignationId}.indexerContenu`],
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

  // Downloader fichier, preparer cle
  let cleFichier = null
  let stagingFichier = _downloadManager.getFichierCache(hachageFichier)
  if(!stagingFichier) {
    try {
      cleFichier = await recupererCle(_mq, hachageFichier)
      debug("Cle pour %s est dechiffree, info : %O", hachageFichier, cleFichier.metaCle)
      stagingFichier = await _downloadManager.downloaderFuuid(hachageFichier, cleFichier, {mimetype, dechiffrer: true, timeout: 25000})
    } catch(err) {
      debug("genererPreviewImage Erreur cles fichier %s non disponible : %O", hachageFichier, err)
      return {ok: false, err: 'Cles non disponibles : '+err}
    }
  } else {
    cleFichier = stagingFichier.cle
  }

  const fichierDechiffre = stagingFichier.path

  // // Downloader et dechiffrer le fichier
  // try {
  //   var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
  //     hachageFichier, mimetype, cleFichier, {extension})

  // } catch(err) {
  //   debug("genererPreviewImage Erreur download fichier avec downloaderFichierProtege : %O", err)
  //   return {ok: false, err: ''+err}
  // }

  const {cleSymmetrique, clesPubliques} = cleFichier
  var resultatConversion = null
  try {
    debug("Debut generation preview %O", message)
    resultatConversion = await traitementMedia.genererPreviewImage(
      _mq, fichierDechiffre, message, {cleSecrete: cleSymmetrique, fuuid: hachageFichier, extension})
    debug("Fin traitement thumbnails/posters, resultat : %O", resultatConversion)
  } catch(err) {
    debug("genererPreviewImage Erreur creation preview image %s : %O", hachageFichier, err)
    return {ok: false, err: 'Erreur creation preview image : '+err}
  }
  // finally {
  //   // Note: pour pdf, on utilise autoclean (indexation survient en meme temps)
  //   if(mimetype !== 'application/pdf') {
  //     // Nettoyer fichier dechiffre temporaire
  //     cleanup().catch(err=>console.debug("Erreur suppression fichier dechiffre %s : %O", hachageFichier, err))
  //   }
  // }

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

    const images = await traiterConversions(hachageFichier, conversions, clesPubliques, transactionAssocier, cleFichier)
    // transactionAssocier.images = images

    // debug("Transaction associer images converties : %O", transactionAssocier)
    // _mq.transmettreTransactionFormattee(
    //   transactionAssocier, 'GrosFichiers', {action: 'associerConversions', exchange: '4.secure', ajouterCertificat: true}
    // ).catch(err=>{
    //     console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
    //   })

    if(mimetype.startsWith('image/')) {
      // Le fichier est une image - il ne reste aucun traitement media supplementaire a faire
      _downloadManager.setFichierDownloadOk(hachageFichier)
    }

  } catch(err) {
    debug("genererPreviewImage Erreur preparation resultat conversion fichier %s : %O", hachageFichier, err)
    return {ok: false, err: 'Erreur preparation resultat conversion : '+err}
  }

}

async function traiterConversions(fuuid, conversions, clesPubliques, transactionAssocier, cleFichier) {
  // Extraire information d'images converties sous un dict
  const images = {}

  // Conserver dict de labels d'images (e.g. image.thumb, image.small)
  const thumbnails = conversions.filter(item=>['thumb', 'small'].includes(item.cle)).reduce((acc, item)=>{
    acc[item.cle] = item
    return acc
  }, {})

  const imagesLarges = conversions.filter(item=>!['thumb', 'small'].includes(item.cle))

  debug("Images thumbnails: %O\nImages larges: %O", thumbnails, imagesLarges)

  const cleSecrete = cleFichier.cleSymmetrique

  // Uploader thumbnails
  {
    const thumb = {...thumbnails.thumb.informationImage},
          small = {...thumbnails.small.informationImage}

    const generateurTransaction = async infoSmall => {
      small.hachage = infoSmall.hachage
      small.taille = infoSmall.taille
      small.header = infoSmall.header
      small.format = infoSmall.format
      
      const transactionContenu = {...transactionAssocier}
      transactionContenu.images = {thumb, small}
      
      // Cleanup transaction
      const transactionMetadata = transactionContenu.metadata
      if(transactionContenu.duration === 'N/A') delete transactionContenu.duration
      if(transactionMetadata) {
        if(transactionMetadata.nbFrames === 'N/A') delete transactionMetadata.nbFrames
      }

      const transactionSignee = await _mq.pki.formatterMessage(
        transactionContenu, DOMAINE_GROSFICHIERS, {action: 'associerConversions', ajouterCertificat: true})
      debug("Transaction thumbnails : ", transactionSignee)

      return transactionSignee
    }

    await _transfertConsignation.stagerFichier(thumbnails.small.fichierTmp, cleSecrete, {generateurTransaction})
    
  }

  // Uploader images larges (individuellement)
  for await (let conversion of imagesLarges) {
    // Preparer la transaction de contenu
    const resultat = {...conversion.informationImage}
    const cle = conversion.cle
    images[cle] = resultat

    // Chiffrer et conserver image dans staging local pour upload vers consignation
    // const {uuidCorrelation, hachage, taille, header, format} = 
    const generateurTransaction = async infoImage => {
      resultat.hachage = infoImage.hachage
      resultat.taille = infoImage.taille
      resultat.header = infoImage.header
      resultat.format = infoImage.format

      const transactionContenu = {...transactionAssocier}
      transactionContenu.images = {[cle]: resultat}

      // Cleanup transaction
      const transactionMetadata = transactionContenu.metadata
      if(transactionContenu.duration === 'N/A') delete transactionContenu.duration
      if(transactionMetadata) {
        if(transactionMetadata.nbFrames === 'N/A') delete transactionMetadata.nbFrames
      }

      const transactionSignee = await _mq.pki.formatterMessage(
        transactionContenu, DOMAINE_GROSFICHIERS, {action: 'associerConversions', ajouterCertificat: true})
      debug("Transaction contenu image : %O", transactionContenu)
      
      return transactionSignee
    }

    await _transfertConsignation.stagerFichier(conversion.fichierTmp, cleSecrete, {generateurTransaction})

  }

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

  // Downloader fichier, preparer cle
  let cleFichier = null
  let stagingFichier = _downloadManager.getFichierCache(hachageFichier)
  if(!stagingFichier) {
    try {
      cleFichier = await recupererCle(_mq, hachageFichier)
      debug("Cle pour %s est dechiffree, info : %O", hachageFichier, cleFichier.metaCle)
      stagingFichier = await _downloadManager.downloaderFuuid(hachageFichier, cleFichier, {mimetype, dechiffrer: true, timeout: 47000})
    } catch(err) {
      debug("genererPreviewImage Erreur cles fichier %s non disponible : %O", hachageFichier, err)
      return {ok: false, err: 'Cles non disponibles : '+err}
    }
  } else {
    cleFichier = stagingFichier.cle
  }

  const fichierDechiffre = stagingFichier.path
  
  // const cleFichier = await recupererCle(_mq, hachageFichier)
  const {cleSymmetrique, informationCle, clesPubliques} = cleFichier
  // // Downloader et dechiffrer le fichier
  // try {
  //   var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
  //     hachageFichier, mimetype, cleFichier, {extension})
  // } catch(err) {
  //   debug("genererPreviewVideo Erreur download fichier avec downloaderFichierProtege : %O", err)
  //   return {ok: false, err: ''+err}
  // }

  // Transmettre transaction preview
  // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  var resultatConversion = null
  try {
    const optsConversion = {cleSecrete: cleSymmetrique, metaCle: informationCle, clesPubliques}
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
      nbFrames: probeVideo.raw.nb_frames!=="N/A"?probeVideo.raw.nb_frames:null,
      // duration: probeVideo.raw.duration,
      videoBitrate:  probeVideo.raw.bitrate,
    }

    let duration = null
    if(probeVideo.raw.duration !== 'N/A') {
      duration = Math.round(probeVideo.raw.duration * 1000) / 1000.0  // Arrondir a la millisec
    }

    const transactionAssocier = {
      tuuid: message.tuuid,
      fuuid: message.fuuid,
      width: probeVideo.width,
      height: probeVideo.height,
      mimetype: mimetype,
      videoCodec: probeVideo.raw.codec_name,
      duration,
      metadata,
    }
    transactionAssocier.anime = true
  
    // Extraire information d'images converties sous un dict
    await traiterConversions(hachageFichier, conversions, clesPubliques, transactionAssocier, cleFichier)

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

  // Downloader fichier, preparer cle
  let cleFichier = null
  let stagingFichier = _downloadManager.getFichierCache(fuuid)
  if(!stagingFichier) {
    try {
      cleFichier = await recupererCle(_mq, fuuid)
      debug("Cle pour %s est dechiffree, info : %O", fuuid, cleFichier.metaCle)
      stagingFichier = await _downloadManager.downloaderFuuid(fuuid, cleFichier, {mimetype, dechiffrer: true, timeout: 47000})
    } catch(err) {
      debug("genererPreviewImage Erreur cles fichier %s non disponible : %O", fuuid, err)
      return {ok: false, err: 'Cles non disponibles : '+err}
    }
  } else {
    cleFichier = stagingFichier.cle
  }

  const fichierDechiffre = stagingFichier.path
  
  // const cleFichier = await recupererCle(_mq, fuuid)
  const {cleSymmetrique, informationCle, clesPubliques} = cleFichier

  debug("_traiterCommandeTranscodage fuuid: %s, cle: %O", fuuid, informationCle)

  // Creer un progress cb regulier - utilise comme healthcheck pour eviter la reallocation de la conversion de fichier
  // const progressDownload = params => {
  //   params = params || {}
  //   position = params.position || 0

  //   const {fuuid, cle_conversion } = message
  //   const [mimetype, videoCodec, heightStr, videoQuality ] = cle_conversion.split(';')
  //   const height = Number.parseInt(heightStr.replace('p', ''))
  //   const paramsVideo = {fuuid, mimetype, videoCodec, height, videoQuality, etat: 'downloading'}
  //   progressUpdate(_mq, paramsVideo, {percent: 0})
  // }

  // let timeoutProgressDownload = setTimeout(progressDownload, 20000)

  // // Downloader et dechiffrer le fichier
  // try {
  //   progressDownload()
  //   var {path: fichierDechiffre, cleanup} = await _transfertConsignation.downloaderFichierProtege(
  //     fuuid, mimetype, cleFichier, {progress: progressDownload})
  // } catch(err) {
  //   debug("_traiterCommandeTranscodage Erreur download fichier avec downloaderFichierProtege : %O", err)
  //   return {ok: false, err: ''+err}
  // } finally {
  //   clearTimeout(timeoutProgressDownload)
  // }

  try {
    debug("_traiterCommandeTranscodage fichier temporaire: %s", fichierDechiffre)

    stagingFichier.actif = true
    stagingFichier.dernierAcces = new Date()
    await traiterCommandeTranscodage(_mq, fichierDechiffre, reponseGetJob, {cleSecrete: cleSymmetrique})
      .catch(err=>{
        debug("media._traiterCommandeTranscodage ERROR Erreur transcodage  %s: %O", fuuid, err)
        return {ok: false, err: 'Erreur transcodage '+err}
      })
  } catch(err) {
    debug("media._traiterCommandeTranscodage ERROR Erreur transcodage %s: %O", fuuid, err)
    return {ok: false, err: 'Erreur transcodage '+err}
  } finally {
    // maintenant autoclean
    //if(cleanup) cleanup()
    stagingFichier.actif = false
    stagingFichier.dernierAcces = new Date()
  }
}

async function _indexerDocumentContenu(message) {
  debug("Traitement _indexerDocumentContenu : %O", message)
  throw new Error('not implemented')

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

module.exports = {setMq, setHandlers, on_connecter, genererPreviewImage}
