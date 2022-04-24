const debug = require('debug')('millegrilles:messages:media')
const traitementMedia = require('../traitementMedia.js')
const { traiterCommandeTranscodage } = require('../transformationsVideo')
const transfertConsignation = require('../transfertConsignation')

const urlServeurIndex = process.env.MG_ELASTICSEARCH_URL || 'http://elasticsearch:9200'

const EXPIRATION_MESSAGE_DEFAUT = 15 * 60 * 1000,  // 15 minutes en millisec
      EXPIRATION_COMMANDE_TRANSCODAGE = 30 * 60 * 1000  // 30 minutes en millisec

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle',
      DOMAINE_GROSFICHIERS = 'GrosFichiers'

// Variables globales
var _mq = null,
    _idmg = null,
    _storeConsignation = null  // injecte dans www

function setMq(mq) {
  _mq = mq
  _idmg = mq.pki.idmg
  debug("IDMG RabbitMQ %s", this.idmg)
}

function setStoreConsignation(urlConsignationFichiers, mq, storeConsignation) {
  // Config upload consignation
  debug("Set store consignation : %O", storeConsignation)
  _storeConsignation = storeConsignation
  if(!_mq) setMq(mq)
  transfertConsignation.init(urlConsignationFichiers, mq, storeConsignation)
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
      ['commande.fichiers.transcoderVideo'],
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
  var hachageFichier = message.hachage || message.fuuid
  var mimetype = message.mimetype
  if(message.version_courante) {
    // C'est une retransmission
    hachageFichier = message.version_courante.hachage || message.version_courante.fuuid
    mimetype = mimetype || message.version_courante.mimetype
  }
  const cleFichier = await recupererCle(hachageFichier, message)
  debug("Cle pour %s est dechiffree, info : %O", hachageFichier, cleFichier.metaCle)

  // Downloader et dechiffrer le fichier
  const {path: fichierDechiffre, cleanup} = await transfertConsignation.downloaderFichierProtege(
    hachageFichier, mimetype, cleFichier)

  const {clesPubliques} = cleFichier
  var resultatConversion = null
  try {
    debug("Debut generation preview %O", message)
    resultatConversion = await traitementMedia.genererPreviewImage(
      _mq, fichierDechiffre, message, {clesPubliques, fuuid: hachageFichier})
    debug("Fin traitement thumbnails/posters, resultat : %O", resultatConversion)
  } finally {
    cleanup()  // Nettoyer fichier dechiffre temporaire
  }

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
    const {uuidCorrelation, commandeMaitrecles, hachage, taille} = await transfertConsignation.stagerFichier(
      _mq, thumbnails.small.fichierTmp, clesPubliques, identificateurs_document, _storeConsignation)

    try {
      // Ajouter information pour image small
      small.hachage = hachage
      small.taille = taille
    
      let transactionContenu = {...transactionAssocier}
      transactionContenu.images = {thumb, small}
      transactionContenu = await _mq.pki.formatterMessage(
        transactionContenu, DOMAINE_GROSFICHIERS, {action: 'associerConversions', ajouterCertificat: true})
      debug("Transaction thumbnails : %O", transactionContenu)
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
    const {uuidCorrelation, commandeMaitrecles, hachage, taille} = await transfertConsignation.stagerFichier(
      _mq, conversion.fichierTmp, clesPubliques, identificateurs_document, _storeConsignation)
    try {
      resultat.hachage = hachage
      resultat.taille = taille

      let transactionContenu = {...transactionAssocier}
      transactionContenu.images = {[cle]: resultat}
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
  //     const {hachage, taille} = await transfertConsignation.uploaderFichierTraite(
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

  if(!hachageFichier) {
    console.error("ERROR media.genererPreviewVideo Aucune information de fichier dans le message : %O", message)
    return
  }
  const cleFichier = await recupererCle(hachageFichier, message)
  const {cleDechiffree, informationCle, clesPubliques} = cleFichier

  // Downloader et dechiffrer le fichier
  const {path: fichierDechiffre, cleanup} = await transfertConsignation.downloaderFichierProtege(
    hachageFichier, mimetype, cleFichier)

  // Transmettre transaction preview
  // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  var resultatConversion = null
  try {
    const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}
    debug("Debut generation preview")
    resultatConversion = await traitementMedia.genererPreviewVideo(_mq, fichierDechiffre, message, optsConversion)
    debug("Fin traitement preview, resultat : %O", resultatConversion)

    const {metadataImage, metadataVideo, conversions} = resultatConversion

    const transactionAssocier = {
      tuuid: message.tuuid,
      fuuid: message.fuuid,
      width: metadataImage.width,
      height: metadataImage.height,
      mimetype: mimetype,
      metadata: metadataVideo,
    }
    transactionAssocier.anime = true
  
    // Extraire information d'images converties sous un dict
    const images = await traiterConversions(hachageFichier, conversions, clesPubliques)
    transactionAssocier.images = images
  
    debug("Transaction associer images converties : %O", transactionAssocier)
  
    _mq.transmettreTransactionFormattee(
      transactionAssocier, 'GrosFichiers', {action: 'associerConversions', exchange: '4.secure', ajouterCertificat: true}
    )
    .catch(err=>{
      console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
      debug("ERROR media.genererPreviewImage Erreur association conversions d'image message %O", message)
    })

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
    cleanup()  // Supprimer fichier dechiffre temporaire
  }

}

async function _traiterCommandeTranscodage(message) {

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_COMMANDE_TRANSCODAGE})) {
    console.warn("WARN media.traiterCommandeTranscodage Commande expiree, on l'ignore : %O", message)
    return
  }

  debug("Traitement genererPreviewVideo : %O", message)

  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  const versionCourante = message.version_courante || {},
        hachageFichier = message.fuuid || message.hachage || message.fuuid_v_courante || versionCourante.fuuid || versionCourante.hachage,
        mimetype = message.mimetype
  if(!hachageFichier) {
    console.error("ERROR media.genererPreviewVideo Aucune information de fichier dans le message : %O", message)
    return
  }
  const cleFichier = await recupererCle(hachageFichier, message)
  const {cleDechiffree, informationCle, clesPubliques} = cleFichier

  debug("_traiterCommandeTranscodage fuuid: %s, cle: %O", hachageFichier, informationCle)

  // Downloader et dechiffrer le fichier
  const {path: fichierDechiffre, cleanup} = await transfertConsignation.downloaderFichierProtege(
    hachageFichier, mimetype, cleFichier)

  try {
    debug("_traiterCommandeTranscodage fichier temporaire: %s", fichierDechiffre)

    await traiterCommandeTranscodage(_mq, fichierDechiffre, clesPubliques, message)
      .catch(err=>{
        console.error("media._traiterCommandeTranscodage ERROR %s: %O", message.fuuid, err)
      })
  } finally {
    if(cleanup) cleanup()
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

  const cleFichier = await recupererCle(fuuid, message)
  // const {cleDechiffree, informationCle, clesPubliques} = cleFichier

  // Downloader et dechiffrer le fichier
  const {path: fichierDechiffre, cleanup} = await transfertConsignation.downloaderFichierProtege(
    fuuid, mimetype, cleFichier)

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
    if(cleanup) cleanup()
  }
}

async function recupererCle(hachageFichier, permission) {
  const liste_hachage_bytes = [hachageFichier]
  // Note: permission n'est plus requise - le certificat media donne acces a toutes les cles (domaine=GrosFichiers)
  // Le message peut avoir une permission attachee
  // if(permission.permission) permission = permission.permission

  // Demander cles publiques pour rechiffrage
  const reponseClesPubliques = await _mq.transmettreRequete(
    'MaitreDesCles', {}, {action: 'certMaitreDesCles', ajouterCertificat: true})
  const clesPubliques = [reponseClesPubliques.certificat, [_mq.pki.ca]]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const domaine = 'MaitreDesCles',
        action = 'dechiffrage'
  const requete = {liste_hachage_bytes}  //, permission}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await _mq.transmettreRequete(domaine, requete, {action, ajouterCertificat: true, decoder: true})
  debug("Reponse requete dechiffrage : %O", reponseCle)
  if(reponseCle.acces !== '1.permis') {
    return {err: reponseCle.acces, msg: `Erreur dechiffrage cle pour generer preview de ${hachageFichier}`}
  }
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  // Dechiffrer cle recue
  const metaCle = reponseCle.cles[hachageFichier]
  const cleChiffree = metaCle.cle
  const cleSymmetrique = await _mq.pki.decrypterAsymetrique(cleChiffree)

  return {cleSymmetrique, metaCle, clesPubliques}
}

module.exports = {setMq, setStoreConsignation, on_connecter, genererPreviewImage}
