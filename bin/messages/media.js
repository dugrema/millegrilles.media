const debug = require('debug')('millegrilles:messages:media')
const traitementMedia = require('../traitementMedia.js')
const { traiterCommandeTranscodage } = require('../transformationsVideo')
const transfertConsignation = require('../transfertConsignation')

const urlServeurIndex = process.env.MG_ELASTICSEARCH_URL || 'http://elasticsearch:9200'
const urlConsignationFichiers = process.env.MG_FICHIERS_URL || 'https://fichiers:443'

const EXPIRATION_MESSAGE_DEFAUT = 15 * 60 * 1000,  // 15 minutes en millisec
      EXPIRATION_COMMANDE_TRANSCODAGE = 30 * 60 * 1000  // 30 minutes en millisec

const DOMAINE_MAITREDESCLES = 'MaitreDesCles',
      ACTION_SAUVEGARDERCLE = 'sauvegarderCle'

// Variables globales
var _mq = null,
    _idmg = null

function setMq(mq) {
  _mq = mq
  _idmg = mq.pki.idmg
  debug("IDMG RabbitMQ %s", this.idmg)

  // Config upload consignation
  transfertConsignation.setUrlServeurConsignation(urlConsignationFichiers, mq.pki)
}

// Appele lors d'une reconnexion MQ
function on_connecter() {
  enregistrerChannel()
}

function enregistrerChannel() {
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return genererPreviewImage(message)},
    ['commande.fichiers.genererPosterImage'],
    {
      // operationLongue: true,
      qCustom: 'image',
    }
  )
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return genererPreviewVideo(message)},
    ['commande.fichiers.genererPosterVideo'],
    {
      // operationLongue: true,
      qCustom: 'image',
    }
  )
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{return _traiterCommandeTranscodage(message)},
    ['commande.fichiers.transcoderVideo'],
    {
      // operationLongue: true,
      qCustom: 'video',
    }
  )
  _mq.routingKeyManager.addRoutingKeyCallback(
    (routingKey, message)=>{
      debug("indexerContenu : rk (%s) = %O", routingKey, message)
      return _indexerDocumentContenu(message)
    },
    ['commande.fichiers.indexerContenu'],
    {
      operationLongue: true,
      // qCustom: 'operationLongue',
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

  try {
    debug("Debut generation preview %O", message)
    const {clesPubliques} = cleFichier
    const resultatConversion = await traitementMedia.genererPreviewImage(
      _mq, fichierDechiffre, message, {clesPubliques, fuuid: hachageFichier})
    debug("Fin traitement thumbnails/posters, resultat : %O", resultatConversion)

    const {nbFrames, conversions} = resultatConversion
    const metadataImage = resultatConversion.metadataImage || {}

    // Extraire information d'images converties sous un dict
    let resultatPreview = null  // Utiliser poster (legacy)
    const images = {}
    const identificateurs_document = {type: 'image', fuuid_reference: hachageFichier}
    for(let idx in conversions) {
      const conversion = conversions[idx]
      debug("!!!CONVERSION : %O", conversion)
      const resultat = {...conversion.informationImage}
      const cle = conversion.cle
      images[cle] = resultat

      if(conversion.fichierTmp) {
        // Chiffrer et uploader le fichier tmp
        const {hachage, taille} = await transfertConsignation.uploaderFichierTraite(
          _mq, conversion.fichierTmp, clesPubliques, identificateurs_document)
        // Ajouter nouveau hachage (fuuid fichier converti)
        resultat.hachage = hachage
        resultat.taille = taille
      }
      if (conversion.commandeMaitreCles) {
        // Emettre la commande de maitre des cles
        const commandeMaitreCles = conversion.commandeMaitreCles
        const partition = commandeMaitreCles._partition
        delete commandeMaitreCles._partition
        await _mq.transmettreCommande(DOMAINE_MAITREDESCLES, commandeMaitreCles, {action: ACTION_SAUVEGARDERCLE, partition})
      }

    }

    // Transmettre transaction preview
    // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
    // const domaineActionAssocier = 'GrosFichiers.associerConversions'
    const transactionAssocier = {
      tuuid: message.tuuid,
      fuuid: hachageFichier,  // message.fuuid,
      images,
      width: metadataImage.width,
      height: metadataImage.height,
      mimetype: metadataImage['mime type'],
    }
    // Determiner si on a une image animee (fichier avec plusieurs frames, sauf PDF (plusieurs pages))
    const estPdf = transactionAssocier.mimetype === 'application/pdf'
    if(!estPdf && nbFrames > 1) transactionAssocier.anime = true

    debug("Transaction associer images converties : %O", transactionAssocier)

    _mq.transmettreTransactionFormattee(transactionAssocier, 'GrosFichiers', {action: 'associerConversions', ajouterCertificat: true})
      .catch(err=>{
        console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
      })
  } finally {
    // Effacer fichier tmp
    cleanup()
  }
}

async function genererPreviewVideo(message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
    console.warn("WARN media.genererPreviewVideo Commande expiree, on l'ignore : %O", message)
    return
  }

  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  const versionCourante = message.version_courante || {},
        hachageFichier = message.fuuid || message.hachage || message.fuuid_v_courante || versionCourante.fuuid || versionCourante.hachage
  // if(message.version_courante) {
  //   // C'est une retransmission
  //   hachageFichier = message.version_courante.hachage
  // }
  if(!hachageFichier) {
    console.error("ERROR media.genererPreviewVideo Aucune information de fichier dans le message : %O", message)
    return
  }
  const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(hachageFichier, message)

  const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}

  debug("Debut generation preview")
  const resultatConversion = await traitementMedia.genererPreviewVideo(mq, pathConsignation, message, optsConversion)
  debug("Fin traitement preview, resultat : %O", resultatConversion)

  const {metadataImage, metadataVideo, nbFrames, conversions} = resultatConversion

  // Extraire information d'images converties sous un dict
  let resultatPreview = null  // Utiliser poster (legacy)
  const images = {}
  for(let idx in conversions) {
    const conversion = conversions[idx]
    const resultat = {...conversion.informationImage}
    const cle = resultat.cle
    delete resultat.cle
    images[cle] = resultat
  }

  // Transmettre transaction preview
  // const domaineActionAssocierPreview = 'GrosFichiers.associerPreview'
  const transactionAssocier = {
    tuuid: message.tuuid,
    fuuid: message.fuuid,
    images,
    width: metadataImage.width,
    height: metadataImage.height,
    // mimetype: metadataImage['mime type'],
    metadata: metadataVideo,
  }
  transactionAssocier.anime = true

  debug("Transaction associer images converties : %O", transactionAssocier)

  mq.transmettreTransactionFormattee(transactionAssocier, 'GrosFichiers', {action: 'associerConversions', ajouterCertificat: true})
    .catch(err=>{
      console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
      debug("ERROR media.genererPreviewImage Erreur association conversions d'image message %O", message)
    })
}

function _traiterCommandeTranscodage(mq, pathConsignation, message) {

  // Verifier si la commande est expiree
  if(_mq.estExpire(message, {expiration: EXPIRATION_COMMANDE_TRANSCODAGE})) {
    console.warn("WARN media.traiterCommandeTranscodage Commande expiree, on l'ignore : %O", message)
    return
  }

  return traiterCommandeTranscodage(mq, pathConsignation, message)
    .catch(err=>{
      console.error("media._traiterCommandeTranscodage ERROR %s: %O", message.fuuid, err)
    })
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
  const {cleDechiffree, informationCle, clesPubliques} = cleFichier

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

  // Demander cles publiques pour rechiffrage
  const reponseClesPubliques = await _mq.transmettreRequete(
    'MaitreDesCles', {}, {action: 'certMaitreDesCles', ajouterCertificat: true})
  const clesPubliques = [reponseClesPubliques.certificat, [_mq.pki.ca]]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const domaine = 'MaitreDesCles',
        action = 'dechiffrage'
  const requete = {liste_hachage_bytes, permission}
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

module.exports = {setMq, on_connecter, genererPreviewImage}
