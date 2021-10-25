const debug = require('debug')('millegrilles:messages:media')
const {PathConsignation} = require('../traitementFichier')
const traitementMedia = require('../traitementMedia.js')
const { traiterCommandeTranscodage } = require('../transformationsVideo')

const urlServeurIndex = process.env.MG_SERVEUR_INDEX_URL || 'http://elasticsearch:9200'

const EXPIRATION_MESSAGE_DEFAUT = 15 * 60 * 1000,  // 15 minutes en millisec
      EXPIRATION_COMMANDE_TRANSCODAGE = 30 * 60 * 1000  // 30 minutes en millisec

// Traitement d'images pour creer des thumbnails et preview
class GenerateurMedia {

  constructor(mq) {
    this.mq = mq;
    this.idmg = this.mq.pki.idmg;

    this.pathConsignation = new PathConsignation({idmg: this.idmg});

    debug("Path RabbitMQ %s : %s", this.idmg, this.pathConsignation.consignationPath);
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewImage(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.genererPosterImage'],
      {
        // operationLongue: true,
        qCustom: 'image',
      }
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return genererPreviewVideo(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.genererPosterVideo'],
      {
        // operationLongue: true,
        qCustom: 'image',
      }
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return _traiterCommandeTranscodage(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.transcoderVideo'],
      {
        // operationLongue: true,
        qCustom: 'video',
      }
    )
    this.mq.routingKeyManager.addRoutingKeyCallback(
      (routingKey, message)=>{return _indexerDocumentContenu(this.mq, this.pathConsignation, message)},
      ['commande.fichiers.indexerContenu'],
      {
        operationLongue: true,
        // qCustom: 'operationLongue',
      }
    )
  }

}

async function genererPreviewImage(mq, pathConsignation, message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}

  debug("GenererPreviewImage message recu : %O", message)

  // Verifier si la commande est expiree
  if(mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
    console.warn("WARN media.genererPreviewImage Commande expiree, on l'ignore : %O", message)
    return
  }

  // Transmettre demande cle et attendre retour sur l'autre Q (on bloque Q operations longues)
  var hachageFichier = message.hachage || message.fuuid
  if(message.version_courante) {
    // C'est une retransmission
    hachageFichier = message.version_courante.hachage || message.version_courante.fuuid
  }
  const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(mq, hachageFichier, message)

  const optsConversion = {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}

  debug("Debut generation preview %O", message)
  const resultatConversion = await traitementMedia.genererPreviewImage(mq, pathConsignation, message, optsConversion)
  debug("Fin traitement preview, resultat : %O", resultatConversion)

  const {nbFrames, conversions} = resultatConversion
  const metadataImage = resultatConversion.metadataImage || {}

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

  mq.transmettreTransactionFormattee(transactionAssocier, 'GrosFichiers', {action: 'associerConversions', ajouterCertificat: true})
    .catch(err=>{
      console.error("ERROR media.genererPreviewImage Erreur association conversions d'image : %O", err)
    })
}

async function genererPreviewVideo(mq, pathConsignation, message) {
  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var opts = {}

  // Verifier si la commande est expiree
  if(mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
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
  const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(mq, hachageFichier, message)

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
  if(mq.estExpire(message, {expiration: EXPIRATION_COMMANDE_TRANSCODAGE})) {
    console.warn("WARN media.traiterCommandeTranscodage Commande expiree, on l'ignore : %O", message)
    return
  }

  return traiterCommandeTranscodage(mq, pathConsignation, message)
    .catch(err=>{
      console.error("media._traiterCommandeTranscodage ERROR %s: %O", message.fuuid, err)
    })
}

async function _indexerDocumentContenu(mq, pathConsignation, message) {
  debug("Traitement _indexerDocumentContenu : %O", message)

  // Verifier si la commande est expiree
  if(mq.estExpire(message, {expiration: EXPIRATION_MESSAGE_DEFAUT})) {
    console.warn("WARN media.indexerDocumentContenu Commande expiree, on l'ignore : %O", message)
    return
  }

  const fuuid = message.fuuid
  const {cleDechiffree, informationCle, clesPubliques} = await recupererCle(mq, fuuid, message)
  const optsConversion = {urlServeurIndex, cleSymmetrique: cleDechiffree, metaCle: informationCle}
  await traitementMedia.indexerDocument(mq, pathConsignation, message, optsConversion)

  const commandeResultat = { ok: true, fuuid }
  await mq.transmettreCommande(
    "GrosFichiers",
    commandeResultat,
    {action: 'confirmerFichierIndexe', ajouterCertificat: true, nowait: true}
  )
}

async function recupererCle(mq, hachageFichier, permission) {
  const liste_hachage_bytes = [hachageFichier]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  // const chainePem = mq.pki.getChainePems()
  const domaine = 'MaitreDesCles',
        action = 'dechiffrage'
  const requete = {liste_hachage_bytes, permission}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await mq.transmettreRequete(domaine, requete, {action, ajouterCertificat: true, decoder: true})
  debug("Reponse requete dechiffrage : %O", reponseCle)
  if(reponseCle.acces !== '1.permis') {
    return {err: reponseCle.acces, msg: `Erreur dechiffrage cle pour generer preview de ${hachageFichier}`}
  }
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  // Dechiffrer cle recue
  const informationCle = reponseCle.cles[hachageFichier]
  const cleChiffree = informationCle.cle
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  // Demander cles publiques pour chiffrer preview
  const domaineActionClesPubliques = 'MaitreDesCles.certMaitreDesCles'
  const reponseClesPubliques = await mq.transmettreRequete(domaineActionClesPubliques, {})
  const clesPubliques = [reponseClesPubliques.certificat, [mq.pki.ca]]

  return {cleDechiffree, informationCle, clesPubliques}
}

module.exports = {GenerateurMedia}
