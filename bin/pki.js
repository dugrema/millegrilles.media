const debug = require('debug')('media:pki')
const forge = require('@dugrema/node-forge');
const { extraireExtensionsMillegrille } = require('@dugrema/millegrilles.utiljs/src/forgecommon')

const PEM_CERT_DEBUT = '-----BEGIN CERTIFICATE-----'
const PEM_CERT_FIN = '-----END CERTIFICATE-----'
const L2PRIVE = '2.prive'

async function recupererCle(mq, hachageFichier) {
  const liste_hachage_bytes = [hachageFichier]
  // Note: permission n'est plus requise - le certificat media donne acces a toutes les cles (domaine=GrosFichiers)
  // Le message peut avoir une permission attachee
  // if(permission.permission) permission = permission.permission

  // Demander cles publiques pour rechiffrage
  const reponseClesPubliques = await mq.transmettreRequete(
    'MaitreDesCles', {}, {action: 'certMaitreDesCles', ajouterCertificat: true})
  debug("Recuperer cle : maitre des cles = %O", reponseClesPubliques)

  if(reponseClesPubliques.ok === false || !reponseClesPubliques.certificat) {
    throw new Error("Erreur chargement reference maitre des cles")
  }  const clesPubliques = [reponseClesPubliques.certificat]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const domaine = 'MaitreDesCles',
        action = 'dechiffrage'
  const requete = {liste_hachage_bytes}  //, permission}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await mq.transmettreRequete(domaine, requete, {action, ajouterCertificat: true, decoder: true})
  debug("Reponse requete dechiffrage : %O", reponseCle)
  if(reponseCle.acces !== '1.permis') {
    return {err: reponseCle.acces, msg: `Erreur dechiffrage cle pour generer preview de ${hachageFichier}`}
  }
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  // Dechiffrer cle recue
  const metaCle = reponseCle.cles[hachageFichier]
  const cleChiffree = metaCle.cle
  const cleSymmetrique = await mq.pki.decrypterAsymetrique(cleChiffree)

  return {cleSymmetrique, metaCle, clesPubliques}
}

// Retourne de l'information sur le certificat et un flag
// pour indiquer si le certificat est valide pour acceder a des ressources
// params req, res, next proviennent d'express
// rend disponible: {idmg: str, protege: bool, prive: bool}
function verificationCertificatSSL(req, res, next) {
  const peerCertificate = req.connection.getPeerCertificate();

  if ( process.env.DISABLE_SSL_AUTH ) {
    const mq = req.amqpdao
    req.autorisationMillegrille = {
      idmg: mq.pki.idmg, secure: true, protege: true, prive: true, public: true, roles: ['media']
    }
    debug("Fake autorisation (flag: DISABLE_SSL_AUTH)")
    return next()
  }

  if( peerCertificate && peerCertificate.subject ) {
    debug("PEER Certificate:\n%O", peerCertificate);
  } else {
    debug("PEER (client) cert manquant")
    // console.error("Erreur cert SSL manquant, IDMG non fourni");
    return res.sendStatus(403)  // Access denied
  }

  debug("Valider l'autorisation d'acces via headers/contenu\nHeaders\n%O", req.headers)
  const headers = req.headers
  const nginxVerified = headers.verified && headers.verified !== 'NONE'

  if(!nginxVerified) {
      debug("Echec de verification NGINX, acces refuse");
      return res.sendStatus(403)  // Access denied
  }

  // Extraire certificat DER
  const raw = peerCertificate.raw
  const rawString = String.fromCharCode.apply(null, raw)
  const asn1Obj = forge.asn1.fromDer(rawString)
  const cert = forge.pki.certificateFromAsn1(asn1Obj)

  // Verifier extensions (exchanges)
  const extensions = extraireExtensionsMillegrille(cert)
  const exchanges = extensions.niveauxSecurite

  const secure = exchanges.includes('4.secure'),
        protege = exchanges.includes('3.protege'),
        prive = exchanges.includes('2.prive'),
        public = exchanges.includes('1.public')

  if(public || prive || protege || secure) {
      // Ok, certificat correct
  } else {
      debug("Niveau de securite non supporte %O, acces refuse" + exchanges);
      return res.sendStatus(403)  // Access denied
  }

  // Utilisation du issuer pour identifier le idmg -> dans le cas d'un XS,
  // le issuer est fiable parce qu'il est signe par la millegrille locale.
  const idmg = peerCertificate.issuer.O

  // Sauvegarder l'information d'autorisation de MilleGrille sur objet req.
  req.autorisationMillegrille = {
    idmg, secure, protege, prive, public, ...extensions,
  }

  next()
}

module.exports = {recupererCle, verificationCertificatSSL};
