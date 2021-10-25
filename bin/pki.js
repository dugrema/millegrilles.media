const debug = require('debug')('millegrilles:utilpki')
const crypto = require('crypto');
const forge = require('node-forge');
const stringify = require('json-stable-stringify');
const fs = require('fs');
const path = require('path');
const tmp = require('tmp');
const {extraireExtensionsMillegrille} = require('@dugrema/millegrilles.common/lib/forgecommon')

const REPERTOIRE_CERTS_TMP = tmp.dirSync().name;  //'/tmp/consignationfichiers.certs';
console.info("Repertoire temporaire certs : %s", REPERTOIRE_CERTS_TMP);

const PEM_CERT_DEBUT = '-----BEGIN CERTIFICATE-----';
const PEM_CERT_FIN = '-----END CERTIFICATE-----';
// const ROLES_PERMIS_SSL = ['web_protege', 'domaines', 'maitrecles', 'monitor', 'prive', 'backup', 'core']

class PKIUtils {
  // Classe qui supporte des operations avec certificats et cles privees.

  constructor(certs) {
    this.idmg = null;

    // Cle pour cert
    this.cle = certs.key;
    this.password = certs.password;  // Mot de passe pour la cle, optionnel

    // Contenu format texte PEM
    this.chainePEM = certs.cert;
    this.hotePEM = certs.hote || certs.cert;  // Chaine XS pour connexion middleware
    this.hoteCA = certs.hoteMillegrille || certs.millegrille;
    this.ca = certs.millegrille;

    this.caIntermediaires = [];

    // Objets node-forge
    this.certPEM = null;
    this.cleForge = null; // Objet cle charge en memoire (forge)
    this.cert = null;     // Objet certificat charge en memoire (forge)
    this.caStore = null;  // CA store pour valider les chaines de certificats

    this.cacheCertsParFingerprint = {};

  }

  chargerCertificatPEM(pem) {
    let parsedCert = forge.pki.certificateFromPem(pem);
    return parsedCert;
  }

  async chargerPEMs(certs) {
    // Preparer repertoire pour sauvegarder PEMS
    fs.mkdir(REPERTOIRE_CERTS_TMP, {recursive: true, mode: 0o700}, e=>{
      if(e) {
        throw new Error(e);
      }
    });

    // Charger le certificat pour conserver commonName, fingerprint
    await this.chargerCertificats(certs);
    this._verifierCertificat();

    let cle = this.cle;
    if(this.password) {
      // console.debug("Cle chiffree");
      this.cleForge = forge.pki.decryptRsaPrivateKey(cle, this.password);
      // Re-exporter la cle en format PEM dechiffre (utilise par RabbitMQ)
      this.cle = forge.pki.privateKeyToPem(this.cleForge);
    } else {
      this.cleForge = forge.pki.privateKeyFromPem(cle);
    }
    // console.debug(this.cleForge);

  }

  _verifierCertificat() {
    this.getFingerprint();
  }

  async chargerCertificats(certPems) {

    // Charger certificat local
    var certs = splitPEMCerts(certPems.cert);
    // console.debug(certs);
    this.certPEM = certs[0];

    // console.debug(this.certPEM);
    let parsedCert = this.chargerCertificatPEM(this.certPEM);
    // console.debug(parsedCert);

    this.idmg = parsedCert.issuer.getField("O").value;
    console.debug("IDMG %s", this.idmg);

    this.fingerprint = getCertificateFingerprint(parsedCert);
    this.cert = parsedCert;
    this.commonName = parsedCert.subject.getField('CN').value;

    // Sauvegarder certificats intermediaires
    const certsChaineCAList = certs.slice(1);
    const certsIntermediaires = [];
    for(let idx in certsChaineCAList) {
      var certIntermediaire = certsChaineCAList[idx];
      let intermediaire = this.chargerCertificatPEM(certIntermediaire);
      certsIntermediaires.push(intermediaire);
    }
    this.caIntermediaires = certsIntermediaires;

    // console.log("Certificat du noeud. Sujet CN: " +
    // this.commonName + ", fingerprint: " + this.fingerprint);

    // Creer le CA store pour verifier les certificats.
    let parsedCACert = this.chargerCertificatPEM(this.ca);
    this.caStore = forge.pki.createCaStore([parsedCACert]);
    // console.debug("CA store");
    // console.debug(this.caStore);

  }

  getFingerprint() {
    return this.fingerprint;
  }

  getCommonName() {
    return this.commonName;
  }

  signerTransaction(transaction) {

    let signature = 'N/A';
    const sign = crypto.createSign('SHA512');

    // Stringify en json trie
    let transactionJson = stringify(transaction);
    // console.log("Message utilise pour signature: " + transactionJson);

    // Creer algo signature et signer
    sign.write(transactionJson);
    let parametresSignature = {
      "key": this.cle,
      "padding": crypto.constants.RSA_PKCS1_PSS_PADDING
    }
    signature = sign.sign(parametresSignature, 'base64');

    return signature;
  }

  hacherTransaction(transaction, opts) {
    if(!opts) opts = {};

    let hachage_transaction = 'N/A';
    const hash = crypto.createHash(opts.hachage || 'sha256');

    // Copier transaction sans l'entete
    let copie_transaction = {};
    for(let elem in transaction) {
      if (elem != 'en-tete' && ! elem.startsWith('_')) {
        copie_transaction[elem] = transaction[elem];
      }
    }

    // Stringify en json trie
    let transactionJson = stringify(copie_transaction);
    // console.log("Message utilise pour hachage: " + transactionJson);

    // Creer algo signature et signer
    hash.write(transactionJson);
    //hash.end();

    hachage_transaction = hash.digest('base64')

    return hachage_transaction;
  }

  preparerMessageCertificat() {
    // Retourne un message qui peut etre transmis a MQ avec le certificat
    // utilise par ce noeud. Sert a verifier la signature des transactions.
    let certificatBuffer = this.chainePEM;

    let transactionCertificat = {
        evenement: 'pki.certificat',
        fingerprint: this.fingerprint,
        certificat_pem: certificatBuffer,
    }

    return transactionCertificat;
  }

  getFingerprintFromForge(cert) {
    return forge.md.sha1.create().update(forge.asn1.toDer(forge.pki.certificateToAsn1(cert)).getBytes()).digest().toHex();
  }

  // Sauvegarde un message de certificat en format JSON
  async sauvegarderMessageCertificat(message, fingerprint) {
    let fichierExiste = fingerprint && await new Promise((resolve, reject)=>{
      if(fingerprint) {
        // Verifier si le fichier existe deja
        let fichier = path.join(REPERTOIRE_CERTS_TMP, fingerprint + '.json');
        fs.access(fichier, fs.constants.F_OK, (err) => {
          let existe = ! err;
          // console.debug("Fichier existe ? " + existe);
          resolve(existe);
        });
      } else {
        resolve(false);
      }
    });

    if( ! fichierExiste ) {
      let json_message = JSON.parse(message);
      let certificat_pem = json_message.certificat_pem;

      let certificat = this.chargerCertificatPEM(certificat_pem);
      let fingerprintCalcule = getCertificateFingerprint(certificat);
      let fichier = path.join(REPERTOIRE_CERTS_TMP, fingerprintCalcule + '.json');

      // Sauvegarder sur disque
      fs.writeFile(fichier, message, ()=>{
        // console.debug("Fichier certificat " + fingerprintCalcule + ".json sauvegarde");
      });
    } else {
      // console.debug("Fichier certificat existe deja : " + fingerprint + ".json");
    }
  }

  // Charge la chaine de certificats pour ce fingerprint
  async getCertificate(fingerprint) {
    let certificat = this.cacheCertsParFingerprint[fingerprint];
    if( ! certificat ) {
      // Verifier si le certificat existe sur le disque
      certificat = await new Promise((resolve, reject)=>{
        let fichier = path.join(REPERTOIRE_CERTS_TMP, fingerprint + '.json');
        let pem = fs.readFile(fichier, (err, data)=>{
          if(err) {
            return reject(err);
          }

          if(!data) {
            return resolve(); // No data
          }

          try {
            let messageJson = JSON.parse(data.toString());
            let pem = messageJson.certificat_pem;
            let intermediaires = messageJson.certificats_intermediaires;

            if( ! intermediaires ) {
              // On va tenter d'introduire le certificat de MilleGrille local
              intermediaires = this.caIntermediaires;
            }

            let certificat = this.chargerCertificatPEM(pem);

            let chaine = [certificat, ...intermediaires];
            // console.debug("CHAINE");
            // console.debug(chaine);

            let fingerprintCalcule = getCertificateFingerprint(certificat);
            if(fingerprintCalcule !== fingerprint) {
              // Supprimer fichier invalide
              fs.unlink(fichier, ()=>{});
              return reject('Fingerprint ' + fingerprintCalcule + ' ne correspond pas au fichier : ' + fingerprint + '.json. Fichier supprime.');
            }

            // Valider le certificat avec le store
            let valide = true;
            try {
              forge.pki.verifyCertificateChain(this.caStore, chaine);
            } catch (err) {
              valide = false;
              console.log('Certificate verification failure: ' +
                JSON.stringify(err, null, 2));
            }

            if(valide) {
              this.cacheCertsParFingerprint[fingerprintCalcule] = chaine;
            } else {
              certificat = null;
            }

            resolve(chaine);
          } catch(err) {
            console.error("Erreur traitement certificat");
            console.error(err);
            reject(err);
          }

        });
      })
      .catch(err=>{
        if(err.code === 'ENOENT') {
          // Fichier non trouve, ok.
        } else {
          console.error("Erreur acces fichier cert");
          console.error(err);
        }
      });
    }
    return certificat;
  }

  // Verifie la signature d'un message
  // Retourne vrai si le message est valide, faux si invalide.
  async verifierSignatureMessage(message, certificat) {
    // Par defaut utiliser le certificat deja charge
    if(!certificat) {
      let certificatChaine = await this.getCertificate(fingerprint);
      if( ! certificatChaine ) {
        // console.debug("Certificat inconnu : " + fingerprint);
        throw new CertificatInconnu("Certificat inconnu : " + fingerprint);
      }
      certificat = certificatChaine[0];
    }

    return verifierSignatureMessage(message, certificat)
  }

  async decrypterAsymetrique(contenuSecret) {
    // console.debug("CONTENU SECRET CHIFFRE : " + contenuSecret)
    let cleSecrete = forge.util.decode64(contenuSecret);

    // Decrypter la cle secrete avec notre cle privee
    var decryptedSecretKey = this.cleForge.decrypt(cleSecrete, 'RSA-OAEP', {
      md: forge.md.sha256.create(),
      mgf1: {
        md: forge.md.sha256.create()
      }
    });
    // console.debug("Cle secrete decryptee string " + decryptedSecretKey);
    // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
    // console.debug("Cle secrete decryptee (" + decryptedSecretKey.length + ") bytes");
    // console.debug(decryptedSecretKey);
    return decryptedSecretKey;
  }

};

// Verifie la signature d'un message
// Retourne vrai si le message est valide, faux si invalide.
async function f(message, certificat) {
  let fingerprint = message['en-tete']['certificat'];
  let signatureBase64 = message['_signature'];
  let signature = Buffer.from(signatureBase64, 'base64');

  let messageFiltre = {};
  for(let cle in message) {
    if( ! cle.startsWith('_') ) {
      messageFiltre[cle] = message[cle];
    }
  }
  // Stringify en ordre (stable)
  messageFiltre = stringify(messageFiltre);

  let keyLength = certificat.publicKey.n.bitLength();
  // Calcul taille salt:
  // http://bouncy-castle.1462172.n4.nabble.com/Is-Bouncy-Castle-SHA256withRSA-PSS-compatible-with-OpenSSL-RSA-PSS-padding-with-SHA256-digest-td4656843.html
  let saltLength = (keyLength - 512) / 8 - 2;
  // console.debug("Salt length: " + saltLength);

  var pss = forge.pss.create({
    md: forge.md.sha512.create(),
    mgf: forge.mgf.mgf1.create(forge.md.sha512.create()),
    saltLength,
    // optionally pass 'prng' with a custom PRNG implementation
  });
  var md = forge.md.sha512.create();
  md.update(messageFiltre, 'utf8');

  try {
    var publicKey = certificat.publicKey;
    let valide = publicKey.verify(md.digest().getBytes(), signature, pss);
    return valide;
  } catch (err) {
    console.debug("Erreur verification signature");
    console.debug(err);
    return false;
  }

}

function getCertificateFingerprint(cert) {
  const fingerprint = forge.md.sha1.create()
    .update(forge.asn1.toDer(forge.pki.certificateToAsn1(cert)).getBytes())
    .digest()
    .toHex();
  return fingerprint;
}

function splitPEMCerts(certs) {
  var splitCerts = certs.split(PEM_CERT_DEBUT).map(c=>{
    return PEM_CERT_DEBUT + c;
  });
  return splitCerts.slice(1);
}

class ValidateurSignature {

  constructor(caCertPEM) {
    this.caStore = forge.pki.createCaStore([]);

    if(caCertPEM) {
      this.caStore.addCertificate(caCertPEM)
    }

    // Creer un cache des chaines valides
    // Permet d'eviter de verifier les chaines a chaque fois, on passe
    // directement a la verification de la signature.
    this.chainCache = {};
  }

  ajouterCertificatCA(caCertPEM) {
    // let parsedCACert = pki.chargerCertificatPEM(caCertPEM);
    this.caStore.addCertificate(caCertPEM);
  }

  verifierChaine(chaineCertsPEM) {
    const chainePki = chaineCertsPEM.reduce((liste, certPEM)=>{
      let parsedCert = forge.pki.certificateFromPem(certPEM)
      liste.push(parsedCert)
      return liste
    }, []);

    if(forge.pki.verifyCertificateChain(this.caStore, chainePki)) {

      // Sauvegarder la chaine de certificats dans le cache memoire
      const certificatFeuille = chainePki[0];
      const fingerprint = getCertificateFingerprint(certificatFeuille);
      this.chainCache[fingerprint] = chainePki;

      return true;
    }
    return false;

  }

  async verifierSignature(message, chaineCerts) {
    if(chaineCerts) {
      if(this.verifierChaine(chaineCerts)) {
        // Charger le certificat PEM en format node-forge pour verifier signature
        const certificat = forge.pki.certificateFromPem(chaineCerts[0]);
        return await verifierSignatureMessage(message, certificat);
      } else {
        return false;
      }
    } else {
      // On utilise le cache, permet d'eviter de verifier la chaine
      const fingerprint = message['en-tete'].certificat;
      const certificat = this.chainCache[fingerprint][0];
      return await verifierSignatureMessage(message, certificat);
    }
  }

  isChainValid(fingerprint) {
    if(this.chainCache[fingerprint]) {
      return true;
    }
    return false;
  }

}

// Retourne de l'information sur le certificat et un flag
// pour indiquer si le certificat est valide pour acceder a des ressources
// params req, res, next proviennent d'express
// rend disponible: {idmg: str, protege: bool, prive: bool}
function verificationCertificatSSL(req, res, next) {
  const peerCertificate = req.connection.getPeerCertificate();

  if( peerCertificate && peerCertificate.subject ) {
    debug("PEER Certificate:\n%O", peerCertificate);
  } else {
    debug("PEER (client) cert manquant")
  }

  if ( ! peerCertificate || ! peerCertificate.subject ) {
    // DEV
    if ( process.env.DISABLE_SSL_AUTH && process.env.IDMG ) {
      req.autorisationMillegrille = {
        idmg:process.env.IDMG, protege: true, prive: true, public: true, securite: '3.protege'
      }
      debug("Fake autorisation %s:\n%O", req.url, req.autorisationMillegrille)
      return next()
    } else {
      console.error("Erreur cert SSL manquant, IDMG non fourni");
      res.sendStatus(403);  // Access denied
      return;
    }
  }

  const typeCertificat = peerCertificate.subject.OU;
  if( ['nginx', 'prive', 'monitor'].includes(typeCertificat) ) {
    debug("Certificat nginx, valider l'autorisation d'acces via headers/contenu\nHeaders\n%O",
      req.headers)

    const headers = req.headers
    const nginxVerified = headers.verified && headers.verified !== 'NONE'
    const userAuthentifie = headers['x-user-id']?true:false

    if(nginxVerified) {
      debug("NGINX a verifie le certificat client, on utilise l'information des headers")
      // ....
    } else {
      // Le certificat n'a pas ete fourni en mode "client ssl", il faut aller chercher dans
      // le contenu des headers et valider la signature avant de proceder
      const certificatsHeader = req.headers.certificat
      if(!certificatsHeader) {
        if(!userAuthentifie) {
          // Usager n'est pas authentifie. Refuser l'acces
          return res.sendStatus(403)
        }

        debug("Requete url:%s aucun certificat SSL fourni, verifier si acces prive ou public", req.url)
        const idmg = peerCertificate.subject.O

        if(req.headers['user-securite']) {
          const securite = req.headers['user-securite']
          // Acces prive aux fichiers
          req.autorisationMillegrille = {
            idmg,
            protege: '3.protege'===securite?true:false,
            prive: ['3.protege', '2.prive'].includes(securite)?true:false,
            public: true,
            securite,
          }
        } else {
          // Acces public aux fichiers de la MilleGrille qui correspond a NGINX
          req.autorisationMillegrille = {
            idmg,
            protege: false,
            prive: false,
            public: true,
            securite: '1.public'
          }
        }

        // Acces limite (prive ou public) est autorise
        debug("Acces limite pour url %s\n%O", req.url, req.autorisationMillegrille)
        return next()

        // res.sendStatus(403);  // Access denied
      }

      const chaineCerts = certificatsHeader.split(';').join('\n')
      const listeCerts = splitPEMCerts(chaineCerts)
      let parsedCert = forge.pki.certificateFromPem(listeCerts[0]);
      const idmg = parsedCert.issuer.getField('O').value
      debug("IDMG certificat client : %s, certs:\n%O", idmg, parsedCert.issuer)

      // Valider la chaine de certificats
      const fctRabbitMQParIdmg = req.fctRabbitMQParIdmg
      const rabbitMQ = fctRabbitMQParIdmg(idmg)
      debug("Certificat CA: %O", rabbitMQ.pki.ca)
      const validateurSignature = new ValidateurSignature(rabbitMQ.pki.ca)
      var chaineCertValide = false
      try {
        chaineCertValide = validateurSignature.verifierChaine(listeCerts)
        debug("Validite de chaine cert client : %s", chaineCertValide)
      } catch(err) {
        console.error("Erreur validation chaine de certificats: %O", err)
      }

      if(!chaineCertValide) {
        console.error("Requete url:%s refuse, aucun certificat SSL fourni", req.url)
        res.sendStatus(403);  // Access denied
        return;
      }

      // Injecter les certificats, validateur de signature
      req.certificat = listeCerts
      req.validateurSignature = validateurSignature
    }

  }
  else {
    // Pas un usager (via nginx), verifier si c'est un serveur avec droit
    // d'acces direct (instance prive, protege ou secure)

    // Extraire certificat DER
    const raw = peerCertificate.raw
    const rawString = String.fromCharCode.apply(null, raw)
    const asn1Obj = forge.asn1.fromDer(rawString)
    const cert = forge.pki.certificateFromAsn1(asn1Obj)

    // Verifier extensions (exchanges)
    const extensions = extraireExtensionsMillegrille(cert)
    const exchanges = extensions.niveauxSecurite
    if(exchanges.includes('2.prive') || exchanges.includes('3.protege') || exchanges.includes('4.secure')) {
      // Ok, certificat correct
    } else {
      console.error("Niveau de securite non supporte %O, acces refuse" + exchanges);
      res.sendStatus(403);  // Access denied
      return;
    }
  }

  // Utilisation du issuer pour identifier le idmg -> dans le cas d'un XS,
  // le issuer est fiable parce qu'il est signe par la millegrille locale.
  const idmg = peerCertificate.issuer.O;

  const protege = typeCertificat === 'coupdoeil'; // TODO: verifier si exchange protege
  const securite = protege?'3.protege':'2.prive'

  // Sauvegarder l'information d'autorisation de MilleGrille sur objet req.
  req.autorisationMillegrille = {
    idmg, protege: true, prive: true, public: true, securite: '3.protege'
  }

  next();
}

class CertificatInconnu extends Error {
  constructor(message) {
    super(message);
    this.inconnu = true;
  }
}

module.exports = {PKIUtils, ValidateurSignature, verificationCertificatSSL};
