const debug = require('debug')('millegrilles:fichiers:cryptoUtils')
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const forge = require('node-forge');
const {Transform} = require('stream')
const multibase = require('multibase')
const { creerCipher, preparerCommandeMaitrecles } = require('@dugrema/millegrilles.common/lib/chiffrage')

const AES_ALGORITHM = 'aes-256-cbc';  // Meme algorithme utilise sur MG en Python
const RSA_ALGORITHM = 'RSA-OAEP';

function decrypter(sourceCryptee, destination, cleSecreteDecryptee, iv, opts) {
  if(!opts) opts = {}

  let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv, opts)
  return _decrypter(sourceCryptee, destination, cryptoStream, opts)
}

function decrypterGCM(sourceCryptee, destination, cleSecreteDecryptee, iv, tag, opts) {
  if(!opts) opts = {}
  const params = {sourceCryptee, destination, cleSecreteDecryptee, iv, tag, opts}
  // debug("DecrypterGCM params : %O", params)

  let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv, {...opts, tag})
  return _decrypter(sourceCryptee, destination, cryptoStream, opts)
}

function gcmStreamReaderFactory(sourceCrypteePath, cleSecreteDecryptee, iv, tag, opts) {
  /* Genere un objet qui agit comme "StreamReader" (e.g. reader.pipe(writer)) */
  const factory = _ => {
    let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv, {...opts, tag})
    let readStream = fs.createReadStream(sourceCrypteePath)
    readStream.pipe(cryptoStream.reader)
    readStream.on('error', err=>{
      console.error("Erreur ouverture/lecture fichier : %O", err)
      cryptoStream.writer.emit('error', err)
    })
    return cryptoStream.writer
  }

  return factory
}

function _decrypter(sourceCryptee, destination, cryptoStream, opts) {
  let writeStream = fs.createWriteStream(destination)

  // Calculer taille et sha256 du fichier decrypte. Necessaire pour transaction.
  const sha512 = crypto.createHash('sha512')
  var sha512Hash = null
  var tailleFichier = 0

  cryptoStream.writer.on('data', chunk=>{
    sha512.update(chunk);
    tailleFichier = tailleFichier + chunk.length;
  });
  cryptoStream.writer.on('end', ()=>{
    // Comparer hash a celui du header
    sha512Hash = 'sha512_b64:' + sha512.digest('base64')
  });

  // Pipe dechiffrage -> writer
  cryptoStream.writer.pipe(writeStream)

  let readStream = fs.createReadStream(sourceCryptee)

  return new Promise((resolve, reject)=>{
    writeStream.on('close', ()=>{
      resolve({tailleFichier, sha512Hash});
    });
    writeStream.on('error', err=>{
      console.error("cryptoUtils._decrypter writeStream Erreur ecriture dechiffrage fichier %O", err);
      reject(err)
    })

    readStream.on('error', err=>{
      console.error("cryptoUtils._decrypter readStream Erreur lecture fichier pour dechiffrage %O", err);
      reject(err)
    })

    // Lancer le traitement du fichier
    readStream.pipe(cryptoStream.reader)
  })
}

function getDecipherPipe4fuuid(cleSecrete, iv, opts) {
  if(!opts) opts = {}
  // On prepare un decipher pipe pour decrypter le contenu.

  let ivBuffer = Buffer.from(multibase.decode(iv));

  let decryptedSecretKey;
  if(typeof cleSecrete === 'string') {
    // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
    if( opts.cleFormat !== 'hex' ) {
      decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
      decryptedSecretKey = decryptedSecretKey.toString('utf8');
    } else {
      decryptedSecretKey = cleSecrete
    }

    // debug("**** DECRYPTED SECRET KEY **** : %O", decryptedSecretKey)
    var typedArray = new Uint8Array(decryptedSecretKey.match(/[\da-f]{2}/gi).map(function (h) {
     return parseInt(h, 16)
    }));

    decryptedSecretKey = typedArray;
  } else {
    decryptedSecretKey = cleSecrete;
  }

  // Creer un decipher stream
  const transformStream = new Transform()
  var ivLu = true; //opts.tag?true:false
  transformStream._transform = (chunk, encoding, next) => {
    // debug("Chunk taille : %s, encoding : %s", chunk.length, encoding)

    if(!ivLu) {
      ivLu = true

      // Verifier le iv
      const ivExtrait = chunk.slice(0, 16).toString('base64')
      if(ivExtrait !== iv) {
        console.error('cryptoUtils.decrypter: IV ne correspond pas, chunk length : %s, IV lu : %s, IV attendu : %s', chunk.length, ivExtrait, iv)
        return next('cryptoUtils.decrypter: IV ne correspond pas')  // err
      }
      // Retirer les 16 premiers bytes (IV) du fichier dechiffre
      chunk = chunk.slice(16)
    }
    transformStream.push(chunk)
    next()
  }

  var decipher = null
  if(opts.tag) {
    const bufferTag = Buffer.from(multibase.decode(opts.tag))
    decipher = crypto.createDecipheriv('aes-256-gcm', decryptedSecretKey, ivBuffer)
    decipher.setAuthTag(bufferTag)
  } else {
    decipher = crypto.createDecipheriv('aes-256-cbc', decryptedSecretKey, ivBuffer)
  }

  decipher.pipe(transformStream)

  transformStream.close = _ => {decipher.end()}

  return {reader: decipher, writer: transformStream}
}

function decrypterSymmetrique(contenuCrypte, cleSecrete, iv) {
  return new Promise((resolve, reject)=>{

    // Dechiffrage avec node-forge
    let cleSecreteBuffer = forge.util.decode64(cleSecrete.toString('base64'));
    let ivBuffer = forge.util.decode64(iv);

    var decipher = forge.cipher.createDecipher('AES-CBC', cleSecreteBuffer);
    decipher.start({iv: ivBuffer});
    let bufferContenu = forge.util.createBuffer(forge.util.decode64(contenuCrypte));
    decipher.update(bufferContenu);
    var result = decipher.finish(); // check 'result' for true/false

    if(!result) {
      reject("Erreur dechiffrage");
    }
    let output = decipher.output;
    contenuDecrypteString = output.data.toString('utf8');

    // Enlever 16 bytes pour IV
    contenuDecrypteString = contenuDecrypteString.slice(16);

    resolve(contenuDecrypteString)
  })
}

async function chargerCleDechiffrage(mq, hachage_bytes) {
  const liste_hachage_bytes = [hachage_bytes]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const domaine = 'MaitreDesCles', action = 'dechiffrage'
  const requete = {liste_hachage_bytes}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await mq.transmettreRequete(domaine, requete, {action, ajouterCertificat: true})
  if(!reponseCle.cle) {
    return {err: reponseCle.acces, msg: `Erreur dechiffrage cle pour generer preview de ${message.fuuid}`}
  }
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  // Dechiffrer cle recue
  const informationCle = reponseCle.cles[hachage_bytes]
  const cleChiffree = informationCle.cle
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  // Demander cles publiques pour chiffrer video transcode
  const domaineActionClesPubliques = 'MaitreDesCles.certMaitreDesCles'
  const reponseClesPubliques = await mq.transmettreRequete(domaineActionClesPubliques, {})
  const clesPubliques = [reponseClesPubliques.certificat, [mq.pki.ca]]

  // opts = {cleSymmetrique: cleDechiffree, iv: informationCle.iv, clesPubliques}
  return {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}
}

async function chargerCleDechiffragePermission(mq, hachage_bytes, permission) {
  const liste_hachage_bytes = [hachage_bytes]

  // Ajouter chaine de certificats pour indiquer avec quelle cle re-chiffrer le secret
  const domaine = 'MaitreDesCles', action = 'dechiffrage'
  const requete = {liste_hachage_bytes, permission}
  debug("Nouvelle requete dechiffrage cle a transmettre : %O", requete)
  const reponseCle = await mq.transmettreRequete(domaine, requete, {action, ajouterCertificat: true})
  if(reponseCle.code !== 1) {
    debug("chargerCleDechiffragePermission Erreur demande cle dechiffrage : %O", reponseCle)
    return {err: reponseCle.err, msg: `Erreur dechiffrage cle de ${hachage_bytes}`}
  }
  debug("Reponse cle re-chiffree pour fichier : %O", reponseCle)

  // Dechiffrer cle recue
  const informationCle = reponseCle.cles[hachage_bytes]
  const cleChiffree = informationCle.cle
  const cleDechiffree = await mq.pki.decrypterAsymetrique(cleChiffree)

  // Demander cles publiques pour chiffrer video transcode
  const actionRequeteCerts = 'certMaitreDesCles'
  const reponseClesPubliques = await mq.transmettreRequete(domaine, {}, {action: actionRequeteCerts})
  const clesPubliques = [reponseClesPubliques.certificat, [mq.pki.ca]]

  // opts = {cleSymmetrique: cleDechiffree, iv: informationCle.iv, clesPubliques}
  return {cleSymmetrique: cleDechiffree, metaCle: informationCle, clesPubliques}
}

async function creerOutputstreamChiffrage(certificatsPem, identificateurs_document, domaine, opts) {
  opts = opts || {}

  const cipher = await creerCipher()

  const transformStream = new Transform()
  transformStream.byteCount = 0
  transformStream._transform = (chunk, encoding, next) => {
    const cipherChunk = cipher.update(chunk)
    transformStream.push(cipherChunk)
    transformStream.byteCount += cipherChunk.length
    next()
  }
  transformStream.resultat = null

  transformStream.on('end', async _ =>{
    const infoChiffrage = await cipher.finish()
    const meta = infoChiffrage.meta

    // Preparer commande MaitreDesCles
    transformStream.commandeMaitredescles = await preparerCommandeMaitrecles(
      certificatsPem, infoChiffrage.password, domaine,
      meta.hachage_bytes, meta.iv, meta.tag,
      identificateurs_document,
      opts
    )
    debug("Resultat chiffrage disponible : %O", transformStream.commandeMaitredescles)
  })

  return transformStream
}

async function getCertificatsChiffrage(mq) {
  const domaineActionClesPubliques = 'MaitreDesCles.certMaitreDesCles'
  const reponseClesPubliques = await mq.transmettreRequete(domaineActionClesPubliques, {})
  const clesPubliques = [reponseClesPubliques.certificat, [reponseClesPubliques.certificat_millegrille]]
  return clesPubliques
}

module.exports = {
  decrypter, getDecipherPipe4fuuid, decrypterSymmetrique, decrypterGCM,
  gcmStreamReaderFactory, chargerCleDechiffrage, chargerCleDechiffragePermission, creerOutputstreamChiffrage,
  getCertificatsChiffrage,
}
