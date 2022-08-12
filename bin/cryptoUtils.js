const debug = require('debug')('media:cryptoUtils')
const fs = require('fs');
const crypto = require('crypto');
const {Transform} = require('stream')
// const multibase = require('multibase')
// const { creerCipher, preparerCommandeMaitrecles } = require('@dugrema/millegrilles.common/lib/chiffrage')
const { preparerCipher, preparerDecipher, preparerCommandeMaitrecles } = require('@dugrema/millegrilles.nodejs/src/chiffrage')
const { base64 } = require('multiformats/bases/base64');
// const { dechiffrerCle } = require('@dugrema/millegrilles.utiljs/src/chiffrage.ed25519');

// const AES_ALGORITHM = 'aes-256-cbc';  // Meme algorithme utilise sur MG en Python
// const RSA_ALGORITHM = 'RSA-OAEP';

async function getDecipherPipe4fuuid(cleSecrete, iv, opts) {
  if(!opts) opts = {}
  // On prepare un decipher pipe pour decrypter le contenu.

  if(typeof(iv) === 'string') iv = base64.decode(iv)

  const decryptedSecretKey = cleSecrete  // await dechiffrerCle(cleSecrete)

  // Creer un decipher stream
  const transformStream = new Transform()
  // var ivLu = true; //opts.tag?true:false
  transformStream._transform = (chunk, encoding, next) => {
    transformStream.push(chunk)
    next()
  }

  var decipher = null
  if(opts.tag) {
    const bufferTag = base64.decode(opts.tag)
    // decipher = crypto.createDecipheriv('aes-256-gcm', decryptedSecretKey, ivBuffer)
    decipher = crypto.createDecipheriv('chacha20-poly1305', decryptedSecretKey, iv, { authTagLength: 16 })
    decipher.setAuthTag(bufferTag)
  } else {
    throw new Error("Format chiffrage non supporte")
  }

  decipher.pipe(transformStream)

  transformStream.close = _ => {decipher.end()}

  return {reader: decipher, writer: transformStream}
}

/**
 * Transform pour dechiffrer un fichier a partir d'un stream.
 * Peut etre utilise a la fois comme reader (input) et writer (vers output) avec un pipe.
 */
async function decipherTransform(key, opts) {

  // Creer decipher
  const decipher = await preparerDecipher(key, opts)

  // Transform stream qui effectue le dechiffrage
  const transformStream = new Transform()

  transformStream._transform = async (chunk, encoding, next) => {
    try {
      const chunkDechiffree = await decipher.update(chunk)
      next(null, chunkDechiffree)
    } catch(err) {
      next(err)
    }
  }
  
  transformStream._flush = async next => {
    try {
      const output = await decipher.finalize()
      next(null, output.message)
    } catch(err) {
      next(err)
    }
  }

  return transformStream
}

/**
 * 
 * @param {*} certificatsPem 
 * @param {*} identificateurs_document 
 * @param {*} domaine 
 * @param {*} certCaInfo {cert, fingerprint}
 * @param {*} opts 
 * @returns 
 */
async function creerOutputstreamChiffrage(certificatsPem, identificateurs_document, domaine, certCaInfo, opts) {
  opts = opts || {}

  const clePubliqueCa = certCaInfo.cert.publicKey.publicKeyBytes
  const cipherInfo = await preparerCipher({clePubliqueEd25519: clePubliqueCa}, opts)
  // console.debug("!!! CipherInfo: %O (opts: %O)", cipherInfo, opts)
  const cipher = cipherInfo.cipher

  const transformStream = new Transform()

  transformStream._transform = async (chunk, encoding, next) => {
    const cipherChunk = await cipher.update(chunk)
    next(null, cipherChunk)
  }

  transformStream._flush = async next => {
    try {
      const resultatChiffrage = await cipher.finalize()
      
      // Permet d'extraire les params de chiffrage comme tag, header, hachage, etc.
      transformStream.resultatChiffrage = resultatChiffrage  

      // console.debug("Resultat chiffrage : %O", resultatChiffrage)

      // Preparer commande MaitreDesCles
      transformStream.commandeMaitredescles = await preparerCommandeMaitrecles(
        certificatsPem, cipherInfo.secretKey, domaine, resultatChiffrage.hachage, identificateurs_document,
        {...opts, ...resultatChiffrage}
      )

      // Ajouter cle chiffree pour cle de millegrille
      transformStream.commandeMaitredescles.cles[certCaInfo.fingerprint] = cipherInfo.secretChiffre

      next(null, resultatChiffrage.ciphertext)  // Emettre derniers bytes
    } catch(err) {
      next(err)
    }
  }

  // transformStream.on('end', async _ =>{
  //   const infoChiffrage = await cipher.finalize()
  //   // console.debug("!!! InfoChiffrage : %O", infoChiffrage)
  //   // const meta = {iv: cipherInfo.iv, ...infoChiffrage.meta}

  //   // Preparer commande MaitreDesCles
  //   transformStream.commandeMaitredescles = await preparerCommandeMaitrecles(
  //     certificatsPem, cipherInfo.secretKey, domaine,
  //     infoChiffrage.hachage, iv, infoChiffrage.tag,
  //     identificateurs_document,
  //     opts
  //   )

  //   // Ajouter cle chiffree pour cle de millegrille
  //   transformStream.commandeMaitredescles.cles[certCaInfo.fingerprint] = cipherInfo.secretChiffre

  //   debug("Resultat chiffrage disponible : %O", transformStream.commandeMaitredescles)
  // })

  return transformStream
}

async function chiffrerMemoire(pki, fichierSrc, clesPubliques, opts) {
  opts = opts || {}
  debug("chiffrerMemoire Cles publiques pour cipher : %O", clesPubliques)

  const identificateurs_document = opts.identificateurs_document || {}
  const certCaInfo = {cert: pki.caForge, fingerprint: pki.fingerprintCa}

  // Creer cipher
  const cipherStream = await creerOutputstreamChiffrage(clesPubliques, identificateurs_document, 'GrosFichiers', certCaInfo, opts)
  
  let positionBuffer = 0
  const buffer = new Uint8Array(64*1024); // On ne devrait pas chiffrer de contenu en memoire qui depasse 1 block de cipher

  const promiseDone = new Promise(async (resolve, reject)=>{
    cipherStream.on('error', err=>reject(err))
    cipherStream.on('data', async data => {
      buffer.set(data, positionBuffer)
      positionBuffer += data.length
    })
    cipherStream.on('end', resolve)
    cipherStream.on('error', reject)
  })

  const s = fs.ReadStream(fichierSrc)
  s.pipe(cipherStream)

  await promiseDone

  debug("Resultat cipher stream : %O, buffer chiffre (len: %d): %O", cipherStream, positionBuffer, buffer)
  const {resultatChiffrage, commandeMaitredescles} = cipherStream

  let outputBuffer = buffer.slice(0, positionBuffer)
  if(opts.base64 === true) {
    // Convertir les bytes en base64
    outputBuffer = base64.encode(outputBuffer)
  }

  debug("Information chiffrage fichier : %O", resultatChiffrage)
  return {
    tailleFichier: positionBuffer,
    data: outputBuffer,
    hachage: resultatChiffrage.hachage,
    meta: resultatChiffrage,
    commandeMaitreCles: commandeMaitredescles
  }

}

module.exports = {
  getDecipherPipe4fuuid, 
  decipherTransform,
  // DecipherTransformStream,
  // gcmStreamReaderFactory,
  creerOutputstreamChiffrage,
  chiffrerMemoire,
}
