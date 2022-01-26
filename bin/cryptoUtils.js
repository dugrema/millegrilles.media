const debug = require('debug')('millegrilles:fichiers:cryptoUtils')
const fs = require('fs');
//const crypto = require('crypto');
//const {Transform} = require('stream')
const multibase = require('multibase')
// const { creerCipher, preparerCommandeMaitrecles } = require('@dugrema/millegrilles.common/lib/chiffrage')
const { getCipher, preparerCommandeMaitrecles } = require('@dugrema/millegrilles.utiljs')
const { base64 } = require('multiformats/bases/base64')

const AES_ALGORITHM = 'aes-256-cbc';  // Meme algorithme utilise sur MG en Python
const RSA_ALGORITHM = 'RSA-OAEP';

// function decrypterGCM(sourceCryptee, destination, cleSecreteDecryptee, iv, tag, opts) {
//   if(!opts) opts = {}
//   const params = {sourceCryptee, destination, cleSecreteDecryptee, iv, tag, opts}
//   // debug("DecrypterGCM params : %O", params)

//   let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv, {...opts, tag})
//   return _decrypter(sourceCryptee, destination, cryptoStream, opts)
// }

// function gcmStreamReaderFactory(sourceCrypteePath, cleSecreteDecryptee, iv, tag, opts) {
//   /* Genere un objet qui agit comme "StreamReader" (e.g. reader.pipe(writer)) */
//   const factory = _ => {
//     let cryptoStream = getDecipherPipe4fuuid(cleSecreteDecryptee, iv, {...opts, tag})
//     let readStream = fs.createReadStream(sourceCrypteePath)
//     readStream.pipe(cryptoStream.reader)
//     readStream.on('error', err=>{
//       console.error("Erreur ouverture/lecture fichier : %O", err)
//       cryptoStream.writer.emit('error', err)
//     })
//     return cryptoStream.writer
//   }

//   return factory
// }

// function _decrypter(sourceCryptee, destination, cryptoStream, opts) {
//   let writeStream = fs.createWriteStream(destination)

//   // Calculer taille et sha256 du fichier decrypte. Necessaire pour transaction.
//   const sha512 = crypto.createHash('sha512')
//   var sha512Hash = null
//   var tailleFichier = 0

//   cryptoStream.writer.on('data', chunk=>{
//     sha512.update(chunk);
//     tailleFichier = tailleFichier + chunk.length;
//   });
//   cryptoStream.writer.on('end', ()=>{
//     // Comparer hash a celui du header
//     sha512Hash = 'sha512_b64:' + sha512.digest('base64')
//   });

//   // Pipe dechiffrage -> writer
//   cryptoStream.writer.pipe(writeStream)

//   let readStream = fs.createReadStream(sourceCryptee)

//   return new Promise((resolve, reject)=>{
//     writeStream.on('close', ()=>{
//       resolve({tailleFichier, sha512Hash});
//     });
//     writeStream.on('error', err=>{
//       console.error("cryptoUtils._decrypter writeStream Erreur ecriture dechiffrage fichier %O", err);
//       reject(err)
//     })

//     readStream.on('error', err=>{
//       console.error("cryptoUtils._decrypter readStream Erreur lecture fichier pour dechiffrage %O", err);
//       reject(err)
//     })

//     // Lancer le traitement du fichier
//     readStream.pipe(cryptoStream.reader)
//   })
// }

// function getDecipherPipe4fuuid(cleSecrete, iv, opts) {
//   if(!opts) opts = {}
//   // On prepare un decipher pipe pour decrypter le contenu.

//   let ivBuffer = Buffer.from(multibase.decode(iv));

//   let decryptedSecretKey;
//   if(typeof cleSecrete === 'string') {
//     // decryptedSecretKey = Buffer.from(forge.util.binary.hex.decode(decryptedSecretKey));
//     if( opts.cleFormat !== 'hex' ) {
//       decryptedSecretKey = Buffer.from(cleSecrete, 'base64');
//       decryptedSecretKey = decryptedSecretKey.toString('utf8');
//     } else {
//       decryptedSecretKey = cleSecrete
//     }

//     // debug("**** DECRYPTED SECRET KEY **** : %O", decryptedSecretKey)
//     var typedArray = new Uint8Array(decryptedSecretKey.match(/[\da-f]{2}/gi).map(function (h) {
//      return parseInt(h, 16)
//     }));

//     decryptedSecretKey = typedArray;
//   } else {
//     decryptedSecretKey = cleSecrete;
//   }

//   // Creer un decipher stream
//   const transformStream = new Transform()
//   var ivLu = true; //opts.tag?true:false
//   transformStream._transform = (chunk, encoding, next) => {
//     // debug("Chunk taille : %s, encoding : %s", chunk.length, encoding)

//     if(!ivLu) {
//       ivLu = true

//       // Verifier le iv
//       const ivExtrait = chunk.slice(0, 16).toString('base64')
//       if(ivExtrait !== iv) {
//         console.error('cryptoUtils.decrypter: IV ne correspond pas, chunk length : %s, IV lu : %s, IV attendu : %s', chunk.length, ivExtrait, iv)
//         return next('cryptoUtils.decrypter: IV ne correspond pas')  // err
//       }
//       // Retirer les 16 premiers bytes (IV) du fichier dechiffre
//       chunk = chunk.slice(16)
//     }
//     transformStream.push(chunk)
//     next()
//   }

//   var decipher = null
//   if(opts.tag) {
//     const bufferTag = Buffer.from(multibase.decode(opts.tag))
//     decipher = crypto.createDecipheriv('aes-256-gcm', decryptedSecretKey, ivBuffer)
//     decipher.setAuthTag(bufferTag)
//   } else {
//     throw new Error("Format chiffrage non supporte")
//   }

//   decipher.pipe(transformStream)

//   transformStream.close = _ => {decipher.end()}

//   return {reader: decipher, writer: transformStream}
// }

// async function creerOutputstreamChiffrage(certificatsPem, identificateurs_document, domaine, opts) {
//   opts = opts || {}

//   const cipher = await creerCipher()

//   const transformStream = new Transform()
//   transformStream.byteCount = 0
//   transformStream._transform = (chunk, encoding, next) => {
//     const cipherChunk = cipher.update(chunk)
//     transformStream.push(cipherChunk)
//     transformStream.byteCount += cipherChunk.length
//     next()
//   }
//   transformStream.resultat = null

//   transformStream.on('end', async _ =>{
//     const infoChiffrage = await cipher.finish()
//     const meta = infoChiffrage.meta

//     // Preparer commande MaitreDesCles
//     transformStream.commandeMaitredescles = await preparerCommandeMaitrecles(
//       certificatsPem, infoChiffrage.password, domaine,
//       meta.hachage_bytes, meta.iv, meta.tag,
//       identificateurs_document,
//       opts
//     )
//     debug("Resultat chiffrage disponible : %O", transformStream.commandeMaitredescles)
//   })

//   return transformStream
// }

async function chiffrerMemoire(pki, fichierSrc, clesPubliques, opts) {
  opts = opts || {}

  const identificateurs_document = opts.identificateurs_document || {}

  // Creer cipher
  debug("Cles publiques pour cipher : %O", clesPubliques)
  const cipher = await pki.creerCipherChiffrageAsymmetrique(
    clesPubliques, 'GrosFichiers', identificateurs_document
  )

  return new Promise((resolve, reject)=>{
    const s = fs.ReadStream(fichierSrc)
    var tailleFichier = 0
    var buffer = [];
    s.on('error', err=>reject(err))
    s.on('data', data => {
      const contenuCrypte = cipher.update(data);
      tailleFichier += contenuCrypte.length
      buffer = [...buffer, ...contenuCrypte]
    })
    s.on('end', async _ => {
      const informationChiffrage = await cipher.finalize()

      if(opts.base64 === true) {
        // Convertir les bytes en base64
        const ab = Uint8Array.from(buffer)
        buffer = base64.encode(ab)
      }

      debug("Information chiffrage fichier : %O", informationChiffrage)
      return resolve({
        tailleFichier,
        data: buffer,
        meta: informationChiffrage.meta,
        commandeMaitreCles: informationChiffrage.commandeMaitreCles
      })
    })
  })

}

module.exports = {
  // getDecipherPipe4fuuid, 
  // gcmStreamReaderFactory,
  // creerOutputstreamChiffrage,
  chiffrerMemoire,
}
