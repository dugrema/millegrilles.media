//const crypto = require('crypto')
const fs = require('fs')
const { Hacheur, VerificateurHachage } = require('@dugrema/millegrilles.common/lib/hachage')

async function calculerHachageFichier(pathFichier, opts) {
  if(!opts) opts = {};

  const readStream = fs.createReadStream(pathFichier)

  // Calculer hachage sur fichier
  return calculerHachageStream(readStream, opts)
}

async function verifierHachageFichier(pathFichier, hachage, opts) {
  if(!opts) opts = {};

  const readStream = fs.createReadStream(pathFichier)

  // Calculer hachage sur fichier
  return calculerHachageStream(readStream, {...opts, hachage})
}

async function calculerHachageStream(readStream, opts) {
  opts = opts || {}

  var hacheur = null, verificateur = null
  if(opts.hachage) {
    try {
      verificateur = new VerificateurHachage(opts.hachage)
    } catch(err) {
      console.error("Erreur chargement verificateur : %O", err)
    }
  }
  hacheur = new Hacheur(opts)

  // let fonctionHash = opts.fonctionHash || 'sha512'
  // fonctionHash = fonctionHash.split('_')[0]  // Enlever _b64 si present
  // const sha = crypto.createHash(fonctionHash)

  return new Promise(async (resolve, reject)=>{
    readStream.on('data', chunk=>{
      // sha.update(chunk)
      if(hacheur) hacheur.update(chunk)
      if(verificateur) verificateur.update(chunk)
    })
    readStream.on('end', ()=>{
      // const resultat = sha.digest('base64')
      // resolve(fonctionHash + '_b64:' + resultat)
      var hachage = opts.hachage
      if(hacheur) {
        hachage = hacheur.finalize()
      }
      if(verificateur) {
        try {
          verificateur.verify()
        } catch(err) {
          err.hachage = hachage
          return reject(err)
        }
      }

      // Cas ou le hachage fourni est mauvais
      if(opts.hachage && ! verificateur) {
        const error = new Error(`Hachage fourni est illisible : ${opts.hachage}, hachage calcule : ${hachage}`)
        error.hachage = hachage
        reject(error)
      }
      resolve(hachage)
    })
    readStream.on('error', err=> {
      reject(err)
    })

    if(readStream.read) readStream.read()
    else readStream.resume()
  })
}

// function calculerHachageData(data, opts) {
//   if(!opts) opts = {}
//   let fonctionHash = opts.fonctionHash || 'sha512'
//   fonctionHash = fonctionHash.split('_')[0]  // Enlever _b64 si present
//
//   // Calculer SHA512 sur fichier de backup
//   const sha = crypto.createHash(fonctionHash);
//   sha.update(data)
//
//   const digest = sha.digest('base64')
//   return fonctionHash + '_b64:' + digest
// }

module.exports = { calculerHachageFichier, verifierHachageFichier, calculerHachageStream }
