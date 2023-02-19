const debug = require('debug')('downloadManager')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const axios = require('axios')
const https = require('https')

const { decipherTransform } = require('./cryptoUtils')

const INTERVALLE_RUN_TRANSFERT = 900_000,
      CONST_TAILLE_SPLIT_MAX_DEFAULT = 5 * 1024 * 1024

/** 
 * Manager de download de fichiers de media.
 * Download, dechiffre et gere le repertoire de media dechiffre.
 */
function MediaDownloadManager(mq, pathStaging, storeConsignation) {
    this.mq = mq
    this.pathStaging = pathStaging
    this.storeConsignation = storeConsignation

    this.queueFuuids = []
    
    // fuuid: {callbacksPending: [], path: string, dateCreation: Date, dernierAcces: Date, cle: Object}
    this.fuuidsStaging = {}

    this.timerTraitement = null

    const pki = mq.pki
    // Configurer httpsAgent avec les certificats/cles
    // Note: pas de verif CA, la destination peut etre un serveur public 
    //       (Host Gator, Amazon, CloudFlare, etc.). C'est safe, le contenu 
    //       est chiffre et la cle est separee et signee.
    this.httpsAgent = new https.Agent({
      rejectUnauthorized: false,
      // ca: pki.ca,
      cert: pki.chainePEM,
      key: pki.cle,
    })
  
}

MediaDownloadManager.prototype.init = async function() {
    // Demarrer thread de traitement
    debug("Demarrage download manager, path staging ", this.pathStaging)

    const pathWork = path.join(this.pathStaging, 'work'),
          pathCache = path.join(this.pathStaging, 'cache')

    await Promise.all([
        fsPromises.mkdir(pathWork, {recursive: true}), 
        fsPromises.mkdir(pathCache, {recursive: true}),
    ])

    this.threadTransfert()
      .catch(err=>console.error("Erreur demarrage thread ", err))
}

/** Retourne l'information de staging d'un fichier present dans le cache */
MediaDownloadManager.prototype.getFichierCache = function(fuuid) {
    const staging = this.fuuidsStaging[fuuid]
    if(staging && staging.path) return staging
    return false
}

MediaDownloadManager.prototype.downloaderFuuid = async function(fuuid, cle, opts) {
    opts = opts || {}

    const dechiffrer = opts.dechiffrer || false  // Si true, on dechiffre le cache (requis pour ffmpeg, image magick)
    const mimetype = opts.mimetype

    let staging = this.fuuidsStaging[fuuid]

    let nouveau = false
    if(!staging) {
        // Creer staging

        staging = {
            fuuid,
            cle,
            mimetype,
            dechiffrer,          // Si true, indique qu'on doit dechiffrer le cache sur disque

            path: null,

            callbacksPending: [],
            dateCreation: new Date(),
            dernierAcces: null,

            uploadsEnCours: [],  // Liste de sessions d'upload en cours
            uploade: false,      // Indique qu'au moins 1 upload du cache a ete complete avec succes
            supprimer: false,    // Indique qu'on peut supprimer le cache
        }
        this.fuuidsStaging[fuuid] = staging

        nouveau = true
    } 
    
    // Ajouter callback pour attendre la fin du download
    let promiseDownload = null
    if(!staging.path) {
        // Ajouter callback au staging existant
        promiseDownload = new Promise( (resolve, reject) => {
            const cb = async result => {
                if(result.err) return reject(result.err)
                resolve(result)
            }
            staging.callbacksPending.push(cb)
        })
    }

    // Le callback est pret, ajouter fuuid a la Q de traitement si nouveau
    if(nouveau) {
        debug('ajouterFuuid %s, Q: %O', fuuid, this.queueFuuids)
        this.queueFuuids.push(fuuid)
    }

    // S'assurer que la thread est demarree
    if(this.timerTraitement) {
        this.threadTransfert()
            .catch(err=>console.error("Erreur run threadTransfert: %O", err))
    }

    if(promiseDownload) await promiseDownload
    debug("Fichier download complete : ", fuuid)

    // Lancer l'erreur si presente
    if(staging.err) throw staging.err

    return staging
}

/** Retourne un stream qui permet de decoder le contenu du fichier. */
MediaDownloadManager.prototype.pipeDecipher = async function(fuuid, outStream) {
    let staging = this.fuuidsStaging[fuuid]

    if(!staging || !staging.path) {
        throw new Error(' downloadManager.getDecipherStream Fichier pas en cache', fuuid)
    }

    const pathFichier = staging.path,
          cle = staging.cle

    const readStream = fs.createReadStream(pathFichier)
    await dechiffrerStream(readStream, cle, outStream)
}

MediaDownloadManager.prototype.threadTransfert = async function() {
    try {
        debug("Run threadTransfert")
        if(this.timerTraitement) clearTimeout(this.timerTraitement)
        this.timerTraitement = null

        // Process les items de la Q
        debug("threadTransfert Queue avec %d items", this.queueFuuids.length)
        while(this.queueFuuids.length > 0) {
            const fuuid = this.queueFuuids.shift()  // FIFO
            const stagingInfo = this.fuuidsStaging[fuuid],
                  callbacks = stagingInfo.callbacksPending
            try {
                debug("threadTransfert Traiter GET pour item %s", fuuid)
                const pathFichier = await this.getFichier(fuuid)
                stagingInfo.path = pathFichier
                for await (let cb of callbacks) {
                    await cb(stagingInfo)
                }
            } catch(err) {
                stagingInfo.err = err
                for await (let cb of callbacks) {
                    await cb({err})
                }
            } finally {
                stagingInfo.callbacksPending = undefined
                stagingInfo.dernierAcces = new Date()
            }
        }

        // Cleanup, entretien
        await this.entretien()

    } catch(err) {
        console.error(new Date() + ' downloadPriveManager.threadTransfert Erreur execution cycle : %O', err)
    } finally {
        debug("threadTransfert Fin execution cycle, attente %s ms", INTERVALLE_RUN_TRANSFERT)
        
        // Redemarrer apres intervalle
        this.timerTraitement = setTimeout(()=>{
            this.timerTraitement = null
            this.threadTransfert()
                .catch(err=>console.error(new Date() + " downloadPriveManager.threadTransfert Erreur run threadTransfert: %O", err))
        }, INTERVALLE_RUN_TRANSFERT)
    }
}

MediaDownloadManager.prototype.entretien = async function() {
    debug('entretien')
}

MediaDownloadManager.prototype.getFichier = async function(fuuid) {
    debug("getFichier %s", fuuid)
    const pathFichier = path.join(this.pathStaging, 'cache', fuuid)

    const staging = this.fuuidsStaging[fuuid]

    let pathFichierWork = path.join(this.pathStaging, 'work', fuuid)
    if(staging.dechiffrer === true && staging.mimetype) {
        throw new Error('not implemented')
        // Ajouter extension fichier. Utilise par ffmpeg et imagemagick.
    }

    const url = new URL('' + this.storeConsignation.getUrlTransfert())
    url.pathname = path.join(url.pathname, fuuid)

    try {
        // download axios
        debug("getFichier download %s", url.href)
        const reponse = await axios({
            method: 'GET', 
            url: url.href, 
            httpsAgent: this.httpsAgent,
            responseType: 'stream',
            timeout: 5000,
        })
        debug("getFichier reponse %O\nCache vers %O", reponse.headers, pathFichierWork)
        const streamWriter = fs.createWriteStream(pathFichierWork)
        await new Promise((resolve, reject)=>{
            streamWriter.on('close', resolve)
            streamWriter.on('error', reject)

            if(staging.dechiffrer === true) {
                reject(new Error('not implemented'))
            } else {
                reponse.data.pipe(streamWriter)
            }
        })

        await fsPromises.rename(pathFichierWork, pathFichier)

        return pathFichier
    } catch(err) {
        // Supprimer fichier work
        fsPromises.unlink(pathFichierWork)
            .catch(err=>console.error("Erreur supprimer fichier work %s : %O", pathFichierWork, err))
        throw err
    }
}

async function dechiffrerStream(stream, cleFichier, writeStream, opts) {
    opts = opts || {}
    debug("dechiffrerStream cleFichier : %O", cleFichier)
    const decipherTransformStream = await decipherTransform(cleFichier.cleSymmetrique, {...cleFichier.metaCle})
  
    const progress = opts.progress
    let prochainUpdate = 0, position = 0
  
    const promiseTraitement = new Promise((resolve, reject)=>{
        try {
            decipherTransformStream.on('data', chunk=>{
            position += chunk.length
    
            // Updates (keep-alive process)
            const current = new Date().getTime()
            if(prochainUpdate < current) {
                if(progress) progress({position})
                prochainUpdate = current + 5 * 60 * 1000  // 5 secondes
            }
            });
            decipherTransformStream.on('end', ()=>{
            if(progress) progress({position, size: position, complete: true})
            resolve()
            });
            decipherTransformStream.on('error', err=>{
            debug("dechiffrerStream Erreur : %O", err)
            reject(err)
            });
        } catch(err) {
            debug("Erreur preparation events readable : %O", err)
            reject(err)
        }
    })
  
    // Pipe dechiffrage -> writer
    decipherTransformStream.pipe(writeStream)
  
    // Pipe la reponse chiffree dans le dechiffreur
    stream.pipe(decipherTransformStream)
  
    return promiseTraitement
}

// MediaDownloadManager.prototype.stagingFichier = async function(fuuid, stream) {
//     // Staging de fichier public
  
//     // Verifier si le fichier existe deja
//     const pathFuuidLocal = pathConsignation.trouverPathLocal(fuuidEffectif, true)
//     const pathFuuidEffectif = path.join(pathConsignation.consignationPathDownloadStaging, fuuidEffectif)
//     var statFichier = await new Promise((resolve, reject) => {
//       // S'assurer que le path de staging existe
//       fs.mkdir(pathConsignation.consignationPathDownloadStaging, {recursive: true}, err=>{
//         if(err) return reject(err)
//         // Verifier si le fichier existe
//         fs.stat(pathFuuidEffectif, (err, stat)=>{
//           if(err) {
//             if(err.errno == -2) {
//               resolve(null)  // Le fichier n'existe pas, on va le creer
//             } else {
//               reject(err)
//             }
//           } else {
//             // Touch et retourner stat
//             const time = new Date()
//             fs.utimes(pathFuuidEffectif, time, time, err=>{
//               if(err) {
//                 debug("Erreur touch %s : %o", pathFuuidEffectif, err)
//                 return
//               }
//               resolve({pathFuuidLocal, filePath: pathFuuidEffectif, stat})
//             })
//           }
//         })
//       })
//     })
  
//     // Verifier si le fichier existe deja - on n'a rien a faire
//     if(statFichier) return statFichier
  
//     // Le fichier n'existe pas, on le dechiffre dans staging
//     const outStream = fs.createWriteStream(pathFuuidEffectif, {flags: 'wx'})
//     return new Promise((resolve, reject)=>{
//       outStream.on('close', _=>{
//         fs.stat(pathFuuidEffectif, (err, stat)=>{
//           if(err) {
//             reject(err)
//           } else {
//             debug("Fin staging fichier %O", stat)
//             resolve({pathFuuidLocal, filePath: pathFuuidEffectif, stat})
//           }
//         })
  
//       })
//       outStream.on('error', err=>{
//         debug("publicStaging.stagingFichier Erreur staging fichier %s : %O", pathFuuidEffectif, err)
//         reject(err)
//       })
  
//       infoStream.decipherStream.writer.on('error', err=>{
//         debug("Erreur lecture fichier chiffre : %O", err)
//         reject(err)
//       })
  
//       debug("Staging fichier %s", pathFuuidEffectif)
//       infoStream.decipherStream.writer.pipe(outStream)
//       var readStream = fs.createReadStream(pathFuuidLocal);
//       readStream.pipe(infoStream.decipherStream.reader)
//       readStream.on('error', err=>{
//         console.error("publicStaging.stagingFichier Erreur traitement ficheir %s : %O", pathFuuidEffectif, err)
//         reject(err)
//       })
//     })
  
// }

module.exports = MediaDownloadManager
