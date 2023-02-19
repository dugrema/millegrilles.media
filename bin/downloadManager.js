const debug = require('debug')('downloadManager')
const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const axios = require('axios')
const https = require('https')
const readdirp = require('readdirp')

const MIMETYPE_EXT_MAP = require('@dugrema/millegrilles.utiljs/res/mimetype_ext.json')

const { decipherTransform } = require('./cryptoUtils')

const INTERVALLE_RUN_TRANSFERT = 120_000

const EXPIRATION_COMPLETE = 5 * 60_000,
      EXPIRATION_INCOMPLET = 30 * 60_000,
      EXPIRATION_WORK = EXPIRATION_COMPLETE

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
          pathChiffre = path.join(this.pathStaging, 'chiffre'),
          pathDechiffre = path.join(this.pathStaging, 'dechiffre')

    await Promise.all([
        fsPromises.mkdir(pathWork, {recursive: true}), 
        fsPromises.mkdir(pathChiffre, {recursive: true}),
        fsPromises.mkdir(pathDechiffre, {recursive: true}),
    ])

    await this.parseCache()

    this.threadTransfert()
      .catch(err=>console.error("Erreur demarrage thread ", err))
}

/** Retourne l'information de staging d'un fichier present dans le cache */
MediaDownloadManager.prototype.getFichierCache = function(fuuid) {
    const staging = this.fuuidsStaging[fuuid]
    if(staging && staging.path && staging.cle) return staging
    return false
}

MediaDownloadManager.prototype.setFichierDownloadOk = function(fuuid) {
    const staging = this.fuuidsStaging[fuuid]
    if(staging && staging.path && staging.cle) {
        staging.uploade = true
        staging.dernierAcces = new Date()
    }
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
            actif: false,        // Si true, est utilise par un process (ne pas supprimer)
        }
        this.fuuidsStaging[fuuid] = staging

        nouveau = true
    } else if(staging.path && !staging.cle) {
        // Le fichier existe deja localement - il manquait la cle
        staging.cle = cle
        staging.mimetype = mimetype
        staging.dernierAcces = new Date()
        return staging
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
    try {
        staging.actif = true  // Lock pour eviter suppression
        await dechiffrerStream(readStream, cle, outStream)
    } finally {
        staging.actif = false
    }
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
                stagingInfo.actif = true
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
                stagingInfo.actif = false
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

/** Parse les fichiers deja present dans le cache chiffre/dechiffre */
MediaDownloadManager.prototype.parseCache = async function() {
    const pathChiffre = path.join(this.pathStaging, 'chiffre'),
          pathDechiffre = path.join(this.pathStaging, 'dechiffre')

    await this.parseExistants(pathChiffre, false)
    await this.parseExistants(pathDechiffre, true)
}

/** Parse les fichiers deja present dans le cache chiffre/dechiffre */
MediaDownloadManager.prototype.cleanupWork = async function() {
    const pathWork = path.join(this.pathStaging, 'work')

    const dateExpiration = new Date() - EXPIRATION_WORK

    for await (const entry of readdirp(pathWork, {type: 'files', alwaysStat: true})) {
        const { fullPath, stats } = entry
        debug("Entry fichier work : %s", fullPath)
        const { mtimeMs } = stats
        if(mtimeMs < dateExpiration) {
            fsPromises.unlink(fullPath).catch(err=>console.error("Erreur cleanup work fichier %s : %O", fullPath, err))
        }
    }
}

MediaDownloadManager.prototype.entretien = async function() {
    debug('entretien')

    // Identifier fichier qui peuvent etre reclames
    const now = new Date().getTime()
    const expireComplete = now - EXPIRATION_COMPLETE,
          expireIncomplet = now - EXPIRATION_INCOMPLET

    for (let fuuid of Object.keys(this.fuuidsStaging)) {
        const staging = this.fuuidsStaging[fuuid]
        debug("entretien Verifier ", staging)
        if(staging.uploade === true) {
            if(staging.dernierAcces.getTime() < expireComplete) {
                debug("Cleanup fichier complet ", fuuid)
                staging.supprimer = true
            }
        } else if(staging.err) {
            if(staging.dernierAcces.getTime() < expireIncomplet) {
                debug("Cleanup fichier incomplet ", fuuid)
                staging.supprimer = true
            }
        } else if(staging.actif === true) {
            // Ok
        } else if( ! this.queueFuuids.includes(fuuid)) {
            if(!staging.dernierAcces || staging.dernierAcces.getTime() < expireIncomplet) {
                debug("Cleanup fichier status incomplet/restaured ", fuuid)
                staging.supprimer = true
            }
        }
    }

    // Cleanup des fichiers a supprimer
    for (let fuuid of Object.keys(this.fuuidsStaging)) {
        const staging = this.fuuidsStaging[fuuid]
        if(staging.supprimer === true) {
            if(staging.path) {
                debug("entretien Supprimer fichier %s", staging.path)
                fsPromises.unlink(staging.path).catch(err=>console.error("Erreur cleanup %s : %O", staging.path, err))
            }
            delete this.fuuidsStaging[fuuid]
        }
    }

    // Cleanup des fichiers work
    await this.cleanupWork()
}

MediaDownloadManager.prototype.getFichier = async function(fuuid) {
    debug("getFichier %s", fuuid)
    const staging = this.fuuidsStaging[fuuid]
    let pathFichier = null

    let pathFichierWork = path.join(this.pathStaging, 'work', fuuid)
    if(staging.dechiffrer === true && staging.mimetype) {
        // Ajouter extension fichier. Utilise par ffmpeg et imagemagick.
        const extension = MIMETYPE_EXT_MAP[staging.mimetype] || 'bin'
        pathFichier = path.join(this.pathStaging, 'dechiffre', fuuid + '.' + extension)
    } else {
        pathFichier = path.join(this.pathStaging, 'chiffre', fuuid)        
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
                dechiffrerStream(reponse.data, staging.cle, streamWriter).catch(reject)
            } else {
                reponse.data.pipe(streamWriter)
            }
        })

        try {
            await fsPromises.rename(pathFichierWork, pathFichier)
        } catch(err) {
            if(err.code == 'EEXIST') {
                // Ok, fichier existe deja
            } else {
                throw err
            }
        }

        return pathFichier
    } catch(err) {
        // Supprimer fichier work
        fsPromises.unlink(pathFichierWork)
            .catch(err=>console.error("Erreur supprimer fichier work %s : %O", pathFichierWork, err))
        throw err
    }
}

MediaDownloadManager.prototype.parseExistants = async function(directory, dechiffrer) {
    for await (const entry of readdirp(directory, {type: 'files'})) {
        const { basename, fullPath } = entry
        debug("Entry fichier cache : %s", fullPath)
        const fichierParse = path.parse(basename)
        const fuuid = fichierParse.name

        let staging = this.fuuidsStaging[fuuid]
        if(!staging) {
            staging = {
                fuuid,
                cle: null,
                mimetype: null,
                dechiffrer: dechiffrer || false,
                path: fullPath,

                callbacksPending: null,
                dateCreation: new Date(),
                dernierAcces: new Date(),

                uploade: false,      // Indique qu'au moins 1 upload du cache a ete complete avec succes
                supprimer: false,    // Indique qu'on peut supprimer le cache
            }
            this.fuuidsStaging[fuuid] = staging
            debug("Recover fichier staging ", staging)
        }        
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

module.exports = MediaDownloadManager
