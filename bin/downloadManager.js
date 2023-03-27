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
function MediaDownloadManager(mq, pathStaging, getUrlTransfert) {
    this.mq = mq
    this.pathStaging = pathStaging
    this.getUrlTransfert = getUrlTransfert

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

MediaDownloadManager.prototype.attendreDownload = async function(fuuid, opts) {
    const staging = this.fuuidsStaging[fuuid]
    const timeoutDuration = opts.timeout || staging.timeout || 15_000
    if(staging) {
        if(staging.promiseReady) {
            const timeoutPromise = new Promise(resolve => setTimeout(()=>resolve({timeout: true}), timeoutDuration))
            const result = await Promise.race([timeoutPromise, staging.promiseReady])
            if(result.timeout === true) return {timeout: true, ...staging}
        }
        return staging
    }
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

    if(!staging) {
        // Creer staging

        staging = {
            fuuid,
            cle,
            mimetype,
            dechiffrer,          // Si true, indique qu'on doit dechiffrer le cache sur disque
            timeout: opts.timeout,

            path: null,

            // callbacksPending: [],
            promiseReady: null,  // Present si path/err ne sont pas present
            dateCreation: new Date(),
            dernierAcces: null,

            uploadsEnCours: [],  // Liste de sessions d'upload en cours
            uploade: false,      // Indique qu'au moins 1 upload du cache a ete complete avec succes
            supprimer: false,    // Indique qu'on peut supprimer le cache
            actif: false,        // Si true, est utilise par un process (ne pas supprimer)
        }
        this.fuuidsStaging[fuuid] = staging

        staging.promiseReady = new Promise((resolve, reject)=>{
            staging.promiseResolve = resolve
            staging.promiseReject = reject
        })

    } else if(staging.path && !staging.cle) {
        // Le fichier existe deja localement - il manquait la cle
        staging.cle = cle
        staging.mimetype = mimetype
        staging.dernierAcces = new Date()
        return staging
    }
    
    // Ajouter fuuid a la Q de traitement si nouveau
    if(staging.promiseReady !== undefined) {
        debug('ajouterFuuid %s, Q: %O', fuuid, this.queueFuuids)
        this.queueFuuids.push(fuuid)
    }

    // S'assurer que la thread est demarree
    if(this.timerTraitement) {
        this.threadTransfert()
            .catch(err=>console.error("Erreur run threadTransfert: %O", err))
    }

    if(staging.promiseReady) await staging.promiseReady
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
            const stagingInfo = this.fuuidsStaging[fuuid]
            try {
                debug("threadTransfert Traiter GET pour item %s", fuuid)
                stagingInfo.actif = true
                const pathFichier = await this.getFichier(fuuid)
                stagingInfo.path = pathFichier
                stagingInfo.promiseResolve(stagingInfo)
            } catch(err) {
                stagingInfo.err = err
                if(stagingInfo.promiseReject) stagingInfo.promiseReject(err)
                else {
                    console.error(new Date() + " ERROR MediaDownloadManager.threadTransfert while loop Erreur ", err)
                }
            } finally {
                stagingInfo.actif = false
                stagingInfo.promiseReady = undefined
                stagingInfo.promiseResolve = undefined
                stagingInfo.promiseReject = undefined
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

    const dateExpiration = new Date().getTime() - EXPIRATION_WORK
    debug("cleanupWork Date expiration work : ", dateExpiration)

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
            try {
                if(staging.path) {
                    debug("entretien Supprimer fichier %s", staging.path)
                    fsPromises.unlink(staging.path).catch(err=>console.error("Erreur cleanup %s : %O", staging.path, err))
                }
                if(staging.promiseReject) {
                    staging.promiseReject(new Error('cleanup'))
                }
            } catch(err) {
                console.warn(new Date() + " downloadManager WARN Probleme suppression work %s : %O", fuuid, err)
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
    const timeout = staging.timeout || 5000
    let pathFichier = null

    let pathFichierWork = path.join(this.pathStaging, 'work', fuuid)
    if(staging.dechiffrer === true && staging.mimetype) {
        // Ajouter extension fichier. Utilise par ffmpeg et imagemagick.
        const extension = MIMETYPE_EXT_MAP[staging.mimetype] || 'bin'
        pathFichier = path.join(this.pathStaging, 'dechiffre', fuuid + '.' + extension)
    } else {
        pathFichier = path.join(this.pathStaging, 'chiffre', fuuid)        
    }

    const url = new URL('' + this.getUrlTransfert())
    url.pathname = path.join(url.pathname, fuuid)
    url.searchParams.set('internal', '1')  // Flag qui indique requete pour consommation interne

    try {
        // download axios
        debug("getFichier download %s", url.href)
        const expiration = new Date().getTime() + timeout

        let reponse = null, err = null
        while(!reponse) {
            try {
                reponse = await axios({
                    method: 'GET', 
                    url: url.href, 
                    httpsAgent: this.httpsAgent,
                    responseType: 'stream',
                    timeout,
                    maxRedirects: 2,
                })
                debug("getFichier reponse %O\nCache vers %O", reponse.headers, pathFichierWork)
                err = null
            } catch(errAxios) {
                err = errAxios
                debug("getFichier Axios error : ", errAxios)
                const response = errAxios.response
                const status = response.status
                if(status === 404 && expiration > new Date().getTime() + 5000) {
                    // Le fichier n'est pas encore arrive, reessayer
                    await new Promise(resolve=>setTimeout(resolve, 5000))
                } else {
                    throw errAxios
                }
            }
        }

        if(!reponse) throw err
        
        const size = reponse.headers['content-length']
        staging.size = size
        staging.position = 0

        reponse.data.on('data', chunk => {
            staging.position += chunk.length
            debug("Position %d, length %d", staging.position, staging.size)
        })

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
            .catch(err=>{
                if(err.code === 'ENOENT') return  // Ok, fichier n'existait pas
                console.error("Erreur supprimer fichier work %s : %O", pathFichierWork, err)
            })
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

                // callbacksPending: null,
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
