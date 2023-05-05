const debug = require('debug')('media:routeStream')
const express = require('express');
const fs = require('fs')
const fsPromises = require('fs/promises')
const { verifierTokenFichier } = require('@dugrema/millegrilles.nodejs/src/jwt')

const { recupererCle } = require('./pki')

const TIMEOUT_HEAD = 3_000

function route(mq, opts) {
    const router = express.Router()

    const routerTransfert = express.Router()
    router.use('/stream_transfert', routerTransfert)

    // Autorisation
    routerTransfert.get('/:fuuid', verifierJwt, downloadVideoPrive, pipeReponse, cleanup)
    routerTransfert.head('/:fuuid', verifierJwt, downloadVideoPrive, (_req,res)=>res.sendStatus(200))

    return router
}

async function downloadVideoPrive(req, res, next) {
    const mq = req.amqpdao,
          downloadManager = req.downloadManager

    const fuuid = res.fuuid,
          userId = res.userId,
          cleRefFuuid = res.cleRefFuuid,
          format = res.format,
          header = res.header,
          iv = res.iv,
          tag = res.tag,
          roles = res.roles || [],
          mimetype = res.mimetype

    try {
        let download = await downloadManager.attendreDownload(fuuid, {timeout: TIMEOUT_HEAD})
        if(download === false || !download.cle) {
            debug("downloadVideoPrive Fichier absent du cache, downloader ", fuuid)

            // Recuperer la cle de dechiffrage
            let domaine = 'GrosFichiers'
            if(roles.includes('messagerie_web')) domaine = 'Messagerie'
        
            debug("Demande cle dechiffrage a %s (stream: true)", domaine)
            try {
                const cleDechiffrage = await recupererCle(mq, cleRefFuuid, {stream: true, domaine, userId})
                debug("Cle dechiffrage recue : %O", cleDechiffrage.metaCle)
            } catch(err) {
                debug("Acces cle refuse : ", err)
                return res.sendStatus(403)
            }
        
            if(!cleDechiffrage || !cleDechiffrage.metaCle) {
                debug("Acces cle refuse : ", cleDechiffrage)
                return res.sendStatus(403)
            }

            // Injecter params de dechiffrage custom si video est transcode (pas original)
            const metaCle = cleDechiffrage.metaCle
            metaCle.header = header || metaCle.header
            metaCle.iv = iv || metaCle.iv
            metaCle.tag = tag || metaCle.tag
            metaCle.format = format || metaCle.format

            // Downloader fichier
            try {
                downloadManager.downloaderFuuid(fuuid, cleDechiffrage, {mimetype, dechiffrer: true})
                    .catch(err=>console.error(new Date() + ' routeStream.downloadVideoPrive ERROR Echec downloaderFuuid ', err))
                download = await downloadManager.attendreDownload(fuuid, {timeout: TIMEOUT_HEAD})    
            } catch(err) {
                console.error(new Date() + " routeStream.downloadVideoPrive Erreur download %s vers cache : %O", fuuid, err)
                download.err = err
                return res.status(500).send({ok: false, err: ''+err})
            }
        }

        const staging = download

        if(staging.err) {
            return res.status(500).send({ok: false, err: ''+err})
        }

        if(staging.size === undefined || staging.position === undefined) {
            // Erreur - aucune information de chargement
            res.setHeader('Content-Type', mimetype)
            return res.status(500).send({ok: false, err: 'Aucune information de chargement'})
        } else if(staging.timeout === true || !staging.path) {
            // Retourner info de progres
            res.setHeader('Content-Type', mimetype)
            res.setHeader('X-File-Size', staging.size || 0)
            res.setHeader('X-File-Position', staging.position || 0)
            return res.sendStatus(202)
        }

        res.staging = staging   // Conserver pour cleanup

        const statFichier = await fsPromises.stat(staging.path)
        res.statFichier = statFichier

        const contentLength = statFichier.size
        // Calculer taille video pour mgs4
        // const overheadLength = Math.ceil((contentLength / ((64 * 1024)+17))) * 17
        // const decryptedContentLength = contentLength - overheadLength
        res.contentLength = contentLength
    } catch(err) {
        debug("routeStream.downloadVideoPrive ERROR ", err)
        console.error(new Date() + ' routeStream.downloadVideoPrive ERROR ', err)
        return res.sendStatus(500)
    }

    try {
        debug("downloadVideoPrive Pipe stream dechiffrage pour ", fuuid)

        const range = req.headers.range
        if(range) {
            debug("Range request : %s, taille fichier %s", range, res.stat.size)
            const infoRange = readRangeHeader(range, res.stat.size)
            debug("Range retourne : %O", infoRange)
            res.range = infoRange

            res.setHeader('Content-Length', infoRange.End - infoRange.Start + 1)
        } else {
            res.setHeader('Content-Length', contentLength)
        }

        res.setHeader('Content-Type', mimetype)
        // res.setHeader('Content-Length', contentLength)
        res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
        res.setHeader('Accept-Ranges', 'bytes')

        res.status(200)

        // Next pipe la reponse dechiffree sur GET
        next()
    } catch(err) {
        console.error(new Date() + " routeStream.downloadVideoPrive Erreur stream %s : %O", fuuid, err)
        return res.sendStatus(500)
    }

}

// async function downloadVideoPriveOld(req, res, next) {
//     debug("downloadVideoPrive methode:" + req.method + ": " + req.url);
//     debug("Headers : %O\nAutorisation: %o", req.headers, req.autorisationMillegrille);

//     var mq = req.amqpdao
//     const fuuid = res.fuuid,
//           userId = res.userId,
//           cleRefFuuid = res.cleRefFuuid,
//           format = res.format,
//           header = res.header,
//           iv = res.iv,
//           tag = res.tag,
//           roles = res.roles || []

//     debug("Verifier l'acces est autorise %s, ref fuuid %s", req.url, cleRefFuuid)

//     // Demander une permission de dechiffrage et stager le fichier.
//     try {
//         let cacheEntry = req.transfertConsignation.getCacheItem(fuuid)
//         if(!cacheEntry) {
//             debug("Cache MISS sur %s dechiffre", fuuid)

//             let domaine = 'GrosFichiers'
//             if(roles.includes('messagerie_web')) domaine = 'Messagerie'

//             debug("Demande cle dechiffrage a %s (stream: true)", domaine)
//             const cleDechiffrage = await recupererCle(mq, cleRefFuuid, {stream: true, domaine, userId})
//             debug("Cle dechiffrage recue : %O", cleDechiffrage.metaCle)

//             if(!cleDechiffrage || !cleDechiffrage.metaCle) {
//                 debug("Acces cle refuse : ", cleDechiffrage)
//                 return res.sendStatus(403)
//             }

//             const metaCle = cleDechiffrage.metaCle

//             metaCle.header = header || metaCle.header
//             metaCle.iv = iv || metaCle.iv
//             metaCle.tag = tag || metaCle.tag
//             metaCle.format = format || metaCle.format

//             // let paramsGrosFichiers = {nom: fichierMetadata.nom}
//             let paramsGrosFichiers = {nom: fuuid}

//             let mimetype = res.mimetype

//             // Stager le fichier dechiffre
//             try {
//                 debug("Stager fichier %s : %O", fuuid, cleDechiffrage)
//                 cacheEntry = await req.transfertConsignation.getDownloadCacheFichier(
//                     fuuid, mimetype, cleDechiffrage, {metadata: paramsGrosFichiers})
                
//                 // Attendre fin du dechiffrage
//                 await cacheEntry.ready
//                 debug("Cache ready pour %s", fuuid)
//             } catch(err) {
//                 debug("genererPreviewImage Erreur download fichier avec downloaderFichierProtege : %O", err)
//                 return res.sendStatus(500)
//             }
//         } else {
//             debug("Cache HIT sur %s dechiffre", fuuid)
//         }

//         debug("Fichier a streamer : %O", cacheEntry)
//         res.cacheEntry = cacheEntry

//         const pathFichierDechiffre = cacheEntry.decryptedPath,
//               metadata = cacheEntry.metadata,
//               mimetype = cacheEntry.mimetype

//         const statFichier = await fsPromises.stat(pathFichierDechiffre)
//         debug("Stat fichier %s :\n%O", pathFichierDechiffre, statFichier)

//         res.fuuid = fuuid

//         // Preparer le fichier dechiffre dans repertoire de staging
//         // const infoFichierEffectif = await stagingPublic(pathConsignation, fuuidEffectif, infoStream)
//         res.stat = statFichier
//         res.filePath = pathFichierDechiffre

//         // Ajouter information de header pour slicing (HTTP 206)
//         // res.setHeader('Content-Length', res.stat.size)
//         res.setHeader('Accept-Ranges', 'bytes')

//         // res.setHeader('Content-Length', res.tailleFichier)
//         res.setHeader('Content-Type', mimetype)

//         // Cache control public, permet de faire un cache via proxy (nginx)
//         res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
//         res.setHeader('fuuid', res.fuuid)
//         res.setHeader('securite', '2.prive')
//         res.setHeader('Last-Modified', res.stat.mtime)
    
//         const range = req.headers.range
//         if(range) {
//             debug("Range request : %s, taille fichier %s", range, res.stat.size)
//             const infoRange = readRangeHeader(range, res.stat.size)
//             debug("Range retourne : %O", infoRange)
//             res.range = infoRange

//             res.setHeader('Content-Length', infoRange.End - infoRange.Start + 1)
//         } else {
//             res.setHeader('Content-Length', res.stat.size)
//         }

//     } catch(err) {
//         console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
//         return res.sendStatus(500)
//     }

//     next()
// }

// Sert a preparer un fichier temporaire local pour determiner la taille, supporter slicing
async function pipeReponse(req, res, next) {
    // const downloadManager = req.downloadManager
    const { range, fuuid, staging, contentLength } = res
  
    if(staging) {
        if(range) {
            var start = range.Start,
                end = range.End
        
            // If the range can't be fulfilled.
            if (start >= contentLength) { // || end >= stat.size) {
                // Indicate the acceptable range.
                res.setHeader('Content-Range', 'bytes */' + contentLength)  // File size.
        
                // Return the 416 'Requested Range Not Satisfiable'.
                res.writeHead(416)
                return res.end()
            }
        
            res.setHeader('Content-Range', 'bytes ' + start + '-' + end + '/' + contentLength)
        
            debug("Transmission range fichier %d a %d bytes (taille :%d) : %s", start, end, contentLength, filePath)
            const readStream = fs.createReadStream(staging.path, { start: start, end: end })
        
            // HACK : on ne supporte pas seek durant dechiffrage. On se fie au cache NGINX.
            // if(start > 0) return res.sendStatus(416)

            res.on('close', ()=>next())
            res.writeHead(206)
            readStream.pipe(res)
            // await downloadManager.pipeDecipher(fuuid, res)
        } else {
            // Transmission directe du fichier
            const readStream = fs.createReadStream(staging.path)
            res.on('close', ()=>next())
            res.writeHead(200)
            readStream.pipe(res)
            // await downloadManager.pipeDecipher(fuuid, res)
        }
    } else {
        console.error("pipeReponse fichier n'est pas en cache ", fuuid)
        res.sendStatus(500)
    }

}

function cleanup(req, res, next) {
    const downloadManager = req.downloadManager
    const { range, fuuid, staging, contentLength } = res

    debug("Cleanup ", fuuid)
    downloadManager.setFichierDownloadOk(fuuid)
}
  
function readRangeHeader(range, totalLength) {
    /* src : https://www.codeproject.com/articles/813480/http-partial-content-in-node-js
    * Example of the method 'split' with regular expression.
    *
    * Input: bytes=100-200
    * Output: [null, 100, 200, null]
    *
    * Input: bytes=-200
    * Output: [null, null, 200, null]
    */

    if (range === null || range.length == 0) {
        debug("readRangeHeader %O => NULL", range)
        return null
    }

    var array = range.split(/bytes=([0-9]*)-([0-9]*)/);
    var start = parseInt(array[1]);
    var end = parseInt(array[2]);

    if(isNaN(end) || end > totalLength) {
        end = totalLength - 1
    }

    var result = {
        Start: isNaN(start) ? 0 : start,
        End: end,
    }
    return result
}

async function verifierJwt(req, res, next) {
    const mq = req.amqpdao,
          pki = mq.pki

    const url = new URL('https://localhost' + req.url)
    const jwt = url.searchParams.get('jwt')

    const fuuid = req.params.fuuid
    res.fuuid = fuuid
    debug("downloadVideoPrive Fuuid : %s, Params: %O", fuuid, req.params)

    debug("JWT token ", jwt)

    try {
        const resultatToken = await verifierTokenFichier(pki, jwt)
        debug("verifierAutorisationStream Contenu token JWT (valide) : ", resultatToken)

        // S'assurer que le certificat signataire est de type collections
        const roles = resultatToken.extensions.roles,
              niveauxSecurite = resultatToken.extensions.niveauxSecurite
        
              if( ! niveauxSecurite.includes('2.prive') ) {
            debug("verifierAutorisationStream JWT signe par mauvais type de certificat (doit avoir exchange 2.prive)")
            return res.sendStatus(403)
        }
        
        if( ! roles.includes('messagerie_web') && ! roles.includes('collections') ) {
            debug("verifierAutorisationStream JWT signe par mauvais type de certificat (doit etre collections/messagerie)")
            return res.sendStatus(403)
        }

        if(fuuid !== resultatToken.payload.sub) {
            debug("verifierAutorisationStream URL et JWT fuuid ne correspondent pas")
            return res.sendStatus(403)  // Fuuid mismatch
        }

        // Verifier domaine
        const payload = resultatToken.payload || {}
        res.userId = payload.userId  // Utiliser userId du token
        res.cleRefFuuid = payload.ref || fuuid
        res.format = payload.format
        res.header = payload.header
        res.iv = payload.iv
        res.tag = payload.tag
        res.mimetype = payload.mimetype
        res.roles = roles
        res.jwt = resultatToken

        debug("verifierJwt Fuuid %s, userId %s", res.cleRefFuuid, res.userId)

    } catch(err) {
        if(err.code === 'ERR_JWS_SIGNATURE_VERIFICATION_FAILED') {
            debug("verifierAutorisationStream Token JWT invalide")
            return res.sendStatus(403)
        } else {
            throw err
        }
    }

    return next()
}

module.exports = route
