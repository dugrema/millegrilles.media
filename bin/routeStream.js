const debug = require('debug')('media:routeStream')
const express = require('express');
const path = require('path');
const fs = require('fs')
const fsPromises = require('fs/promises')

const { recupererCle } = require('./pki')

const STAGING_FILE_TIMEOUT_MSEC = 300000

function route(opts) {
    const router = express.Router();
  
    router.get('/stream_transfert/:fuuid', downloadVideoPrive, pipeReponse)
    router.get('/stream_transfert/:fuuid/*', downloadVideoPrive, pipeReponse)  // Supporter nom fichier (e.g. /video.mov)
  
    return router
}

async function downloadVideoPrive(req, res, next) {
    debug("downloadVideoPrive methode:" + req.method + ": " + req.url);
    debug("Headers : %O\nAutorisation: %o", req.headers, req.autorisationMillegrille);

    const fuuid = req.params.fuuid
    res.fuuid = fuuid
    debug("downloadVideoPrive Fuuid : %s", fuuid)

    var mq = req.amqpdao

    debug("Verifier l'acces est autorise %s", req.url)

    // Demander une permission de dechiffrage et stager le fichier.
    try {
        let cacheEntry = req.transfertConsignation.getCacheItem(fuuid)
        if(!cacheEntry) {
            debug("Cache MISS sur %s dechiffre", fuuid)

            // Recuperer la cle
            debug("Demande cle dechiffrage")
            const cleDechiffrage = await recupererCle(mq, fuuid)
            debug("Cle dechiffrage recue : %O", cleDechiffrage.metaCle)

            // Recuperer information sur le GrosFichier (pour mimetype, nom du fichier)
            const requete = {fuuids_documents: [fuuid]}
            const reponseFichiers = await mq.transmettreRequete(
                'GrosFichiers', requete, 
                {action: 'documentsParFuuid', exchange: '2.prive', attacherCertificat: true}
            )
            if(!reponseFichiers || reponseFichiers.ok === false) {
                debug("Erreur dans reponse fichiers : %O", reponseFichiers)
                return {ok: false, err: `fuuid inconnu ou err : ${fuuid}`}
            }

            debug("Reponse info fichiers : %O", reponseFichiers)

            const fichierMetadata = reponseFichiers.fichiers.pop()
            debug("Fichier metadata: %O", fichierMetadata)

            const infoVideo = Object.values(fichierMetadata.version_courante.video).filter(item=>item.fuuid_video===fuuid).pop()
            debug("Info video %s\n%O", fuuid, infoVideo)
            if(!infoVideo) {
                debug("Aucuns videos associes")
                return res.sendStatus(404)
            }

            const mimetype = infoVideo.mimetype
            if(!mimetype.startsWith('video/')) {
                debug("Le mimetype n'est pas video")
                return res.sendStatus(403)
            }

            let paramsGrosFichiers = {nom: fichierMetadata.nom}

            // Stager le fichier dechiffre
            try {
                debug("Stager fichier %s", fuuid)
                cacheEntry = await req.transfertConsignation.getDownloadCacheFichier(
                    fuuid, mimetype, cleDechiffrage, {metadata: paramsGrosFichiers})
                
                // Attendre fin du dechiffrage
                await cacheEntry.ready
            } catch(err) {
                debug("genererPreviewImage Erreur download fichier avec downloaderFichierProtege : %O", err)
                return res.sendStatus(500)
            }
        } else {
            debug("Cache HIT sur %s dechiffre", fuuid)
        }

        debug("Fichier a streamer : %O", cacheEntry)
        const pathFichierDechiffre = cacheEntry.decryptedPath,
              metadata = cacheEntry.metadata,
              mimetype = cacheEntry.mimetype

        const statFichier = await fsPromises.stat(pathFichierDechiffre)
        debug("Stat fichier %s :\n%O", pathFichierDechiffre, statFichier)

        res.fuuid = fuuid

        // Preparer le fichier dechiffre dans repertoire de staging
        // const infoFichierEffectif = await stagingPublic(pathConsignation, fuuidEffectif, infoStream)
        res.stat = statFichier
        res.filePath = pathFichierDechiffre

        // Ajouter information de header pour slicing (HTTP 206)
        // res.setHeader('Content-Length', res.stat.size)
        res.setHeader('Accept-Ranges', 'bytes')

        // res.setHeader('Content-Length', res.tailleFichier)
        res.setHeader('Content-Type', mimetype)

        // Cache control public, permet de faire un cache via proxy (nginx)
        res.setHeader('Cache-Control', 'public, max-age=604800, immutable')
        res.setHeader('fuuid', res.fuuid)
        res.setHeader('securite', '2.prive')
        res.setHeader('Last-Modified', res.stat.mtime)
    
        const range = req.headers.range
        if(range) {
            debug("Range request : %s, taille fichier %s", range, res.stat.size)
            const infoRange = readRangeHeader(range, res.stat.size)
            debug("Range retourne : %O", infoRange)
            res.range = infoRange

            res.setHeader('Content-Length', infoRange.End - infoRange.Start + 1)
        } else {
            res.setHeader('Content-Length', res.stat.size)
        }

    } catch(err) {
        console.error("Erreur traitement dechiffrage stream pour %s:\n%O", req.url, err)
        return res.sendStatus(500)
    }

    next()
}

// Sert a preparer un fichier temporaire local pour determiner la taille, supporter slicing
function pipeReponse(req, res) {
    // const header = res.responseHeader
    const { range, filePath, fileRedirect, stat } = res
  
    if(range) {
      var start = range.Start,
          end = range.End
  
      // If the range can't be fulfilled.
      if (start >= stat.size) { // || end >= stat.size) {
        // Indicate the acceptable range.
        res.setHeader('Content-Range', 'bytes */' + stat.size)  // File size.
  
        // Return the 416 'Requested Range Not Satisfiable'.
        res.writeHead(416)
        return res.end()
      }
  
      res.setHeader('Content-Range', 'bytes ' + start + '-' + end + '/' + stat.size)
  
      debug("Transmission range fichier %d a %d bytes (taille :%d) : %s", start, end, stat.size, filePath)
      const readStream = fs.createReadStream(filePath, { start: start, end: end })

      let totalBytes = 0
      readStream.on('data', chunk=>{
        totalBytes += chunk.length
      })
      readStream.on('end', ()=>{
        debug("!!! TOTAL BYTES : %d", totalBytes)
      })

      res.status(206)
      readStream.pipe(res)
    } else if(fileRedirect) {
      // Redirection
      res.status(307).send(fileRedirect)
    } else if(filePath) {
      // Transmission directe du fichier
      const readStream = fs.createReadStream(filePath)
      res.writeHead(200)
      readStream.pipe(res)
    }
  
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

module.exports = route
