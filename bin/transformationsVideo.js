const debug = require('debug')('millegrilles:fichiers:transformationsVideo')
const fs = require('fs')
const tmpPromises = require('tmp-promise')
const path = require('path')
const FFmpeg = require('fluent-ffmpeg')

const transfertConsignation = require('./transfertConsignation')

const PROFILS_TRANSCODAGE = {
  webm: {
    videoBitrate: 1000000,
    height: 720,
    videoCodec: 'libvpx-vp9',
    audioCodec: 'libopus',
    audioBitrate: '128k',
    format: 'webm',
    videoCodecName: 'vp9',
  },
  mp4: {
    videoBitrate: 250000,
    height: 240,
    videoCodec: 'libx264',
    audioCodec: 'aac',
    audioBitrate: '64k',
    format: 'mp4',
    videoCodecName: 'h264',
  }
}

async function probeVideo(input, opts) {
  opts = opts || {}
  const maxHeight = opts.maxHeight || 720,
        maxBitrate = opts.maxBitrate || 750000,
        utiliserTailleOriginale = opts.utiliserTailleOriginale || false

  const resultat = await new Promise((resolve, reject)=>{
    if(input.on) {
      input.on('error', err=>{
        reject(err)
      })
    }
    FFmpeg.ffprobe(input, (err, metadata) => {
      if(err) return reject(err)
      resolve(metadata)
    })
  })

  debug("Resultat probe : %O", resultat)

  const infoVideo = resultat.streams.filter(item=>item.codec_type === 'video')[0]
  // debug("Information video : %O", infoVideo)

  // Determiner le bitrate et la taille (verticale) du video pour eviter un
  // upscaling ou augmentation bitrate
  const bitrate = infoVideo.bit_rate,
        raw = infoVideo.raw || {},
        height = raw.height || infoVideo.height,
        width = raw.width || infoVideo.width,
        nb_frames = infoVideo.nb_frames !== 'N/A'?infoVideo.nb_frames:null

  debug("Trouve : taille %dx%d, bitrate %d", width, height, bitrate)

  let heightEncoding = height
  if(!utiliserTailleOriginale) {
    heightEncoding = [2160, 1440, 1080, 720, 480, 360, 320, 240].filter(item=>{
      return item <= height && item <= maxHeight
    }).shift() || 240
  }
  let bitRateEncoding = [8000000, 4000000, 3000000, 2000000, 1500000, 1000000, 500000, 250000].filter(item=>{
    return item <= bitrate && item <= maxBitrate
  }).shift() || 250000

  // Calculer width
  const widthEncoding = Math.round(width * heightEncoding / height)

  debug("Information pour encodage : taille %dx%d, bit rate %d", widthEncoding, heightEncoding, bitRateEncoding)

  return {
    height: heightEncoding,
    width: widthEncoding,
    bitrate: bitRateEncoding,
    original: {height, width},
    nb_frames,
    raw: infoVideo
  }
}

async function transcoderVideo(streamFactory, outputStream, opts) {
  if(!opts) opts = {}

  var   videoBitrate = opts.videoBitrate || 250000
        height = opts.height || 240
        width = opts.width || 427,
        utiliserTailleOriginale = opts.utiliserTailleOriginale || false

  const videoCodec = opts.videoCodec || 'libx264',
        audioCodec = opts.audioCodec || 'aac',
        audioBitrate = opts.audioBitrate || '64k',
        format = opts.format || 'mp4',
        progressCb = opts.progressCb

  var input = streamFactory()
  var videoInfo = await probeVideo(input, {maxBitrate: videoBitrate, maxHeight: height, utiliserTailleOriginale})
  input.close()
  videoBitrate = videoInfo.bitrate
  height = videoInfo.height || height
  width = videoInfo.width

  // videoBitrate = '' + (videoBitrate / 1000) + 'k'
  debug('Utilisation video bitrate : %s, format %dx%d\nInfo: %O', videoBitrate, width, height, videoInfo)

  // Tenter transcodage avec un stream - fallback sur fichier direct
  // Va etre utilise avec un decipher sur fichiers .mgs2
  var modeInputStream = true
  input = streamFactory()  // Reset stream (utilise par probeVideo)

  var progressHook, framesTotal = videoInfo.nb_frames, framesCourant = 0
  var passe = 1
  if(progressCb) {
    progressHook = progress => {
      if(framesTotal) {
        progressCb({framesTotal, passe, ...progress})
      } else {
        progressCb({passe, ...progress})
        // Conserver le nombre de frames connus pour passe 2
        framesCourant = progress.frames
      }
    }
  }

  // Creer repertoire temporaire pour fichiers de log, outputfile
  const tmpDir = await tmpPromises.dir({unsafeCleanup: true})

  // Passe 1
  debug("Debut passe 1")
  var fichierInputTmp = null  //, fichierOutputTmp = null
  try {
    const videoOpts = { videoBitrate, height, width, videoCodec }
    const optsTranscodage = {
      progressCb: progressHook,
      tmpDir: tmpDir.path,
    }

    let stopFct = null, ok = false  // Sert a arreter ffmpeg si probleme avec nodejs (e.g. CTRL-C/restart container)
    try {
      const transcodageVideo = transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
      stopFct = transcodageVideo.stop
      await transcodageVideo.promise // Attendre fin
      ok = true
    } catch(err) {
      // Verifier si on a une erreur de streaming (e.g. video .mov n'est pas
      // supporte en streaming)
      const errMsg = err.message
      if(errMsg.indexOf("ffmpeg exited with code 1") === -1) {
        throw err  // Erreur non geree
      }

      debug("Echec parsing stream, dechiffrer dans un fichier temporaire et utiliser directement")
      modeInputStream = false

      // Copier le contenu du stream dans un fichier temporaire
      input = path.join(tmpDir.path, 'input.dat')
      fichierInputTmp = await extraireFichierTemporaire(input, streamFactory())
      // input = fichierInputTmp.path
      debug("Fichier temporaire input pret: %s", input)

      ok = false
      const transcodageVideo = transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
      stopFct = transcodageVideo.stop
      await transcodageVideo.promise // Attendre fin
      ok = true

    } finally {
      if(!ok && stopFct) stopFct()  // Forcer l'arret du processus
    }
    debug("Passe 1 terminee, debut passe 2")

    // Passe 2
    passe = 2  // Pour progressCb
    const audioOpts = {audioCodec, audioBitrate}
    if(modeInputStream) {
      // Reset inputstream
      input = streamFactory()
    }

    // Set nombre de frames trouves dans la passe 1 au besoin (sert au progress %)
    if(!framesTotal) framesTotal = framesCourant

    //fichierOutputTmp = await tmpPromises.file({keep: true, postfix: '.' + format})
    const destinationPath = path.join(tmpDir.path, 'output.' + format)
    // const destinationPath = fichierOutputTmp.path
    debug("Fichier temporaire output : %s", destinationPath)

    try {
      ok = false
      const transcodageVideo = transcoderPasse(2, input, destinationPath, videoOpts, audioOpts, optsTranscodage)
      stopFct = transcodageVideo.stop
      await transcodageVideo.promise // Attendre fin
      ok = true
    } catch(err) {
      // Verifier si on a une erreur de streaming (e.g. video .mov n'est pas
      // supporte en streaming)
      const errMsg = err.message
      if(errMsg.indexOf("ffmpeg exited with code 1") === -1) {
        throw err  // Erreur non geree
      }

      debug("Echec parsing stream, dechiffrer dans un fichier temporaire et utiliser directement")
      modeInputStream = false

      // Copier le contenu du stream dans un fichier temporaire
      input = path.join(tmpDir.path, 'input.dat')
      fichierInputTmp = await extraireFichierTemporaire(input, streamFactory())
      // input = fichierInputTmp.path
      debug("Fichier temporaire input pret: %s, videoOps: %O", input, videoOpts)

      // Recommencer passe 1 et faire passe 2
      ok = false
      passe = 1  // Pour progressCb
      let transcodageVideo = transcoderPasse(1, input, null, videoOpts, null, optsTranscodage)
      stopFct = transcodageVideo.stop
      await transcodageVideo.promise // Attendre fin
      ok = true
      debug("Passe 1 terminee pour %s", input)

      ok = false
      passe = 2  // Pour progressCb
      transcodageVideo = transcoderPasse(2, input, destinationPath, videoOpts, audioOpts, optsTranscodage)
      stopFct = transcodageVideo.stop
      await transcodageVideo.promise // Attendre fin
      ok = true
      debug("Passe 2 terminee pour %s", input)

    } finally {
      if(!ok && stopFct) stopFct()  // Forcer l'arret du processus
    }
    debug("Passe 2 terminee, transferer le fichier output")

    const outputFileReader = fs.createReadStream(destinationPath)
    const promiseOutput = new Promise((resolve, reject)=>{
      outputFileReader.on('error', err=>reject(err))
      outputFileReader.on('end', _=>resolve())
    })
    outputFileReader.pipe(outputStream)
    await promiseOutput

    // Faire un probe de l'output pour recuperer stats
    var videoInfo = await probeVideo(destinationPath)
    debug("Information video transcode: %O", videoInfo.raw)

    return {video: videoOpts, audio: audioOpts, probe: videoInfo.raw}
  } finally {
    if(tmpDir) {
      debug("Suppression du repertoire temporaire %s", tmpDir.path)
      tmpDir.cleanup()
    }
  }
}

function transcoderPasse(passe, source, destinationPath, videoOpts, audioOpts, opts) {
  videoOpts = videoOpts || {}
  audioOpts = audioOpts || {}  // Non-utilise pour passe 1
  opts = opts || {}

  const nbThreads = opts.threads || 4

  const videoBitrate = videoOpts.videoBitrate,
        height = videoOpts.height,
        width = videoOpts.width || '?',
        videoCodec = videoOpts.videoCodec

  const audioCodec = audioOpts.audioCodec,
        audioBitrate = audioOpts.audioBitrate

  const progressCb = opts.progressCb,
        format = opts.format || 'mp4',
        tmpDir = opts.tmpDir || '/tmp'

  const ffmpegProcessCmd = new FFmpeg(source, {niceness: 10})
    .withVideoBitrate(''+Math.floor(videoBitrate/1000)+'k')
    .withSize(''+ width + 'x' + height)
    .videoCodec(videoCodec)

  var passlog = path.join(tmpDir, 'ffmpeg2pass')
  if(passe === 1) {
    // Passe 1, desactiver traitement stream audio
    ffmpegProcessCmd
      .outputOptions([
        '-an',
        '-f', 'null',
        '-pass', '1',
        '-threads', ''+nbThreads,
        '-passlogfile', passlog
      ])
  } else if(passe === 2) {
    debug("Audio info : %O, format %s", audioOpts, format)
    ffmpegProcessCmd
      .audioCodec(audioCodec)
      .audioBitrate(audioBitrate)
      .outputOptions([
        '-pass', '2',
        '-threads', ''+nbThreads,
        // '-slices', ''+nbThreads,
        //'-cpu-used', ''+nbCores,
        '-movflags', 'faststart',
        '-metadata', 'COM.APPLE.QUICKTIME.LOCATION.ISO6709=',
        '-metadata', 'location=',
        '-metadata', 'location-eng=',
        '-passlogfile', passlog])
  } else {
    throw new Error("Passe doit etre 1 ou 2 (passe=%O)", passe)
  }

  if(progressCb) {
    ffmpegProcessCmd.on('progress', progressCb)
  }

  const processPromise = new Promise((resolve, reject)=>{
    ffmpegProcessCmd.on('error', function(err, stdout, stderr) {
      console.error('ERROR - transformationsVideo.transcoderPasse: %O\nstderr: %O\nInfo params: %O', err, stderr, ffmpegProcessCmd)
      reject(err);
    })
    ffmpegProcessCmd.on('end', function(filenames) {
      resolve()
    })
  })

  // Demarrer le traitement
  if(passe === 1) {
    // Aucun ouput a sauvegarder pour passe 1
    ffmpegProcessCmd.saveToFile('/dev/null')
  } else if(passe === 2) {
    ffmpegProcessCmd
      .saveToFile(destinationPath)
  }

  return {
    promise: processPromise,
    stop: ffmpegProcessCmd => {stopCommand(ffmpegProcessCmd)}
  }
}

async function stopCommand(commandeFfmpeg) {
  // Send custom signals
  const fct = async () => {
    commandeFfmpeg.kill('SIGSTOP')
    setTimeout( () => {
      commandeFfmpeg.kill('SIGCONT')
    }, 5000)
  }
  await fct
}

async function extraireFichierTemporaire(fichierPath, inputStream) {
  //const fichierInputTmp = await tmpPromises.file({keep: true})
  debug("Fichier temporaire : %s", fichierPath)

  const outputStream = fs.createWriteStream(fichierPath)
  const promiseOutput =  new Promise((resolve, reject)=>{
    outputStream.on('error', err=>{
      reject(err)
      // fichierInputTmp.cleanup()
    })
    outputStream.on('close', _=>{resolve()})
  })

  inputStream.pipe(outputStream)
  // inputStream.read()

  return promiseOutput
}

async function traiterCommandeTranscodage(mq, fichierDechiffre, clesPubliques, message, storeConsignation) {
  debug("Commande traiterCommandeTranscodage video recue : %O", message)

  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var tuuid = message.tuuid,
      fuuid = message.fuuid,
      mimetype = message.mimetype,
      videoBitrate = message.videoBitrate,
      height = message.resolutionVideo || message.height,
      uuidCorrelationCleanup = null

  try {
    const profil = getProfilTranscodage(message)
    if(!profil) {
      console.error("traiterCommandeTranscodage profil non trouvable : %O", message)
      throw new Error("Profil inconnue pour parametres fournis " + JSON.stringify(message))
    }
    videoBitrate = profil.videoBitrate
    height = profil.height

    // Transmettre evenement debut de transcodage
    mq.emettreEvenement({fuuid, mimetype, videoBitrate, height}, `evenement.fichiers.${fuuid}.transcodageDebut`)
    mq.emettreEvenement({fuuid, mimetype, videoBitrate, height}, `evenement.fichiers.${fuuid}.transcodageDebut`, {exchange: '2.prive'})

    // Fonction de progres
    const progressCb = progress => { progressUpdate(mq, {fuuid, mimetype, videoBitrate, height}, progress) }

    // Creer un factory d'input streams decipher
    const inputStreamFactory = () => { return fs.createReadStream(fichierDechiffre) }

    // Transmettre transaction info chiffrage
    const identificateurs_document = {
        attachement_fuuid: fuuid,
        type: 'video',
      }

    const opts = {...profil, progressCb}
    debug("Debut dechiffrage fichier video, opts : %O", opts)
    const fichierOutputTmp = await tmpPromises.file({prefix: 'video-', keep: true})
    try {
      const outputStream = fs.createWriteStream(fichierOutputTmp.path)
      let resultatTranscodage = await transcoderVideo(inputStreamFactory, outputStream, opts)
      debug("Resultat transcodage : %O", resultatTranscodage)

      // resultatUpload = await uploaderFichierTraite(mq, fichierOutputTmp.path, clesPubliques, identificateurs_document)
      var {uuidCorrelation, commandeMaitrecles, hachage, taille} = await transfertConsignation.stagerFichier(
        mq, fichierOutputTmp.path, clesPubliques, identificateurs_document, storeConsignation)
      uuidCorrelationCleanup = uuidCorrelation
      
      const probeInfo = resultatTranscodage.probe

      // Transmettre transaction associer video transcode
      var transactionAssocierVideo = {
        tuuid, fuuid,
  
        mimetype: message.mimetype,
        fuuid_video: hachage,
        hachage: hachage,
  
        width: probeInfo.width,
        height: probeInfo.height,
        codec: profil.videoCodecName,
        bitrate: resultatTranscodage.video.videoBitrate,
        taille_fichier: taille,
      }
      transactionAssocierVideo = await mq.pki.formatterMessage(
        transactionAssocierVideo, 'GrosFichiers', {action: 'associerVideo', ajouterCertificat: true})
      debug("Transaction transcoder video : %O", transactionAssocierVideo)

    } finally {
      // maintenant autoclean
      // fichierOutputTmp.cleanup().catch(err=>{debug("Err cleanup fichier video tmp (OK) : %O", err)})
    }

    await storeConsignation.stagingReady(mq, transactionAssocierVideo, commandeMaitrecles, uuidCorrelation)

    // const probeInfo = resultatTranscodage.probe

    // // Transmettre transaction associer video transcode
    // const transactionAssocierPreview = {
    //   tuuid, fuuid,

    //   mimetype: message.mimetype,
    //   fuuid_video: resultatUpload.hachage,
    //   hachage: resultatUpload.hachage,

    //   width: probeInfo.width,
    //   height: probeInfo.height,
    //   codec: profil.videoCodecName,
    //   bitrate: resultatTranscodage.video.videoBitrate,
    //   taille_fichier: resultatUpload.taille,
    // }

    mq.emettreEvenement({fuuid, mimetype, videoBitrate, height}, `evenement.fichiers.${fuuid}.transcodageTermine`)
    mq.emettreEvenement({fuuid, mimetype, videoBitrate, height}, `evenement.fichiers.${fuuid}.transcodageTermine`, {exchange: '2.prive'})

    // Transmettre transaction pour associer le video au fuuid
    // const domainePreview = 'GrosFichiers', actionPreview = 'associerVideo'
    // await mq.transmettreTransactionFormattee(transactionAssocierPreview, domainePreview, {action: actionPreview, exchange: '4.secure', ajouterCertificat: true})
  } catch(err) {
    console.error("transformationsVideo: Erreur transcodage : %O", err)
    if(uuidCorrelationCleanup) storeConsignation.stagingDelete(uuidCorrelationCleanup)
    mq.emettreEvenement({fuuid, mimetype, videoBitrate, height, err: ''+err}, `evenement.fichiers.${fuuid}.transcodageErreur`)
    mq.emettreEvenement({fuuid, mimetype, videoBitrate, height, err: ''+err}, `evenement.fichiers.${fuuid}.transcodageErreur`, {exchange: '2.prive'})
    throw err
  }
}

function getProfilTranscodage(params) {
  let profil = null

  // Exemple de params pour un profil complet
  // videoBitrate: 1000000,
  // height: 720,
  // videoCodec: 'libvpx-vp9',
  // audioCodec: 'libopus',
  // audioBitrate: '128k',
  // format: 'webm',
  // videoCodecName: 'vp9',

  // Verifier si on a toute l'information de transcodate inclus dans les params
  switch(params.mimetype) {
    case 'video/webm':
      profil = {...PROFILS_TRANSCODAGE.webm, ...params}
      break
    case 'video/mp4':
      profil = {...PROFILS_TRANSCODAGE.mp4, ...params}
      break
  }

  // Note : le mimetype a deja inclus l'information de codecVideo (webm === VP9, mp4 === h264)

  // Mapping codec audio
  if(params.bitrateVideo) {
    profil.videoBitrate = params.bitrateVideo
  }
  if(params.bitrateAudio) {
    profil.audioBitrate = params.audioBitrate
  }
  if(params.resolutionVideo) {
    profil.height = params.resolutionVideo
  }
  if(params.codecAudio === 'aac') {
    profil.audioCodec = 'aac'
  } else if(params.codecAudio === 'opus') {
    profil.audioCodec = 'libopus'
  }

  return profil
}

function progressUpdate(mq, paramsVideo, progress) {
  /* Transmet un evenement de progres pour un transcodage video */
  var pctProgres = '', ponderation = 1, bump = 0

  if(progress.passe === 1) {
    ponderation = 10
  } else if (progress.passe === 2) {
    ponderation = 90
    bump = 10
  }

  if(progress.framesTotal && progress.frames) {
    // Methode la plus precise
    pctProgres = Math.floor(progress.frames * ponderation / progress.framesTotal) + bump
  } else if(progress.percent) {
    // Moins precis, se fier a ffmpeg
    pctProgres = progress.percent
  }

  if(pctProgres) {
    const {mimetype, fuuid} = paramsVideo
    debug("Progres %s vers %s %d%", fuuid, mimetype, pctProgres)

    const domaineAction = `evenement.fichiers.${fuuid}.transcodageProgres`
    const contenuEvenement = {...paramsVideo, pctProgres, passe: progress.passe}
    mq.emettreEvenement(contenuEvenement, domaineAction)
    mq.emettreEvenement(contenuEvenement, domaineAction, {exchange: '2.prive'})
  }
}

module.exports = {
  probeVideo, transcoderVideo, traiterCommandeTranscodage,
}
