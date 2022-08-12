const debug = require('debug')('media:transformationsVideo')
const fs = require('fs')
const tmpPromises = require('tmp-promise')
const path = require('path')
const FFmpeg = require('fluent-ffmpeg')

const transfertConsignation = require('./transfertConsignation')

const CONST_INTERVALLE_UPDATE = 3 * 1000
const CONST_PASSE_1 = 1

const PROFILS_TRANSCODAGE = {
  h264: {
    qualityVideo: 30,
    videoBitrate: 250000,
    height: 270,
    videoCodec: 'libx264',
    audioCodec: 'aac',
    audioBitrate: '64k',
    format: 'mp4',
    videoCodecName: 'h264',
    doublePass: false,
    preset: 'veryfast',
  },
  vp9: {
    qualityVideo: 36,
    height: 360,
    videoCodec: 'libvpx-vp9',
    audioCodec: 'libopus',
    audioBitrate: '128k',
    format: 'webm',
    videoCodecName: 'vp9',
    doublePass: true,
    preset: 'medium',
  },
  hevc: {
    qualityVideo: 30,
    height: 360,
    videoCodec: 'libx265',
    audioCodec: 'eac3',
    audioBitrate: '128k',
    format: 'mp4',
    videoCodecName: 'hevc',
    doublePass: false,
    preset: 'medium',
  },
}

async function probeVideo(input, opts) {
  opts = opts || {}

  if(typeof(input) === 'string') {
    input = fs.createReadStream(input)
  }

  const maxHeight = opts.maxHeight || 360,
        maxBitrate = opts.maxBitrate || 250000,
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
    heightEncoding = [1080, 720, 480, 360, 320, 270].filter(item=>{
      return item <= height && item <= maxHeight
    }).shift() || 270
  }
  let bitRateEncoding = [2500000, 1600000, 1000000, 600000, 400000, 200000].filter(item=>{
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

async function transcoderVideo(fichierInput, outputStream, opts) {
  if(!opts) opts = {}

  debug("transcoderVideo opts : %O", opts)

  var   videoBitrate = opts.videoBitrate || 200000,
        videoQuality = opts.qualityVideo,
        preset = opts.preset,
        height = opts.height || 270,
        width = opts.width || 480,
        utiliserTailleOriginale = opts.utiliserTailleOriginale || false,
        doublePass = opts.doublePass===true?true:false

  const videoCodec = opts.videoCodec || 'libx264',
        audioCodec = opts.audioCodec || 'aac',
        audioBitrate = opts.audioBitrate || '64k',
        format = opts.format || 'mp4',
        progressCb = opts.progressCb

  if(progressCb) progressCb({percent: 0})

  // Tenter transcodage avec un stream - fallback sur fichier direct
  const videoInfoOriginal = await probeVideo(fichierInput, {maxBitrate: videoBitrate, maxHeight: height, utiliserTailleOriginale})
  videoBitrate = opts.videoBitrate?videoInfoOriginal.bitrate:null  // Bitrate null signifie variable selon quality
  height = videoInfoOriginal.height || height
  width = videoInfoOriginal.width
  
  let progressHook = null, 
      framesTotal = videoInfoOriginal.nb_frames, 
      framesCourant = 0,
      passe = null

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

    // Faire un premier appel pour eviter que la job de conversion soit reallouee
    progressHook({percent: 0})
  }

  // S'assurer que width est pair (requis par certains codec comme HEVC)
  if(width%2 !== 0) width = width - 1

  // videoBitrate = '' + (videoBitrate / 1000) + 'k'
  debug('Utilisation video quality %s, bitrate : %s, format %dx%d\nInfo: %O', videoQuality, videoBitrate, width, height, videoInfoOriginal)

  // Creer repertoire temporaire pour fichiers de log, outputfile
  const tmpDir = await tmpPromises.dir({unsafeCleanup: true})

  const videoOpts = { videoQuality, videoBitrate, height, width, videoCodec, preset }
  const optsTranscodage = { progressCb: progressHook, tmpDir: tmpDir.path }
  const audioOpts = {audioCodec, audioBitrate}

  try {

    if(doublePass) {
      debug("Debut passe 1")
      passe = 1
      await passe1Video(fichierInput, tmpDir, videoOpts, optsTranscodage)
      passe = 2  // Commencer la deuxieme passe
      debug("Passe 1 terminee, debut passe 2")
    }

    let stopFct = null, ok = false  // Sert a arreter ffmpeg si probleme avec nodejs (e.g. CTRL-C/restart container)

    // Set nombre de frames trouves dans la passe 1 au besoin (sert au progress %)
    if(!framesTotal) framesTotal = framesCourant

    //fichierOutputTmp = await tmpPromises.file({keep: true, postfix: '.' + format})
    const destinationPath = path.join(tmpDir.path, 'output.' + format)
    // const destinationPath = fichierOutputTmp.path
    debug("Fichier temporaire output : %s", destinationPath)

    try {
      ok = false
      const transcodageVideo = transcoderPasse(passe, fichierInput, destinationPath, videoOpts, audioOpts, optsTranscodage)
      stopFct = transcodageVideo.stop
      await transcodageVideo.promise // Attendre fin
      ok = true
    } finally {
      if(!ok && stopFct) stopFct()  // Forcer l'arret du processus
    }
    debug("Transcodage termine, transferer le fichier output")

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

async function passe1Video(inputStream, tmpDir, videoOpts, optsTranscodage) {
  // Passe 1
  debug("Debut passe 1")

  const optsTranscodagePasse1 = {...optsTranscodage, tmpDir: tmpDir.path}

  let stopFct = null, ok = false  // Sert a arreter ffmpeg si probleme avec nodejs (e.g. CTRL-C/restart container)
  try {
    const transcodageVideo = transcoderPasse(CONST_PASSE_1, inputStream, null, videoOpts, null, optsTranscodagePasse1)
    stopFct = transcodageVideo.stop
    await transcodageVideo.promise // Attendre fin
    ok = true
  } finally {
    if(!ok && stopFct) stopFct()  // Forcer l'arret du processus
  }
  debug("Passe 1 terminee")
}

function transcoderPasse(passe, source, destinationPath, videoOpts, audioOpts, opts) {
  videoOpts = videoOpts || {}
  audioOpts = audioOpts || {}  // Non-utilise pour passe 1
  opts = opts || {}

  debug("transcoderPasse (%s) destinationPath: %s\nvideoOpts: %O\naudioOpts: %O\nopts: %O", 
        passe, destinationPath, videoOpts, audioOpts, opts)

  const nbThreads = opts.threads || 4

  const videoBitrate = videoOpts.videoBitrate,
        height = videoOpts.height,
        width = videoOpts.width || '?',
        videoCodec = videoOpts.videoCodec,
        preset = videoOpts.preset,
        videoQuality = videoOpts.videoQuality

  const audioCodec = audioOpts.audioCodec,
        audioBitrate = audioOpts.audioBitrate

  const progressCb = opts.progressCb,
        format = opts.format || 'mp4',
        tmpDir = opts.tmpDir || '/tmp'

  const sizeVideo = ''+ width + 'x' + height
  // const sizeVideo = '?x' + height
  debug("Size video : %s", sizeVideo)

  const ffmpegProcessCmd = new FFmpeg(source, {niceness: 10, logger: console})
    .withSize(sizeVideo)
    .videoCodec(videoCodec)

  const inputOptions = []

  debug("FFMPEG inputOptions: %O", inputOptions)
  ffmpegProcessCmd.inputOptions(inputOptions)
  
  var passlog = path.join(tmpDir, 'ffmpeg2pass')
  if(passe === 1) {
    // Passe 1, desactiver traitement stream audio
    ffmpegProcessCmd
      //.noAudio()
      .outputOptions([
        '-an',
        '-f', 'null',
        '-pass', '1',
        '-threads', ''+nbThreads,
        '-passlogfile', passlog
      ])
  } else {
    debug("Audio info : %O, format %s", audioOpts, format)
    let outputOptions = [
        '-threads', ''+nbThreads,
        // '-slices', ''+nbThreads,
        //'-cpu-used', ''+nbCores,
        '-movflags', 'faststart',
        '-metadata', 'COM.APPLE.QUICKTIME.LOCATION.ISO6709=',
        '-metadata', 'location=',
        '-metadata', 'location-eng=',
    ]

    if(preset) {
      outputOptions.push('-preset')
      outputOptions.push(preset)
    }
  
    if(videoCodec === 'libx265') {
      outputOptions.push('-tag:v')
      outputOptions.push('hvc1')
    }
  
    if(videoQuality) {
      outputOptions.push('-crf')
      outputOptions.push(''+videoQuality)
      if(videoBitrate) {
        const bitrate = ''+Math.floor(videoBitrate/1000)+'k'
        debug("Video bitrate : %s avec crf %s", bitrate, videoQuality)
        ffmpegProcessCmd.withVideoBitrate(bitrate)
      } else if(videoCodec === 'vp9' || !videoOpts.videoBitrate) {
        debug("Video bitrate : '0' pour VP9 avec crf %s", videoQuality)
        ffmpegProcessCmd.withVideoBitrate('0')  // Flag requis pour bitrate variable
      }
    } else if(videoBitrate) {
      const bitrate = ''+Math.floor(videoBitrate/1000)+'k'
      debug("Video bitrate constant %s", bitrate)
      ffmpegProcessCmd.withVideoBitrate(''+Math.floor(videoBitrate/1000)+'k')
    }
  
    if(passe === 2) {
      outputOptions = ['-pass', '2', ...outputOptions, '-passlogfile', passlog]
    }

    debug("FFMPEG audioCodec: %s, audioBitrate: %s, outputOptions : %O", audioCodec, audioBitrate, outputOptions)

    const audioBitrateStr = Math.floor(audioBitrate / 1000) + 'k'

    ffmpegProcessCmd
      .audioCodec(audioCodec)
      .audioBitrate(audioBitrateStr)
      .outputOptions(outputOptions)
  }

  if(progressCb) {
    ffmpegProcessCmd.on('progress', progressCb)
    // Faire un appel progress pour eviter la reallocation du video
    progressCb({percent: 0, passe})
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

  debug("Commande ffmpeg : %O", ffmpegProcessCmd)

  // Demarrer le traitement
  if(passe === 1) {
    // Aucun ouput a sauvegarder pour passe 1
    ffmpegProcessCmd.saveToFile('/dev/null')
  } else {
    ffmpegProcessCmd.saveToFile(destinationPath)
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

// async function extraireFichierTemporaire(fichierPath, inputStream) {
//   //const fichierInputTmp = await tmpPromises.file({keep: true})
//   debug("Fichier temporaire : %s", fichierPath)

//   const outputStream = fs.createWriteStream(fichierPath)
//   const promiseOutput =  new Promise((resolve, reject)=>{
//     outputStream.on('error', err=>{
//       reject(err)
//       // fichierInputTmp.cleanup()
//     })
//     outputStream.on('close', _=>{resolve()})
//   })

//   inputStream.pipe(outputStream)
//   // inputStream.read()

//   return promiseOutput
// }

async function traiterCommandeTranscodage(mq, fichierDechiffre, clesPubliques, message, storeConsignation) {
  debug("Commande traiterCommandeTranscodage video recue : %O", message)

  // Verifier si le preview est sur une image chiffree - on va avoir une permission de dechiffrage
  var tuuid = message.tuuid,
      fuuid = message.fuuid,
      user_id = message.user_id,
      mimetype = message.mimetype,
      videoBitrate = message.videoBitrate,
      height = message.resolutionVideo || message.height,
      uuidCorrelationCleanup = null

  let videoQuality = message.qualityVideo,
      videoCodec = message.codecVideo

  try {
    const profil = getProfilTranscodage(message)
    if(!profil) {
      console.error("traiterCommandeTranscodage profil non trouvable : %O", message)
      throw new Error("Profil inconnue pour parametres fournis " + JSON.stringify(message))
    }
    videoBitrate = profil.videoBitrate
    height = profil.height
    videoQuality = profil.qualityVideo || videoQuality
    videoCodec = profil.videoCodecName || videoCodec

    debug("Profil video : %O", profil)

    // Transmettre evenement debut de transcodage
    mq.emettreEvenement({fuuid, mimetype, videoCodec, videoQuality, videoBitrate, height}, `evenement.fichiers.${fuuid}.transcodageDebut`, {exchange: '2.prive'})

    // Fonction de progres
    let lastUpdate = null, complet = false
    const progressCb = (progress, opts) => { 
      opts = opts || {}
      const etat = opts.etat || 'transcodage'
      // debug("traiterCommandeTranscodage Progress update %s / %s;%s:%s : %O", fuuid, mimetype, height, videoBitrate, progress)
      if(progress && progress.force === true) {
        // Ok
      } else if(lastUpdate) {
        if(complet && progress.frames < progress.framesTotal) {
          complet = false  // Nouvelle passe, reset flag
        } else if(!complet && progress.framesTotal === progress.frames && progress.framesTotal) {
          // debug("traiterCommandeTranscodage Complete (passe %s, %s frames)", progress.passe, progress.framesTotal)
          complet = true  // On emet le message de fin une fois
        } else {
          const expiration = new Date().getTime() - CONST_INTERVALLE_UPDATE
          if(lastUpdate > expiration) return
        }
      }
      lastUpdate = new Date().getTime()  // Reset
      progressUpdate(mq, {fuuid, mimetype, videoCodec, videoQuality, videoBitrate, height, etat}, progress) 
    }

    // Transmettre transaction info chiffrage
    const identificateurs_document = {
        attachement_fuuid: fuuid,
        type: 'video',
      }

    const opts = {...profil, progressCb}
    debug("Debut dechiffrage fichier video, opts : %O", opts)
    const fichierOutputTmp = await tmpPromises.file({prefix: 'video-', keep: true})
    try {
      progressCb({percent: 0}, {etat: 'transcodageDebut'})
      const outputStream = fs.createWriteStream(fichierOutputTmp.path)
      let resultatTranscodage = await transcoderVideo(fichierDechiffre, outputStream, opts)
      debug("Resultat transcodage : %O", resultatTranscodage)

      // resultatUpload = await uploaderFichierTraite(mq, fichierOutputTmp.path, clesPubliques, identificateurs_document)
      const stagingInfo = await transfertConsignation.stagerFichier(
        mq, fichierOutputTmp.path, clesPubliques, identificateurs_document, storeConsignation)
      // debug("Video Staging info : %O", stagingInfo)
      var {uuidCorrelation, commandeMaitrecles, hachage, taille} = stagingInfo
      uuidCorrelationCleanup = uuidCorrelation
      
      const probeInfo = resultatTranscodage.probe

      // Transmettre transaction associer video transcode
      var transactionAssocierVideo = {
        tuuid, fuuid, user_id,
  
        mimetype: message.mimetype,
        fuuid_video: hachage,
        hachage: hachage,
  
        width: probeInfo.width,
        height: probeInfo.height,
        codec: profil.videoCodecName,
        bitrate: resultatTranscodage.video.videoBitrate,
        quality: videoQuality,
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
    progressCb({percent: 100}, {etat: 'termine'})
    
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

    // mq.emettreEvenement({fuuid, mimetype, videoCodec, videoQuality, videoBitrate, height}, `evenement.fichiers.${fuuid}.transcodageTermine`, {exchange: '2.prive'})

    // Transmettre transaction pour associer le video au fuuid
    // const domainePreview = 'GrosFichiers', actionPreview = 'associerVideo'
    // await mq.transmettreTransactionFormattee(transactionAssocierPreview, domainePreview, {action: actionPreview, exchange: '4.secure', ajouterCertificat: true})
  } catch(err) {
    console.error("transformationsVideo: Erreur transcodage : %O", err)
    if(uuidCorrelationCleanup) storeConsignation.stagingDelete(uuidCorrelationCleanup)
    progressCb({percent: -1}, {etat: 'erreur'})
    mq.emettreEvenement({fuuid, mimetype, videoCodec, videoQuality, videoBitrate, height, err: ''+err}, `evenement.fichiers.${fuuid}.transcodageErreur`)
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
  switch(params.codecVideo) {
    case 'vp9':
      profil = {...PROFILS_TRANSCODAGE.vp9, ...params}
      break
    case 'hevc':
      profil = {...PROFILS_TRANSCODAGE.hevc, ...params}
      break
    default:
      profil = {...PROFILS_TRANSCODAGE.h264, ...params}
      break
  }

  // Mapping codec audio
  if(params.bitrateVideo) {
    profil.videoBitrate = params.bitrateVideo
  }
  if(params.bitrateAudio) {
    profil.audioBitrate = params.bitrateAudio
  }
  if(params.resolutionVideo) {
    profil.height = params.resolutionVideo
  }
  if(params.codecAudio === 'opus') {
    profil.audioCodec = 'libopus'
  } else {
    profil.audioCodec = params.codecAudio
  }

  return profil
}

function progressUpdate(mq, paramsVideo, progress) {
  /* Transmet un evenement de progres pour un transcodage video */
  var pctProgres = 0, ponderation = 100, bump = 0

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
    pctProgres = Math.floor(progress.percent)
  }

  if(!isNaN(pctProgres)) {
    const {mimetype, fuuid} = paramsVideo
    // debug("Progres %s vers %s %d%", fuuid, mimetype, pctProgres)
    debug("Progres %d %O", pctProgres, paramsVideo)

    const domaineAction = `evenement.fichiers.${fuuid}.transcodageProgres`
    const contenuEvenement = {...paramsVideo, pctProgres, passe: progress.passe}
    // mq.emettreEvenement(contenuEvenement, domaineAction)
    mq.emettreEvenement(contenuEvenement, domaineAction, {exchange: '2.prive'})
  }
}

module.exports = {
  probeVideo, transcoderVideo, traiterCommandeTranscodage, progressUpdate,
}
