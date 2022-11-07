const debug = require('debug')('media:app')
const express = require('express')
const path = require('path')
const { extraireExtensionsMillegrille } = require('@dugrema/millegrilles.utiljs/src/forgecommon')

const initRouteStream = require('./routeStream')
const {verificationCertificatSSL} = require('./pki')

function initialiser(mq, opts) {
  debug("Initialiser app, opts : %O", opts)
  opts = opts || {}
  const middleware = opts.middleware

  const modeStream = detecterModeStream(mq)

  var app = express()

  if(middleware) {
    // Ajouter middleware a mettre en premier
    middleware.forEach(item=>{
      app.use(item)
    })
  }

  // Ajouter composant d'autorisation par certificat client SSL
  app.use(verificationCertificatSSL)

  // // Inject RabbitMQ pour la MilleGrille detectee sous etape SSL
  // app.use((req, res, next)=>{
  //   req.storeConsignation = storeConsignation
  //   next()
  // })

  app.use(express.json())

  app.use(express.urlencoded({ extended: false }))

  app.use(express.static(path.join(__dirname, 'public')))

  const traitementStream = initRouteStream(mq, {...opts, stream: modeStream})
  app.all('/stream_transfert/*', traitementStream)
  app.all('/*/streams/*', traitementStream)

  // catch 404 and forward to error handler
  app.use(function(req, res, next) {
    console.error("app Ressource inconnue : ", req.url);
    res.sendStatus(404);
  })

  // error handler
  app.use(function(err, req, res, next) {
    console.error("app Erreur generique\n%O", err);
    res.sendStatus(err.status || 500);
  })

  // Activer nettoyage sur cedule des repertoires de staging
  //setInterval(cleanupStaging, 5 * 60 * 1000)  // Cleanup aux 5 minutes

  return app;
}

function detecterModeStream(mq) {
    const cert = mq.pki.cert
    const extensions = extraireExtensionsMillegrille(cert)

    debug("Extensions certificat : ", extensions)
    const roles = extensions.roles || []
    const niveauxSecurite = extensions.niveauxSecurite || []

    if(niveauxSecurite.includes('4.secure')) {
      // On a un certificat qui permet de dechiffrer directement sans passer par GrosFichiers
      return false
    }

    if(roles.includes('stream') && niveauxSecurite.includes('2.prive')) {
        console.info("*** Activation mode streaming pour rechiffrage cles ***")
        return true
    }

    throw new Error("Certificat sans permission de dechiffrage : ", extensions)
}

module.exports = {initialiser}
