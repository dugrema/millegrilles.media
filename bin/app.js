const debug = require('debug')('media:app')
var express = require('express')
var path = require('path')

const initRouteStream = require('./routeStream')
const {verificationCertificatSSL} = require('./pki')

function initialiser(mq, opts) {
  debug("Initialiser app, opts : %O", opts)
  opts = opts || {}
  const middleware = opts.middleware

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

  const traitementStream = initRouteStream(mq, opts)
  app.all('/stream_transfert/*', traitementStream)

  // catch 404 and forward to error handler
  app.use(function(req, res, next) {
    console.error("Ressource inconnue");
    res.sendStatus(404);
  })

  // error handler
  app.use(function(err, req, res, next) {
    console.error("Erreur generique\n%O", err);
    res.sendStatus(err.status || 500);
  })

  // Activer nettoyage sur cedule des repertoires de staging
  //setInterval(cleanupStaging, 5 * 60 * 1000)  // Cleanup aux 5 minutes

  return app;
}

module.exports = {initialiser}
