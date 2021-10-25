const debug = require('debug')('millegrilles:amqpdao')
const fs = require('fs')
const {MilleGrillesPKI, MilleGrillesAmqpDAO} = require('@dugrema/millegrilles.common')

const EXPIRATION_MESSAGE_DEFAUT = 30 * 60 * 1000  // 15 minutes en millisec

async function init(opts) {
  opts = opts || {}

  // Preparer certificats
  const certPem = fs.readFileSync(process.env.MG_MQ_CERTFILE).toString('utf-8')
  const keyPem = fs.readFileSync(process.env.MG_MQ_KEYFILE).toString('utf-8')
  const certMillegrillePem = fs.readFileSync(process.env.MG_MQ_CAFILE).toString('utf-8')

  // Charger certificats, PKI
  const certPems = {
    millegrille: certMillegrillePem,
    cert: certPem,
    key: keyPem,
  }

  // Charger PKI
  const instPki = new MilleGrillesPKI()
  instPki.initialiserPkiPEMS(certPems)

  if(opts.redisClient) {
    // Injecter le redisClient dans pki
    instPki.redisClient = opts.redisClient
  }

  // Connecter a MilleGrilles avec AMQP DAO
  // const nomsQCustom = ['image', 'video', 'publication']
  const qCustom = {
    'image': {ttl: EXPIRATION_MESSAGE_DEFAUT},
    'video': {ttl: EXPIRATION_MESSAGE_DEFAUT},
    'publication': {},
  }
  const amqpdao = new MilleGrillesAmqpDAO(instPki, {qCustom})
  const mqConnectionUrl = process.env.MG_MQ_URL;
  await amqpdao.connect(mqConnectionUrl)

  // Attacher les evenements, cles de routage
  await initialiserRabbitMQ(amqpdao)

  // Middleware, injecte l'instance
  const middleware = (req, res, next) => {
    req.amqpdao = amqpdao
    next()
  }

  return {middleware, amqpdao}
}

async function initialiserRabbitMQ(rabbitMQ) {
  // Creer objets de connexion a MQ - importer librairies requises
  const {PkiMessages} = require('./messages/pki');
  rabbitMQ.enregistrerListenerConnexion(new PkiMessages(rabbitMQ));

  // const {DecrypterFichier} = require('../messages/crypto');
  // rabbitMQ.enregistrerListenerConnexion(new DecrypterFichier(rabbitMQ));

  const {GenerateurMedia} = require('./messages/media');
  rabbitMQ.enregistrerListenerConnexion(new GenerateurMedia(rabbitMQ));

  // const {GestionnaireMessagesBackup} = require('../messages/backup')
  // rabbitMQ.enregistrerListenerConnexion(new GestionnaireMessagesBackup(rabbitMQ))

  // const publication = require('../messages/publication')
  // publication.init(rabbitMQ)
  // rabbitMQ.enregistrerListenerConnexion(publication)

  return {rabbitMQ};
}

module.exports = {init}
