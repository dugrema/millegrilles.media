const debug = require('debug')('millegrilles:amqpdao')
const fs = require('fs')
const { MilleGrillesPKI, MilleGrillesAmqpDAO } = require('@dugrema/millegrilles.nodejs')

const EXPIRATION_MESSAGE_DEFAUT = 30 * 60 * 1000  // minutes en millisec
const EXPIRATION_MESSAGE_VIDEO = 10 * 60 * 1000   // minutes en millisec

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
  await instPki.initialiserPkiPEMS(certPems)

  if(opts.redisClient) {
    // Injecter le redisClient dans pki
    instPki.redisClient = opts.redisClient
  }

  // Connecter a MilleGrilles avec AMQP DAO
  // const nomsQCustom = ['image', 'video', 'publication']
  const qCustom = {
    'image': {ttl: EXPIRATION_MESSAGE_DEFAUT, name: 'media/image'},

    // transcodage peut prendre plus de 30 minutes (ACK timeout)
    'video': {ttl: EXPIRATION_MESSAGE_VIDEO, name: 'media/video', preAck: true},

    // indexation peut prendre plus de 30 minutes (ACK timeout)
    'indexation': {ttl: EXPIRATION_MESSAGE_DEFAUT, name: 'media/indexation', preAck: true},
  }
  const amqpdao = new MilleGrillesAmqpDAO(instPki, {qCustom})
  const mqConnectionUrl = process.env.MG_MQ_URL;
  await amqpdao.connect(mqConnectionUrl)

  // Attacher les evenements, cles de routage
  const {media} = await initialiserRabbitMQ(amqpdao)

  // Middleware, injecte l'instance
  const middleware = (req, res, next) => {
    req.amqpdao = amqpdao
    next()
  }

  return {middleware, amqpdao, messageHandler: media}
}

async function initialiserRabbitMQ(rabbitMQ) {
  // Creer objets de connexion a MQ - importer librairies requises
  const {PkiMessages} = require('./messages/pki');
  rabbitMQ.enregistrerListenerConnexion(new PkiMessages(rabbitMQ));

  // const {DecrypterFichier} = require('../messages/crypto');
  // rabbitMQ.enregistrerListenerConnexion(new DecrypterFichier(rabbitMQ));

  const media = require('./messages/media')
  media.setMq(rabbitMQ)
  rabbitMQ.enregistrerListenerConnexion(media);

  // const publication = require('../messages/publication')
  // publication.init(rabbitMQ)
  // rabbitMQ.enregistrerListenerConnexion(publication)

  return {rabbitMQ, media};
}

module.exports = {init}
