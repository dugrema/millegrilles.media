const debug = require('debug')('millegrilles:amqpdao')
const fs = require('fs')
const { MilleGrillesPKI, MilleGrillesAmqpDAO } = require('@dugrema/millegrilles.nodejs')

const EXPIRATION_MESSAGE_DEFAUT = 30 * 60 * 1000  // minutes en millisec
const EXPIRATION_MESSAGE_VIDEO = 10 * 60 * 1000   // minutes en millisec

// const activerQueuesProcessing = process.env.DISABLE_Q_PROCESSING?false:true

async function init(opts) {
  opts = opts || {}
  const activerQueuesProcessing = !!opts.activerQueuesProcessing

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
  const qCustom = {}
  // if(activerQueuesProcessing) {
  //   const commonName = instPki.cert.subject.getField('CN').value
  //   debug("Creer Qs de traitement media avec common name : ", commonName)

  //   qCustom.image = {ttl: EXPIRATION_MESSAGE_DEFAUT, name: `media/${commonName}/image`, autoDelete: false}
  //   qCustom.pdf = {ttl: EXPIRATION_MESSAGE_DEFAUT, name: `media/${commonName}/pdf`, autoDelete: false}
    
  //   // transcodage peut prendre plus de 30 minutes (ACK timeout)
  //   qCustom.video = {ttl: EXPIRATION_MESSAGE_VIDEO, name: `media/${commonName}/video`, preAck: true, autoDelete: false}

  //   // indexation peut prendre plus de 30 minutes (ACK timeout)
  //   qCustom.indexation = {ttl: EXPIRATION_MESSAGE_DEFAUT, name: `media/${commonName}/indexation`, preAck: true, autoDelete: false}
  // } else {
  //   console.info("INFO: amqpdao Q processing est desactive")
  // }
  const amqpdao = new MilleGrillesAmqpDAO(instPki, {qCustom})
  const mqConnectionUrl = process.env.MG_MQ_URL;
  await amqpdao.connect(mqConnectionUrl)

  // Attacher les evenements, cles de routage
  const {media} = await initialiserRabbitMQ(amqpdao, activerQueuesProcessing)

  // Middleware, injecte l'instance
  const middleware = (req, res, next) => {
    req.amqpdao = amqpdao
    next()
  }

  return {middleware, amqpdao, messageHandler: media}
}

async function initialiserRabbitMQ(rabbitMQ, activerQueuesProcessing) {
  // Creer objets de connexion a MQ - importer librairies requises
  const {PkiMessages} = require('./messages/pki');
  rabbitMQ.enregistrerListenerConnexion(new PkiMessages(rabbitMQ));

  // const {DecrypterFichier} = require('../messages/crypto');
  // rabbitMQ.enregistrerListenerConnexion(new DecrypterFichier(rabbitMQ));

  const media = require('./messages/media')
  media.setMq(rabbitMQ, {activerQueuesProcessing})
  rabbitMQ.enregistrerListenerConnexion(media);

  // const publication = require('../messages/publication')
  // publication.init(rabbitMQ)
  // rabbitMQ.enregistrerListenerConnexion(publication)

  return {rabbitMQ, media};
}

module.exports = {init}
