class PkiMessages {

  constructor(mq) {
    this.mq = mq;
    this.fingerprint = null;
    this.routingKeys = null;
  }

  // Appele lors d'une reconnexion MQ
  on_connecter() {
    this.enregistrerChannel();
  }

  enregistrerChannel() {
    this.fingerprint = this.mq.pki.fingerprint  // transmettreCertificat(this.mq);
    this.routingKeys = [
      'requete.certificat.' + this.fingerprint
    ]

    const mq = this.mq;
    this.mq.routingKeyManager.addRoutingKeyCallback(
      function(routingKeys, message, opts) {transmettreCertificat(mq, routingKeys, message, opts)},
      this.routingKeys
    );
  }

}

function transmettreCertificat(mq, routingKeys, message, opts) {
  var replyTo, correlationId;
  if(opts && opts.properties) {
    replyTo = opts.properties.replyTo;
    correlationId = opts.properties.correlationId;
  }

  let messageCertificat = mq.pki.preparerMessageCertificat();
  let fingerprint = messageCertificat.fingerprint;

  if(replyTo && correlationId) {
    // console.debug("Reponse a " + replyTo + ", correlation " + correlationId);
    mq.transmettreReponse(messageCertificat, replyTo, correlationId);
  } else {
    let messageJSONStr = JSON.stringify(messageCertificat);
    mq._publish(
      'evenement.certificat.infoCertificat', messageJSONStr
    );
  }

  return fingerprint;
}

module.exports = {PkiMessages};
