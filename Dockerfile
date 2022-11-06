FROM docker.maceroc.com/nodejsmedia:18_0

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades
VOLUME /var/opt/millegrilles

ENV PORT=443 \
    HOST=media \
    NODE_ENV=production \
    SERVER_TYPE=https \
    MG_MQ_URL=amqps://mq:5673 \
    MG_REDIS_HOST=redis \
    MG_REDIS_PORT=6379 \
    MG_MQ_REDIS_PASSWD=/run/secrets/passwd.redis.txt \
    MG_MQ_CERTFILE=/run/secrets/cert.pem \
    MG_MQ_KEYFILE=/run/secrets/key.pem \
    MG_MQ_CAFILE=/run/secrets/millegrille.cert.pem \
    WEB_CERT=/run/secrets/cert.pem \
    WEB_KEY=/run/secrets/key.pem

EXPOSE 443

COPY ./ ./

RUN export NODE_OPTIONS=--openssl-legacy-provider && \
    npm i --omit-dev && \
    rm -rf /root/.npm

CMD [ "npm", "start" ]
