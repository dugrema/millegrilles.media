FROM docker.maceroc.com/nodejsmedia:16_4

# Create app directory
WORKDIR /usr/src/app

# Volume pour le staging des fichiers uploades via coupdoeil
VOLUME /var/opt/millegrilles

ENV PORT=443 \
    HOST=fichiers \
    NODE_ENV=production

EXPOSE 443

COPY ./ ./

RUN npm i --production && \
    rm -rf /root/.npm

CMD [ "npm", "start" ]
