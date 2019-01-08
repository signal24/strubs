const HttpServer = require('./http/server');
const FuseServer = require('./fuse/server');
const log = require('../log')('server-manager');

class ServerManager {
    constructor() {
        this.servers = [];
    }

    init() {
        log('starting server manager');

        this.servers.push(new HttpServer);
        this.servers.push(new FuseServer);
        this.servers.forEach(server => server.start());
    }
}

module.exports = new ServerManager();