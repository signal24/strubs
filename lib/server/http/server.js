const http = require('http');

const database = require('../../database');
const log = require('../../log')('http-server');

// const ObjectGetRequest = require('./object-get-request');
const ObjectHeadRequest = require('./object-head-request');
// const ObjectOptionsRequest = require('./object-options-request');
const ObjectPutRequest = require('./object-put-request');
// const ObjectDeleteRequest = require('./object-delete-request');

class HttpServer {
    constructor() {
        this.port = 80;

        this._server = http.createServer();
        this._server.on('listening', this._handleHttpListening.bind(this));
        this._server.on('close', this._handleHttpClose.bind(this));
        this._server.on('request', this._handleHttpRequest.bind(this));
    }

    start() {
        this._server.listen(this.port);
    }

    _handleHttpListening() {
        let address = this._server.address();
        log('listening on %s:%d', address.address, address.port);
    }

    _handleHttpClose() {
        log('stopped listening');
    }

    async _handleHttpRequest(req, res) {
        log('new request from %s:%d: %s %s', req.socket.remoteAddress, req.socket.remotePort, req.method, req.url);

        if (!this._validateUrl(req.url))
            return this._outputHttpBadRequest('malformed URL', req, res);

        try {
            if (req.method == 'GET')
                await this._handleHttpGetRequest(req, res);
            else if (req.method == 'HEAD')
                await this._handleHttpHeadRequest(req, res);
            else if (req.method == 'OPTIONS')
                await this._handleHttpOptionsRequest(req, res);
            else if (req.method == 'PUT')
                await this._handleHttpPutRequest(req, res);
            else if (req.method == 'DELETE')
                await this._handleHttpDeleteRequest(req, res);
            else
                this._outputHttpBadRequest('unsupported method', req, res);
        }

        catch (err) {
            log.error('http request encountered error', err);
            this._outputHttpInternalServerError(req, res);
        }
    }

    async _handleHttpGetRequest(req, res) {
        let objectMeta = await this._getObjectMeta(req.url);
        if (!objectMeta) return this._outputHttpNotFound(req, res);
        let request = new ObjectGetRequest(objectMeta, req, res);
        await request.process();
    }

    async _handleHttpHeadRequest(req, res) {
        let objectMeta = await this._getObjectMeta(req.url);
        if (!objectMeta) return this._outputHttpNotFound(req, res);
        let request = new ObjectHeadRequest(objectMeta, req, res);
        await request.process();
    }

    async _handleHttpOptionsRequest(req, res) {
        let objectMeta = await this._getObjectMeta(req.url);
        if (!objectMeta) return this._outputHttpNotFound(req, res);
        let request = new ObjectHeadRequest(objectMeta, req, res);
        await request.process();
    }

    async _handleHttpPutRequest(req, res) {
        if (!req.headers['content-length'])
            return this._outputHttpBadRequest('missing content-length');

        // fake header?
        // ^^ HUH???

        // ensure file doesn't already exist

        let request = new ObjectPutRequest(req, res);
        await request.process();
    }

    async _handleHttpDeleteRequest(req, res) {
        let objectMeta = await this._getObjectMeta(req.url);
        if (!objectMeta) return this._outputHttpNotFound(req, res);
        let request = new ObjectDeleteRequest(objectMeta, req, res);
        await request.process();
    }

    _outputHttpNotFound(req, res) {
        res.writeHead(404, 'Object Not Found');
        res.end('404');
    }

    _outputHttpBadRequest(message, req, res) {
        res.writeHead(400, 'Bad Request');
        res.end(message);
    }

    _outputHttpInternalServerError(req, res) {
        res.writeHead(500, 'Internal Server Error');
        res.end('500');
    }

    _validateUrl(url) {
        if (url.substr(0, 1) != '/')
            return false;
        if (url.includes('//') || url.includes('/./') || url.includes('/../'))
            return false;
        return true;
    }

    async _getObjectMeta(path) {
        try {
            let objectMeta = await database.getObjectByPath(path.substr(1));
            return objectMeta;
        }
        catch (err) {
            if (err.code == 'NOTFOUND')
                return null;
            else
                throw err;
        }
    }
}

module.exports = HttpServer;