const _ = require('lodash');
const mongodb = require('mongodb');

const log = require('./log')('database');
const { createError } = require('./helpers');

// TODO: cache MongoIDs for containers

class Database {
    constructor() {
        this._db = null;
        this._collections = {};
        this._containerCache = [];

        setInterval(this._cleanObjectCache.bind(this), 60000);
    }

    async connect() {
        try {
            log('connecting to database');

            this._client = await mongodb.MongoClient.connect('mongodb://localhost:27017/admin', {
                auth: {
                    user: 'strubs',
                    password: 'XNWd36eT78QxePGNPL6PysM3X8EWY2Em'
                },
                useNewUrlParser: true
            });
            this._db = this._client.db('strubs');

            this._collections.volumes = this._db.collection('volumes');
            this._collections.content = this._db.collection('content');

            log('connected');
        }
        catch (err) {
            throw createError('DBFAIL', 'failed to connect to database', err);
        }
    }

    getVolumes() {
        return new Promise((resolve, reject) => {
            let result = [];
            
            this._collections.volumes.find({})
            .on('data', item => result.push(item))
            .on('end', () => resolve(result));
        });
    }

    async createObjectRecord(object) {
        object._id = new mongodb.ObjectID(object.id);
        delete object.id;
        object.containerId = object.containerId ? new mongodb.ObjectID(object.containerId) : null;
        await this._collections.content.insertOne(object);
    }

    async getObjectById(id) {
        let object = await this._collections.content.findOne({
            _id: new mongodb.ObjectID(id)
        });

        if (!object)
            throw createError('NOTFOUND', 'object not found');

        object.id = object._id.toHexString();
        delete object._id;

        if (object.containerId)
            object.containerId = object.containerId.toHexString();

        return object;
    }

    async getObjectByPath(path) {
        let components = path.split('/');
        let objectName = components.pop();

        let containerId = null;

        if (components.length)
            containerId = await this.getContainer(components);
        
        let object = await this._collections.content.findOne({
            containerId: containerId ? new mongodb.ObjectID(containerId) : null,
            name: objectName
        });

        if (!object)
            throw createError('NOTFOUND', 'object not found');
        
        object.id = object._id.toHexString();
        delete object._id;

        if (object.containerId)
            object.containerId = object.containerId.toHexString();

        if (object.isContainer)
            this._cacheContainer(object);

        return object;
    }

    async getContainer(path, shouldCreateIfNotFound) {
        let components = typeof path == 'string' ? path.split('/') : path;

        let containerId = null;
        let shouldSkipLookup = false;

        while (components.length) {
            let name = components.shift();

            let cachedId = this._getCachedObjectId(name, containerId || null);
            if (cachedId) {
                containerId = cachedId;
                continue;
            }

            let object = null;

            if (!shouldSkipLookup) {
                object = await this._collections.content.findOne({
                    containerId: containerId ? new mongodb.ObjectID(containerId) : null,
                    name: name
                });
            }

            if (!object) {
                if (!shouldCreateIfNotFound)
                    throw createError('NOTFOUND', 'object not found');

                shouldSkipLookup = true;

                object = {
                    containerId: containerId ? new mongodb.ObjectID(containerId) : null,
                    name: name,
                    isContainer: true
                };
                await this._collections.content.insertOne(object);

                object.id = object._id.toHexString();
                delete object._id;

                object.containerId = containerId;
            }

            else if (!object.isContainer) {
                throw createError('ENOTCONTAINER', 'object is not a container');
            }

            else {
                object.id = object._id.toHexString();
                delete object._id;

                object.containerId = containerId;
            }

            this._cacheContainer(object);

            containerId = object.id;
        }

        return containerId;
    }

    async getOrCreateContainer(path) {
        return await this.getContainer(path, true);
    }

    _cacheContainer(object) {
        this._containerCache.push({
            id: object.id,
            containerId: object.containerId,
            name: object.name,
            lastUsed: Date.now()
        });
    }

    _getCachedObjectId(name, containerId) {
        let entry = _.find(this._containerCache, {
            containerId: containerId || null,
            name: name
        });

        if (entry) {
            entry.lastUsed = Date.now();
            return entry.id;
        }

        return null;
    }

    _cleanObjectCache() {
        log('clean object cache!');
    }
}

module.exports = new Database();