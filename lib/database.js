const _ = require('lodash');
const mongodb = require('mongodb');

const log = require('./log')('database');
const { createError } = require('./helpers');

class Database {
    constructor() {
        this._db = null;
        this._collections = {};
        this._objectCache = [];

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
            this._collections.objects = this._db.collection('objects');

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

    async getObjectByPath(path) {
        let components = path.split('/');
        let objectName = components.pop();

        let containerId = null;

        if (components.length)
            containerId = await this.getContainer(components);
        
        let object = await this._collections.objects.findOne({
            container_id: containerId || null,
            name: objectName
        });

        if (!object)
            throw createError('NOTFOUND', 'object not found');

        if (object.is_container)
            this._cacheObject(object);

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
                object = await this._collections.objects.findOne({
                    container_id: containerId || null,
                    name: name
                });
            }

            if (!object) {
                if (!shouldCreateIfNotFound)
                    throw createError('NOTFOUND', 'object not found');

                shouldSkipLookup = true;

                object = {
                    container_id: containerId,
                    name: name,
                    is_container: true
                };
                
                let result = await this._collections.objects.insertOne(object);
            }

            this._cacheObject(object);

            containerId = object._id;
        }

        return containerId;
    }

    async getOrCreateContainer(path) {
        return await this.getContainer(path, true);
    }

    async createObjectRecord(object) {
        object._id = new mongodb.ObjectID(object._id);
        await this._collections.objects.insertOne(object);
    }

    _cacheObject(object) {
        this._objectCache.push({
            id: object._id,
            containerId: object.container_id,
            name: object.name,
            lastUsed: Date.now()
        });
    }

    _getCachedObjectId(name, containerId) {
        let entry = _.find(this._objectCache, {
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