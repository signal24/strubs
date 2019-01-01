// TODO: make sure data is actually flushed to disk on _finish

const { Duplex } = require('stream');

const database = require('../database');
const Log = require('../log');

const ioHelpers = require('./helpers');
const planner = require('./planner');

const FileObjectWriter = require('./file-object/writer');
const FileObjectReader = require('./file-object/reader');

const MODE_WRITE = 1;
const MODE_READ = 2;

class FileObject extends Duplex {
    constructor() {
        super();

        this.id = null;
        this.idBuf = null;
        this.containerId = null;
        this.name = null;
        this.size = null;
        this.mime = null;
        this.md5 = null;
        this.chunkSize = null;
        this.dataSliceCount = null;
        this.dataSliceVolumeIds = null;
        this.paritySliceCount = null;
        this.paritySliceVolumeIds = null;
        
        this._mode = null;
        this._writer = null;
        this._reader = null;

        this._isPersisted = false;
    }

    async createWithSize(size) {
        // generate an ID and store the size
        this.idBuf = ioHelpers.generateObjectId();
        this.id = this.idBuf.toString('hex');
        this.size = size;

        // chunk size, data & parity slice count, target volumes
        let plan = await planner.generatePlan(size);

        // copy over to object
        this.chunkSize = plan.chunkSize;
        this.dataSliceCount = plan.dataSliceCount;
        this.dataSliceVolumeIds = plan.dataVolumes;
        this.paritySliceCount = plan.paritySliceCount;
        this.paritySliceVolumeIds = plan.parityVolumes;
        
        // logging
        this.log(
            // TODO: add inflated size
            'preparing to store %d byte object inflated to ??? bytes, stored in %d byte chunks; data on volumes %s; parity on volumes %s',
            this.size,
            this.chunkSize,
            this.dataSliceVolumeIds.join(', '),
            this.paritySliceVolumeIds.join(', ')
        );

        // create a writer
        this._writer = new FileObjectWriter(this);

        // create slices
        try {
            await this._writer.prepare();
        }
        catch (err) {
            this.log.error('failed to create slices:', err);
            await this._writer.destroy();
            throw new Error('failed to create file object');
        }

        // note that we are now configured and can accept data
        this._mode = MODE_WRITE;

        // logging
        this.log('ready to store');
    }

    async commit() {
        if (this._mode != MODE_WRITE)
            return callback(new Error('file object is not in a writable state'));
        
        await this._writer.commit();

        let dbObject = {
            id: this.id,
            container_id: this.containerId,
            is_file: true,
            name: this.name,
            size: this.size,
            md5: this.md5,
            mime: this.mime,
            chunk_size: this.chunkSize,
            data_volumes: this.dataSliceVolumeIds,
            parity_volumes: this.paritySliceVolumeIds
        };

        if (!dbObject.mime)
            delete dbObject.mime;

        await database.createObjectRecord(dbObject);

        this._isPersisted = true;
        this._mode = null;
        
        this.log('committed');
    }

    async loadFromRecord(record) {
        this._isPersisted = true;

        // copy all the data from the record to the local object
        this.id = record.id;
        this.idBuf = Buffer.from(this.id, 'hex');
        this.size = record.size;
        this.containerId = record.container_id || null;
        this.name = record.name;
        this.size = record.size;
        this.md5 = record.md5;
        this.mime = record.mime || null;
        this.chunkSize = record.chunk_size;
        this.dataSliceVolumeIds = record.data_volumes;
        this.dataSliceCount = this.dataSliceVolumeIds.length;
        this.paritySliceVolumeIds = record.parity_volumes;
        this.paritySliceCount = this.paritySliceVolumeIds.length;

        // logging
        this.log(
            // TODO: add inflated size
            'loaded %d byte object inflated to ??? bytes, stored in %d byte chunks; data on volumes %s; parity on volumes %s',
            this.size,
            this.chunkSize,
            this.dataSliceVolumeIds.join(', '),
            this.paritySliceVolumeIds.join(', ')
        );
    }

    async prepareForRead() {
        // create a reader
        this._reader = new FileObjectReader(this);

        // open slices
        try {
            await this._reader.prepare();
        }
        catch (err) {
            this.log.error('failed to open slices:', err);
            throw new Error('failed to open file object');
        }

        // note that we are now configured and can deliver data
        this._mode = MODE_READ;

        // logging
        this.log('ready to read');
    }

    async destroy() {
        this.log('destroying object');

        if (this._mode == MODE_WRITE) {
            await this._writer.destroy();
        }
        else if (this._mode == MODE_READ) {
            
        }

        if (this._isPersisted) {
            // TODO: remove from DB if it exists
        }

        this._mode = null;
        this._isPersisted = false;
    }

    async _write(data, encoding, callback) {
        if (this._mode != MODE_WRITE)
            return callback(new Error('file object is not in a writable state'));

        try {
            await this._writer.write(data);
        }
        catch (err) {
            return callback(err);
        }

        callback();
    }

    async _final(callback) {
        if (this._mode != MODE_WRITE)
            return callback(new Error('file object is not in a writable state'));
        
        try {
            await this._writer.finish();
        }
        catch (err) {
            return callback(err);
        }

        this.md5 = this._writer.md5;
        
        callback();
    }

    async _read() {
        if (this._mode != MODE_READ)
            return this.emit('error', new Error('file object is not in a readable state'));
        
        try {
            let buffer = await this._reader.readChunk();
            this.push(buffer);
        }
        catch (err) {
            return this.emit('error', err);
        }
    }

    async _destroy(err, callback) {
        console.trace('stream destroy', err);
        callback();
    }

    // TODO: add a thing to close the slices when the reading is complete

    log() {
        this.log = Log('file:' + this.id);
        this.log.apply(this.log, arguments);
    }
}

module.exports = FileObject;