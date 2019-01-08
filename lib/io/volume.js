// TODO: async/await

const EventEmitter = require('events').EventEmitter;

const diskusage = require('diskusage');

const fs = require('fs');
const fsp = fs.promises;

const Log = require('../log');
const config = require('../config');
const helpers = require('./helpers');

class Volume extends EventEmitter {
    constructor(inConfig) {
        super();

        this.id = inConfig.id;
        this.uuid = inConfig.uuid;

        this.blockPath = null;
        this.fsType = null;
        this.mountPoint = null;
        this.mountOptions = null;
        
        this.isMounted = false;
        this.isVerified = false;
        this.isStarted = false;

        this.isEnabled = inConfig.enabled;
        this.isHealthy = inConfig.healthy;
        this.isReadOnly = inConfig.read_only;

        this.deviceSerial = inConfig.drive_serial; // TODO: update this to device_serial in the data
        this.partitionUuid = inConfig.partition_uuid;

        this.bytesTotal = inConfig.partition_size; // TODO: change these to bytes
        this.bytesUsedData = inConfig.data_size;
        this.bytesUsedParity = inConfig.parity_size || 0; // TODO: add
        this.bytesFree = null;
        this.bytesPending = 0;

        this.log = Log('volume' + this.id);

        this.on('error', err => {
            this.log.error(err);
        });
        
        this.log('initialized');
    }

    start() {
        return new Promise(async (resolve, reject) => {
            try {
                this.log('starting...');

                if (!this.isMounted) {
                    this.log('not mounted.');
                    await this.mount();
                }

                await this.verify();
                await this.updateFreeBytes();
                
                this.log('started with %s of %s available', helpers.formatBytes(this.bytesFree), helpers.formatBytes(this.bytesTotal));
                this.isStarted = true;
                this.emit('started');

                resolve();
            }

            catch (err) {
                this.log.error('error encountered while starting the volume', err);
                this.emit('error', err);
                reject(err);
            }
        });
    }

    stop() {

    }

    mount() {
        return new Promise(async (resolve, reject) => {
            this.mountPoint = '/var/run/strubs/mounts/' + this.uuid;

            try {
                await fsp.access(this.mountPoint);
            }
            catch(err) {
                if (err.code != 'ENOENT')
                    return reject(new Error('unable to check mount directory: ' + err));

                this.log('mount point %s does not exist. creating...', this.mountPoint);
                
                try {
                    await fsp.mkdir(this.mountPoint);
                }
                catch (err) {
                    return reject(new Error('unable to create mount directory: ' + err));
                }

                this.log('mount point created');
            }

            this.log('attempting to mount %s (%s) to %s', this.blockPath, this.fsType, this.mountPoint);

            try {
                await helpers.mount(this.blockPath, this.mountPoint, this.fsType, this.mountOptions || {});
            }
            catch (err) {
                return reject(new Error('unable to mount: ' + err));
            }

            this.isMounted = true;
            this.log('mounted block device %s to %s', this.blockPath, this.mountPoint);

            resolve();
        });
    }

    verify() {
        return new Promise(async (resolve, reject) => {
            this.log('verifying volume...');

            try {
                await fsp.access(this.mountPoint);
            }
            catch (err) {
                return reject(new Error('volume mount point inaccessible: ' + err));
            }

            let data;
            try {
                data = await fsp.readFile(this.mountPoint + '/strubs/.identity');
            }
            catch (err) {
                if (err.code == 'ENOENT')
                    await this.createIdentityFile();
                else
                    return reject(new Error('volume identity file could not be read: ' + err));
            }

            // verify header & footer
            if (data[0] != 0x1F || data[1] != 0xFB || data[2] != 0x01 || data[3] != 0xFB || data[data.length - 2] != 0x19 || data[data.length - 1] != 0xFB)
                return reject(new Error('volume identify file corrupt'));

            // verify version
            if (data[4] != 1)
                return reject(new Error('volume identify file has invalid version'));

            // verify matching instance UUID
            if (data.compare(config.identityBuffer, 0, 16, 5, 21) != 0)
                return reject(new Error('volume is not from this STRUBS instance'));

            // verify matching volume UUID
            let volumeUuidBuf = Buffer.from(this.uuid.replace(/[^0-9a-f]/g, ''), 'hex');
            if (data.compare(volumeUuidBuf, 0, 16, 21, 37) != 0)
                return reject(new Error('volume does not match expected volume UUID'));

            // verify matching volume ID
            if (data[37] != this.id)
                return reject(new Error('volume does not match expected volume ID'));

            // TODO: add volume status support

            // yay!
            this.log('verified volume');
            this.isVerified = true;

            resolve();
        });
    }

    async createIdentityFile() {
        let identityBuf = Buffer.alloc(41);
        
        // header
        identityBuf.writeUInt8(0x1F, 0);
        identityBuf.writeUInt8(0xFB, 1);
        identityBuf.writeUInt8(0x01, 2);
        identityBuf.writeUInt8(0xFB, 3);

        // version
        identityBuf.writeUInt8(1, 4);

        // STRUBS identity
        config.identityBuffer.copy(identityBuf, 5);

        // volume UUID
        Buffer.from(this.uuid.replace(/[^0-9a-f]/g, ''), 'hex').copy(identityBuf, 21);

        // volume ID
        identityBuf.writeUInt8(this.id, 37);

        // volume status (O = okay?)
        identityBuf.write('O', 38, 1);

        // footer
        identityBuf.writeUInt8(0x19, 39);
        identityBuf.writeUInt8(0xFB, 40);

        // okay now write it
        try { await fsp.mkdir(this.mountPoint + '/strubs'); } catch (err) {};
        fs.writeFileSync(this.mountPoint + '/strubs/.identity', identityBuf);
    }

    updateFreeBytes() {
        return new Promise((resolve, reject) => {
            diskusage.check(this.mountPoint, (err, info) => {
                if (err)
                    return reject(err);
                this.bytesFree = info.free;
                resolve();
            });
        });
    }
    
    get isReadable() {
        return this.isStarted && this.isEnabled;
    }

    get isWritable() {
        return this.isStarted && this.isEnabled && this.isHealthy && !this.isReadOnly;
    }

    async createTemporaryFh(fileName) {
        if (!this.isWritable)
            throw new Error('volume is not writable');
        
        let path = this.mountPoint + '/strubs/.tmp/' + fileName;
        let fileHandle = await fsp.open(path, 'wx');
        return fileHandle;
    }

    async commitTemporaryFile(fileName) {
        if (!this.isWritable)
            throw new Error('volume is not writable');
        
        let srcPath = this.mountPoint + '/strubs/.tmp/' + fileName;
        let dstFolder = this.mountPoint + '/strubs/' + fileName.substr(0, 1) + '/' + fileName.substr(1, 1) + '/' + fileName.substr(2, 1) + '/' + fileName.substr(3, 1);
        let dstPath = dstFolder + '/' + fileName;
        
        try {
            await fsp.mkdir(dstFolder, { recursive: true });
        }
        catch (err) {
            if (err.code != 'EEXIST')
                throw err;
        }

        await fsp.rename(srcPath, dstPath);
    }

    async deleteTemporaryFile(fileName) {
        if (!this.isWritable)
            throw new Error('volume is not writable');
        
        let path = this.mountPoint + '/strubs/.tmp/' + fileName;
        await fsp.unlink(path);
    }

    async openCommittedFh(fileName) {
        if (!this.isReadable)
            throw new Error('volume is not readable');
        
        let path = this.mountPoint + '/strubs/' + fileName.substr(0, 1) + '/' + fileName.substr(1, 1) + '/' + fileName.substr(2, 1) + '/' + fileName.substr(3, 1) + '/' + fileName;
        let fileHandle = await fsp.open(path, 'r');
        return fileHandle;
    }

    async deleteCommittedFile(fileName) {
        if (!this.isWritable)
            throw new Error('volume is not writable');
        
        let path = this.mountPoint + '/strubs/' + fileName.substr(0, 1) + '/' + fileName.substr(1, 1) + '/' + fileName.substr(2, 1) + '/' + fileName.substr(3, 1) + '/' + fileName;
        await fsp.unlink(path);
    }
}

module.exports = Volume;


/*let b = Buffer.alloc(41);
        b.writeUInt8(0x1F, 0);
        b.writeUInt8(0xFB, 1);
        b.writeUInt8(0x01, 2);
        b.writeUInt8(0xFB, 3);
        b.writeUInt8(1, 4);
        config.identityBuffer.copy(b, 5);
        Buffer.from(this.uuid.replace(/[^0-9a-f]/g, ''), 'hex').copy(b, 21);
        b.writeUInt8(this.id, 37);
        b.write('O', 38, 1);
        b.writeUInt8(0x19, 39);
        b.writeUInt8(0xFB, 40);
        // console.log(b);
        fs.writeFileSync(this.mountPoint + '/strubs/.identity', b);*/