const cryptoAsync = require('../../async-bridges/crypto-async');

const ioManager = require('../manager');

const MODE_WRITE = 1;
const MODE_READ = 2;

class Slice {
    constructor(fileObject, sliceIndex) {
        this.fileObject = fileObject;
        this.sliceIndex = sliceIndex;

        if (sliceIndex < this.fileObject.dataSliceCount)
            this._volumeId = this.fileObject.dataSliceVolumeIds[sliceIndex];
        else if (sliceIndex < this.fileObject.dataSliceCount + this.fileObject.paritySliceCount)
            this._volumeId = this.fileObject.paritySliceVolumeIds[sliceIndex - this.fileObject.dataSliceCount];
        else
            throw new Error('invalid slice index');
        
        this._volume = ioManager.getVolume(this._volumeId);
        this._fileName = fileObject.id.toString('hex') + '.' + sliceIndex;
        this._size = null;
        this._mode = null;
        this._isPerformingIO = false;
        this._isCommitted = false;
    }

    async create() {
        this._size = 48;
        this._mode = MODE_WRITE;

        this._isPerformingIO = true;

        this._outputFh = await this._volume.createTemporaryFh(this._fileName);

        let headerBuf = Buffer.allocUnsafe(48);
        /* 00-03 */ headerBuf.write('\x01\xfb\x02\xfb', 0); // magic header
        /* 04-04 */ headerBuf.writeUInt8(1, 4); // version
        /* 05-06 */ headerBuf.writeUInt16LE(48, 5); // header length
        /* 07-22 */ ; // header checksum (will populate after its computed)
        /* 23-34 */ this.fileObject.id.copy(headerBuf, 23, 0, 12); // file ID
        /* 35-39 */ headerBuf.writeIntLE(this.fileObject.size, 35, 5); // file size
        /* 40-40 */ headerBuf.writeUInt8(this.fileObject.dataSliceCount, 40); // data slice count
        /* 41-41 */ headerBuf.writeUInt8(this.fileObject.paritySliceCount, 41); // parity slice count
        /* 42-42 */ headerBuf.writeUInt8(this.sliceIndex, 42); // slice index
        /* 43-45 */ headerBuf.writeIntLE(this.fileObject.chunkSize, 43, 3); // chunk size
        /* 46-47 */ headerBuf.fill(0, 46); // end padding to make the header length a multiple of 8

        // compute header checksum
        await cryptoAsync.hash('MD5', headerBuf, 23, 25, headerBuf, 7);
        
        // TODO: move into I/O class to handle prioritization
        await this._outputFh.write(headerBuf, 0, 48);

        this._isPerformingIO = false;

        // set up a write buffer
        this._writeBuf = Buffer.allocUnsafe(this.fileObject.chunkSize);

        // TODO: have the slices hold the bytes per volume on create, until committed
    }

    async open() {
        this._mode = MODE_READ;

    }

    async writeChunk(data) {
        if (this._isPerformingIO)
            throw new Error('slice already writing');
        if (this._mode != MODE_WRITE)
            throw new Error('slice not opened for writing');

        this._isPerformingIO = true;
        
        let writeBuf = this._writeBuf;
        let dataLen = data.length;
        let chunkLen = data.length + 16;

        data.copy(writeBuf, 16, 0, dataLen);
        await cryptoAsync.hash('MD5', writeBuf, 16, dataLen, writeBuf, 0);
        await this._outputFh.write(writeBuf, 0, chunkLen);

        this._size += chunkLen;

        this._isPerformingIO = false;
    }

    async readChunk() {
        if (this._isPerformingIO)
            throw new Error('slice already reading');
        if (this._mode != MODE_READ)
            throw new Error('slice not opened for reading');
        
        
    }

    async close() {
        if (this._isPerformingIO)
            throw new Error('slice busy');
        
        this._isPerformingIO = true;
        
        if (this._mode == MODE_WRITE) {
            await this._outputFh.sync();
            await this._outputFh.close();
            this._outputFh = null;
        }
        else if (this._mode == MODE_READ) {
            
        }

        this._mode = null;
        this._isPerformingIO = false;
    }

    async commit() {
        if (this._isPerformingIO)
            throw new Error('slice busy');
        
        this._isPerformingIO = true;
        await this._volume.commitTemporaryFile(this._fileName);
        this._volume.bytesFree -= this._size; // TODO: figure out what I want to do here
        this._isPerformingIO = false;

        this._isCommitted = true;
        this._mode = null;
    }

    async destroy() {
        if (this._isPerformingIO)
            throw new Error('slice busy');

        if (this._mode == MODE_WRITE) {
            await this._outputFh.close();
            this._outputFh = null;
        }
        else if (this._mode == MODE_READ) {
            
        }
        
        this._isPerformingIO = true;
        if (this._isCommitted)
            await this._volume.destroyCommittedFile(this._fileName);
        else
            await this._volume.destroyTemporaryFile(this._fileName);
        this._isPerformingIO = false;
        
        this._mode = null;
    }
}

module.exports = Slice;