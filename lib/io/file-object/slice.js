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
        this._fileName = fileObject.id + '.' + sliceIndex;
        this._size = null;
        this._mode = null;
        this._cursorOffset = 0;
        this._isPerformingIO = false;
        this._isCommitted = false;
    }

    async create() {
        // set up a write buffer
        let writeBuf = this._writeBuf = Buffer.allocUnsafe(this.fileObject.chunkSize);

        // starting size
        this._size = 48;
        this._cursorOffset = 48;

        this._isPerformingIO = true;

        this._outputFh = await this._volume.createTemporaryFh(this._fileName);

        // write the file header data to the write buffer
        /* 00-03 */ writeBuf.write('\x01\xfb\x02\xfb', 0); // magic header
        /* 04-04 */ writeBuf.writeUInt8(1, 4); // version
        /* 05-06 */ writeBuf.writeUInt16LE(48, 5); // header length
        /* 07-22 */ ; // header checksum (will populate after its computed)
        /* 23-34 */ this.fileObject.idBuf.copy(writeBuf, 23, 0, 12); // file ID
        /* 35-39 */ writeBuf.writeIntLE(this.fileObject.size, 35, 5); // file size
        /* 40-40 */ writeBuf.writeUInt8(this.fileObject.dataSliceCount, 40); // data slice count
        /* 41-41 */ writeBuf.writeUInt8(this.fileObject.paritySliceCount, 41); // parity slice count
        /* 42-42 */ writeBuf.writeUInt8(this.sliceIndex, 42); // slice index
        /* 43-45 */ writeBuf.writeIntLE(this.fileObject.chunkSize, 43, 3); // chunk size
        /* 46-47 */ writeBuf.fill(0, 46); // end padding to make the header length a multiple of 8

        // compute header checksum
        await cryptoAsync.hash('md5', writeBuf, 23, 25, writeBuf, 7);

        // TODO: move into I/O class to handle prioritization
        await this._outputFh.write(writeBuf, 0, 48);

        this._isPerformingIO = false;

        // TODO: have the slices hold the bytes per volume on create, until committed (???)

        // set the mode
        this._mode = MODE_WRITE;
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
        await cryptoAsync.hash('md5', writeBuf, 16, dataLen, writeBuf, 0);
        await this._outputFh.write(writeBuf, 0, chunkLen);

        this._size += chunkLen;

        this._isPerformingIO = false;
    }

    isAvailable() {
        return this._volume.isReadable;
    }

    async open() {
        // set up a write buffer
        this._readBuf = Buffer.allocUnsafe(this.fileObject.chunkSize);
        this._hashBuf = Buffer.allocUnsafe(16);

        this._isPerformingIO = true;

        this._inputFh = await this._volume.openCommittedFh(this._fileName);

        await this._inputFh.read(this._readBuf, 0, 48);
        this._cursorOffset = 48;
        // TODO: verify header

        this._isPerformingIO = false;

        // set the mode
        this._mode = MODE_READ;
        this._isCommitted = true;
    }

    seekToHead() {
        this._cursorOffset = 48;
    }

    async readChunk(chunkSize) {
        if (this._isPerformingIO)
            throw new Error('slice already reading');
        if (this._mode != MODE_READ)
            throw new Error('slice not opened for reading');

        this._isPerformingIO = true;

        let readBuf = this._readBuf;
        let hashBuf = this._hashBuf;
        let readLen = chunkSize + 16;

        await this._inputFh.read(readBuf, 0, readLen, this._cursorOffset);

        await cryptoAsync.hash('md5', readBuf, 16, chunkSize, hashBuf, 0);
        if (!readBuf.slice(0, 16).equals(hashBuf))
            this.throwChecksumError();

        this._cursorOffset += readLen;
        this._isPerformingIO = false;

        return readBuf.slice(16, readLen);
    }

    skipChunk(chunkSize) {
        this._cursorOffset += chunkSize + 16;
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
            await this._inputFh.close();
            this._inputFh = null;
        }
        else {
            throw new Error('slice is not open');
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

    // TODO: enable async delete if object isn't committed to deal with "failed to create slices", etc
    async delete() {
        if (this._isPerformingIO)
            throw new Error('slice busy');

        if (this._mode == MODE_WRITE) {
            await this._outputFh.close();
            this._outputFh = null;
        }
        else if (this._mode == MODE_READ) {
            // TODO: should this do anything?
        }

        this._isPerformingIO = true;
        if (this._isCommitted)
            await this._volume.deleteCommittedFile(this._fileName);
        else
            await this._volume.deleteTemporaryFile(this._fileName);
        this._isPerformingIO = false;

        this._mode = null;
    }

    throwChecksumError() {
        let err = new Error('checksum mismatch at ' + this._cursorOffset);
        err.code = 'ECHECKSUM';
        throw err;
    }
}

module.exports = Slice;