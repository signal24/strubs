const crypto = require('crypto');

const Log = require('../../log');

const Base = require('./base');

class FileObjectWriter extends Base {
    constructor(fileObject) {
        super(fileObject);

        this.md5 = null;
        
        this._sliceWritePromises = {};
        this._commitPromises = null;
        this._isDestroying = false;
        this._md5Hasher = crypto.createHash('md5');
    }

    _configureInternals() {
        super._configureInternals();

        this._rsSourcesBits = 0;
        for (let index = 0; index < this.dataSliceCount; index++)
            this._rsSourcesBits |= (1 << index);
        
        this._rsTargetsBits = 0;
        for (let index = this.dataSliceCount; index < this._totalSliceCount; index++)
            this._rsTargetsBits |= (1 << index);
    }

    async prepare() {
        this._configureInternals();
        this._configureStartState();
        this._openSlices();

        let createPromises = [];
        for (let slice of this._slices)
            createPromises.push(slice.create());

        await Promise.all(createPromises);
    }

    async write(data) {
        this._md5Hasher.update(data);

        while (true) {
            let bytesToWrite = Math.min(data.length, this._chunkSetBufferRemaining);
            data.copy(this._chunkSetBuffer, this._chunkSetBufferPosition, 0, bytesToWrite);

            this._chunkSetBufferPosition += bytesToWrite;
            this._chunkSetBufferRemaining -= bytesToWrite;

            this._dataOffset += bytesToWrite;

            if (this._chunkSetBufferRemaining == 0)
                await this._writeBuffer();
            else if (this._chunkSetBufferPosition >= this._chunkSetNextSliceOffset)
                await this._writeBuffer();

            if (bytesToWrite == data.length)
                break;
            
            data = data.slice(bytesToWrite);
        }
    }

    async finish() {
        if (this._chunkSetBufferPosition > 0) {
            this._chunkSetBuffer.fill(0, this._chunkSetBufferPosition);
            this._chunkSetBufferPosition = this._chunkSetDataSize;
            this._chunkSetBufferRemaining = 0;
            await this._writeBuffer();
        }

        for (let sliceIndex in this._sliceWritePromises)
            if (this._sliceWritePromises[sliceIndex])
                await this._sliceWritePromises[sliceIndex];
        
        this.md5 = this._md5Hasher.digest();
        this._md5Hasher = null;
    }

    async commit() {
        // flush any remaining data to disk & close the slices
        this._commitPromises = [];
        for (let slice of this._slices)
            this._commitPromises.push(slice.close());
        await Promise.all(this._commitPromises);
        this._commitPromises = null;

        // if we've been asked to destroy between the time we were
        // asked to commit and now, we should bail here
        if (this._isDestroying)
            throw new Error('object is being destroyed');

        // move slices into place
        this._commitPromises = [];
        for (let slice of this._slices)
            this._commitPromises.push(slice.commit());
        await Promise.all(this._commitPromises);
        this._commitPromises = null;

        // if we've been asked to destroy between the time we were
        // asked to commit and now, we should bail here
        if (this._isDestroying)
            throw new Error('object is being destroyed');
    }

    async _writeBuffer() {
        // while our next slice offset is less than the current position of our buffer,
        // and while our next slice is still a data slice
        // let's write it to the slice, and increment the next slice index
        while (this._chunkSetNextSliceOffset <= this._chunkSetBufferPosition && this._chunkSetNextSliceIndex <= this.dataSliceCount) {
            let sliceIndexToWrite = this._chunkSetNextSliceIndex - 1;
            let sliceData = this._chunkSetBuffer.slice(this._chunkDataSize * sliceIndexToWrite, this._chunkDataSize * this._chunkSetNextSliceIndex);
            await this._queueSliceWrite(sliceIndexToWrite, sliceData);
            this._chunkSetNextSliceIndex++;
            this._chunkSetNextSliceOffset += this._chunkDataSize;
        }

        // if we've run out of data chunks to write
        if (this._chunkSetNextSliceIndex > this.dataSliceCount) {
            // it's time to compute & write parity
            await this._computeAndWriteParity();
            
            // if we've reached the end chunk set
            if (this._dataOffset == this._nextChunkGroupOffset) {
                this._configureNextChunkGroup();
            }

            // otherwise, just reset our buffer positions
            else {
                this._resetBufferPositions();
            }
        }
    }
    
    async _computeAndWriteParity() {
        await this._computeParity();

        for (let sliceIndex = this.dataSliceCount; sliceIndex < this._totalSliceCount; sliceIndex++) {
            let sliceOffset = this._chunkSetSliceOffsets[sliceIndex];
            let sliceData = this._chunkSetBuffer.slice(sliceOffset, sliceOffset + this._chunkDataSize);
            await this._queueSliceWrite(sliceIndex, sliceData);
        }
    }

    async _queueSliceWrite(sliceIndex, data) {
        // if we still have an active promise from the last write, wait for it to finish
        if (this._sliceWritePromises[sliceIndex]) {
            await this._sliceWritePromises[sliceIndex];
        }

        // if we're destroying, bail out of here
        if (this._isDestroying)
            return;

        // fire off a new write & cache it
        let promise = this._slices[sliceIndex].writeChunk(data);
        this._sliceWritePromises[sliceIndex] = promise;

        // when it's finished, clear the cache
        promise.then(() => {
            this._sliceWritePromises[sliceIndex] = null;
        });
    }

    async destroy() {
        this._isDestroying = true;

        if (this._commitPromises)
            await Promise.all(this._commitPromises);

        let destroyPromises = [];
        this._slices.forEach((slice, sliceIndex) => {
            destroyPromises.push(new Promise(async (resolve, reject) => {
                if (this._sliceWritePromises[sliceIndex])
                    await this._sliceWritePromises[sliceIndex];
                await this._slices[sliceIndex].destroy();
                resolve();
            }));
        });

        await Promise.all(destroyPromises);
    }

    log() {
        this.log = Log('file:' + this.id.toString('hex') + ':writer');
        this.log.apply(this.log, arguments);
    }
}

module.exports = FileObjectWriter;