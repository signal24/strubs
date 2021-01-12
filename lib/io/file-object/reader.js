const Log = require('../../log');

const Base = require('./base');

class FileObjectReader extends Base {
    constructor(fileObject) {
        super(fileObject);

        this._sliceReadPromises = {};
        this._sliceReadaheadBuffers = {};

        this._hasReadSegment = false;
        this._shouldEndAtEOF = false;
        this._nextReadPromiseResolver = null;
        this._currentSliceIndex = 0;
        this._unavailableSlices = [];

        this._startOffset = null;
        this._endOffset = null;
    }

    _configureReedSolomon() {
        // this._rsSourcesBits = 0;
        // for (let index = 0; index < this.dataSliceCount; index++)
        //     this._rsSourcesBits |= (1 << index);

        // this._rsTargetsBits = 0;
        // for (let index = this.dataSliceCount; index < this._totalSliceCount; index++)
        //     this._rsTargetsBits |= (1 << index);
    }

    async prepare(options) {
        this._configureInternals();
        this._configureStartState();
        this._instantiateSlices();

        this._startOffset = 0;
        this._endOffset = this.size;

        if (options.shouldEndAtEOF)
            this._shouldEndAtEOF = true;

        // TODO: only open data slices until parity slices are needed
        let openPromises = [];
        this._slices.forEach((slice, index) => {
            if (index > 2) return; // TODO: make dynamic
            openPromises.push(slice.open());
        });
        await Promise.all(openPromises);
    }

    setReadRange(start, end) {
        this._hasReadSegment = false;
        this._startOffset = start;
        this._endOffset = end;

        let targetOffset = null;

        if (this._startOffset < this._standardChunkSetOffset) {
            targetOffset = Math.floor(this._startOffset / this._startChunkDataSize) * this._startChunkDataSize;
        }
        else if (this._startOffset < this._endChunkSetDataOffset) {
            let standardChunkSetOffset = this._startOffset - this._standardChunkSetOffset;
            let adjustedStandardChunkSetOffset = Math.floor(standardChunkSetOffset / this._standardChunkDataSize) * this._standardChunkDataSize;
            targetOffset = this._standardChunkSetOffset + adjustedStandardChunkSetOffset;
        }
        else {
            let endChunkSetOffset = this._startOffset - this._endChunkSetDataOffset;
            let adjustedEndChunkSetOffset = Math.floor(endChunkSetOffset / this._endChunkDataSize) * this._endChunkDataSize;
            targetOffset = this._endChunkSetDataOffset + adjustedEndChunkSetOffset;
        }

        this._alignSlicesToOffset(targetOffset);

        if (this._nextReadPromiseResolver) {
            let promiseResolver = this._nextReadPromiseResolver;
            this.readChunk().then(promiseResolver);
            this._nextReadPromiseResolver = null;
        }
    }

    _alignSlicesToOffset(targetOffset) {
        // TODO: find a less lazy way to do this

        if (this._dataOffset != 0) {
            for (let slice of this._slices)
                slice.seekToHead();

            this._currentSliceIndex = 0;
            this._dataOffset = 0;
            this._configureStartState();
        }

        while (this._dataOffset < targetOffset) {
            this._slices[this._currentSliceIndex].skipChunk(this._chunkDataSize);

            this._currentSliceIndex++;
            if (this._currentSliceIndex == this.dataSliceCount)
                this._currentSliceIndex = 0;

            this._dataOffset += this._chunkDataSize;

            if (this._dataOffset == this._nextChunkGroupOffset)
                this._configureNextChunkGroup();
        }

        if (this._dataOffset != targetOffset) {
            throw new Error('offset must be a chunk boundary');
        }
    }

    async readChunk() {
        if (this._hasReadSegment) {
            if (this._shouldEndAtEOF) return null;
            return new Promise((resolve, reject) => {
                this._nextReadPromiseResolver = resolve;
            });
        }

        let data = await this._slices[this._currentSliceIndex].readChunk(this._chunkDataSize);

        this._currentSliceIndex++;
        if (this._currentSliceIndex == this.dataSliceCount)
            this._currentSliceIndex = 0;

        if (this._startOffset > this._dataOffset) {
            if (this._startOffset >= this._dataOffset + this._chunkDataSize) {
                throw new Error('reader not properly aligned to start chunk');
            }
            data = data.slice(this._startOffset - this._dataOffset);
        }

        this._dataOffset += this._chunkDataSize;

        if (this._dataOffset >= this._endOffset) {
            if (this._dataOffset > this._endOffset)
                data = data.slice(0, data.length - (this._dataOffset - this._endOffset));
            this._hasReadSegment = true;
        }
        else if (this._dataOffset == this._nextChunkGroupOffset)
            this._configureNextChunkGroup();

        return data;
    }

    async close() {
        this.log('closing slices');
        this._hasReadSegment = true;

        // TODO: this should wait until all read promises return instead of an arbitrary delay
        setTimeout(() => {
            this._closeSlices();
        }, 1000);
    }

    async _closeSlices() {
        let closePromises = [];
        this._slices.forEach((slice, index) => {
            if (index > 2) return; // TODO: make dynamic
            closePromises.push(slice.close());
        });

        try {
            await Promise.all(closePromises);
        }
        catch (err) {
            this.log.error('slice encountered error during close', err);
        }
    }

    log() {
        this.log = Log('file:' + this.fileObject.id + ':reader');
        this.log.apply(this.log, arguments);
    }
}

module.exports = FileObjectReader;