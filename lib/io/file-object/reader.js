const Log = require('../../log');

const Base = require('./base');

class FileObjectReader extends Base {
    constructor(fileObject) {
        super(fileObject);

        this._sliceReadPromises = {};
        this._sliceReadaheadBuffers = {};

        this._hasReadSegment = false;
        this._currentSliceIndex = 0;
        this._activeSliceIdxs = [];

        this._unavailableSliceIdxs = [];
        this._paritySliceIdxs = [];
        this._mustReconstructData = false;

        this._startOffset = null;
        this._endOffset = null;
    }

    async prepare() {
        this._configureInternals();
        this._configureStartState();
        this._instantiateSlices();

        this._startOffset = 0;
        this._endOffset = this.size;

        for (let index = 0; index < this.dataSliceCount; index++) {
            if (this._slices[index].isAvailable()) {
                this._activeSliceIdxs.push(index);
            } else {
                this._mustReconstructData = true;
                this._unavailableSliceIdxs.push(index);
            }
        }

        if (this._mustReconstructData) {
            this._prepareReconstruction();
        }

        let openPromises = [];
        this._activeSliceIdxs.forEach(index => {
            const slice = this._slices[index];
            openPromises.push(slice.open());
        });
        await Promise.all(openPromises);
    }

    _prepareReconstruction() {
        for (let index = this.dataSliceCount; index < this._totalSliceCount; index++) {
            if (this._slices[index].isAvailable()) {
                this._activeSliceIdxs.push(index);
                this._paritySliceIdxs.push(index);
            }

            if (this._slices.length == this.dataSliceCount) {
                break;
            }
        }

        if (this._slices.length < this.dataSliceCount) {
            throw new Error('insufficient slices available to reconstruct file');
        }

        this._rsSourcesBits = 0;
        for (let index of this._activeSliceIdxs)
            this._rsSourcesBits |= (1 << index);

        this._rsTargetsBits = 0;
        for (let index of this._unavailableSliceIdxs)
            this._rsTargetsBits |= (1 << index);
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
    }

    _alignSlicesToOffset(targetOffset) {
        // TODO: find a less lazy way to do this

        if (this._dataOffset !== 0) {
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
        // TODO: fork here based on _mustReconstructData

        if (this._hasReadSegment) return null;

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
            if (this._dataOffset > this._endOffset) {
                const overageByteCount = this._dataOffset - this._endOffset;
                data = data.slice(0, data.length - overageByteCount);
            }

            this._hasReadSegment = true;
        }

        else if (this._dataOffset == this._nextChunkGroupOffset)
            this._configureNextChunkGroup();

        return data;
    }

    get hasReachedEOF() {
        return this._dataOffset >= this.size;
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
        this._activeSliceIdxs.forEach(index => {
            const slice = this._slices[index];
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