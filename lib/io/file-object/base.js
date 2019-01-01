const ReedSolomon = require('../../async-bridges/reed-solomon');

const Slice = require('./slice');

class Base {
    constructor(fileObject) {
        this.fileObject = fileObject;

        this.size = this.fileObject.size;
        this.chunkSize = this.fileObject.chunkSize;
        this.dataSliceCount = this.fileObject.dataSliceCount;
        this.dataSliceVolumeIds = this.fileObject.dataSliceVolumeIds;
        this.paritySliceCount = this.fileObject.paritySliceCount;
        this.paritySliceVolumeIds = this.fileObject.paritySliceVolumeIds;

        this._slices = [];
        this._totalSliceCount = null;
        this._rsSourcesBits = null;
        this._rsTargetsBits = null;
        this._rsContext = null;
        this._dataOffset = null;
        this._chunkSetBuffer = null;
        this._chunkSetBufferPosition = null;
        this._chunkSetBufferRemaining = null;
        this._startChunkDataSize = null;
        this._standardChunkSetOffset = null;
        this._standardChunkDataSize = null;
        this._endChunkSetDataOffset = null;
        this._endChunkDataSize = null;
        this._nextChunkGroupOffset = null;
        this._chunkDataSize = null;
        this._chunkSetDataSize = null;
        this._chunkSetParityOffset = null;
        this._chunkSetParitySize = null;
        this._chunkSetSliceOffsets = null;
        this._chunkSetNextSliceIndex = null;
        this._chunkSetNextSliceOffset = null;
    }

    _configureInternals() {
        this._totalSliceCount = this.dataSliceCount + this.paritySliceCount;

        this._rsContext = ReedSolomon.create(this.dataSliceCount, this.paritySliceCount);

        this._dataOffset = 0;

        // how many bytes are useable in a data chunk?
        this._standardChunkDataSize = this.chunkSize - constants.CHUNK_HEADER_SIZE /* MD5 */;

        // prep a buffer to be used 
        let maxBufferSize = this._standardChunkDataSize * this._totalSliceCount;
        this._chunkSetBuffer = Buffer.allocUnsafe(maxBufferSize);
        
        // figure out data chunk sizes and offsets
        this._startChunkDataSize = Math.min(this._standardChunkDataSize - constants.FILE_HEADER_SIZE, Math.floor(this.size / this.dataSliceCount));
        this._startChunkDataSize = Math.max(1, this._startChunkDataSize);
        this._startChunkDataSize = Math.ceil(this._startChunkDataSize / 8) * 8;
        this._standardChunkSetOffset = this._startChunkDataSize * this.dataSliceCount;
        let bytesRemaining = Math.max(0, this.size - this._standardChunkSetOffset);
        let bytesRemainingPerSlice = Math.ceil(bytesRemaining / this.dataSliceCount);
        let standardChunkCountPerSlice = Math.floor(bytesRemainingPerSlice / this._standardChunkDataSize);
        let totalBytesInStandardChunks = standardChunkCountPerSlice * this._standardChunkDataSize * this.dataSliceCount;
        let totalBytesBeforeEndChunkSet = this._standardChunkSetOffset + totalBytesInStandardChunks;
        let endChunkSetBytes = this.size - totalBytesBeforeEndChunkSet;
        this._endChunkSetDataOffset = this.size - endChunkSetBytes;
        this._endChunkDataSize = Math.ceil(endChunkSetBytes / this.dataSliceCount);
        this._endChunkDataSize = Math.ceil(this._endChunkDataSize / 8) * 8;
    }

    _configureStartState() {
        this._chunkDataSize = this._startChunkDataSize;
        this._nextChunkGroupOffset = this._standardChunkSetOffset;
        this._configureCommonState();
    }

    _configureMiddleState() {
        this._chunkDataSize = this._standardChunkDataSize;
        this._nextChunkGroupOffset = this._endChunkSetDataOffset;
        this._configureCommonState();
    }

    _configureEndState() {
        this._chunkDataSize = this._endChunkDataSize;
        this._nextChunkGroupOffset = Number.MAX_SAFE_INTEGER;
        this._configureCommonState();
    }

    _configureCommonState() {
        this._chunkSetDataSize = this._chunkDataSize * this.dataSliceCount;
        this._chunkSetParityOffset = this._chunkSetDataSize;
        this._chunkSetParitySize = this._chunkDataSize * this.paritySliceCount;
        
        this._chunkSetSliceOffsets = [];
        for (let index = 0; index < this._totalSliceCount; index++)
            this._chunkSetSliceOffsets.push(index * this._chunkDataSize);
        
        this._resetBufferPositions();
    }

    _resetBufferPositions() {
        this._chunkSetNextSliceIndex = 1;
        this._chunkSetNextSliceOffset = this._chunkSetSliceOffsets[1];

        this._chunkSetBufferPosition = 0;
        this._chunkSetBufferRemaining = this._chunkSetDataSize;
    }

    _configureNextChunkGroup() {
        if (this._dataOffset == this._endChunkSetDataOffset)
            this._configureEndState();
        else if (this._dataOffset == this._standardChunkSetOffset)
            this._configureMiddleState();
        else
            throw new Error('data offset not valid for this operation');
    }

    async _computeParity() {
        await ReedSolomon.encode(
            this._rsContext,
            this._rsSourcesBits,
            this._rsTargetsBits,
            this._chunkSetBuffer,
            0,
            this._chunkSetDataSize,
            this._chunkSetBuffer,
            this._chunkSetParityOffset,
            this._chunkSetParitySize
        );
    }

    async _instantiateSlices() {
        for (let index = 0; index < this._totalSliceCount; index++) {
            let slice = new Slice(this.fileObject, index);
            this._slices.push(slice);
        }
    }
}

module.exports = Base;