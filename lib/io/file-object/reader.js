const Log = require('../../log');

const Base = require('./base');

class FileObjectReader extends Base {
    constructor(fileObject) {
        super(fileObject);
        
        this._sliceReadPromises = {};
        this._sliceReadaheadBuffers = {};

        this._hasReadCompletely = false;
        this._currentSliceIndex = 0;
        this._unavailableSlices = [];
    }

    _configureReedSolomon() {
        // this._rsSourcesBits = 0;
        // for (let index = 0; index < this.dataSliceCount; index++)
        //     this._rsSourcesBits |= (1 << index);
        
        // this._rsTargetsBits = 0;
        // for (let index = this.dataSliceCount; index < this._totalSliceCount; index++)
        //     this._rsTargetsBits |= (1 << index);
    }

    async prepare() {
        this._configureInternals();
        this._configureStartState();
        this._instantiateSlices();

        // TODO: only open data slices until parity slices are needed
        let openPromises = [];
        for (let slice of this._slices)
            openPromises.push(slice.open());

        await Promise.all(openPromises);
    }

    async readChunk() {
        if (this._hasReadCompletely)
            return null;
        
        let data = await this._slices[this._currentSliceIndex].readChunk(this._chunkDataSize);
        
        this._currentSliceIndex++;
        if (this._currentSliceIndex == this.dataSliceCount)
            this._currentSliceIndex = 0;
        
        this._dataOffset += this._chunkDataSize;
        
        if (this._dataOffset >= this.size) {
            if (this._dataOffset > this.size)
                data = data.slice(0, this._chunkDataSize - (this._dataOffset - this.size));
            this._completeRead();
        }
        else if (this._dataOffset == this._nextChunkGroupOffset)
            this._configureNextChunkGroup();
        
        return data;
    }

    async _completeRead() {
        this.log('read complete');
        this._hasReadCompletely = true;

        let closePromises = [];
        for (let slice of this._slices)
            closePromises.push(slice.close());

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