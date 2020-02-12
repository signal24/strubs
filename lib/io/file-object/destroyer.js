const Log = require('../../log');

const Base = require('./base');

class FileObjectDestroyer extends Base {
    constructor(fileObject) {
        super(fileObject);

        this._instantiateSlices();
    }

    // TODO: standardize destroy vs delete
    async destroy() {
        let destroyPromises = [];
        this._slices.forEach((slice, index) => {
            // TODO: this shouldn't be setting that
            slice._isCommitted = true;
            destroyPromises.push(slice.delete());
        });

        try {
            await Promise.all(destroyPromises);
        }
        catch (err) {
            // TODO: wtf? do better
            this.log();
            this.log.error('slice encountered error during destroy', err);
        }
    }

    log() {
        this.log = Log('file:' + this.fileObject.id + ':destroyer');
        arguments.length && this.log.apply(this.log, arguments);
    }
}

module.exports = FileObjectDestroyer;