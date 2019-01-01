const FileObject = require('../../io/file-object');
const Log = require('../../log');

// TODO: GET and HEAD should import from the same object (or Get from Head?) and have a shared write headers function

class ObjectGetRequest {
    constructor(objectRecord, request, response) {
        this.objectRecord = objectRecord;
        this.request = request;
        this.response = response;
    }

    process() {
        return new Promise(async (resolve, reject) => {
            try {
                this.response.setHeader('X-Object-Id', this.objectRecord.id);

                if (this.objectRecord.container_id)
                    this.response.setHeader('X-Container-Id', this.objectRecord.container_id);

                this.response.setHeader('Content-Length', this.objectRecord.size);
                this.response.setHeader('Content-MD5', this.objectRecord.md5.toString('hex'));

                if (this.objectRecord.mime)
                    this.response.setHeader('Content-Type', this.objectRecord.mime);

                this.response.setHeader('X-Data-Slice-Count', this.objectRecord.data_volumes.length);
                this.response.setHeader('X-Data-Slice-Volumes', this.objectRecord.data_volumes.join(','));
                this.response.setHeader('X-Parity-Slice-Count', this.objectRecord.parity_volumes.length);
                this.response.setHeader('X-Parity-Slice-Volumes', this.objectRecord.parity_volumes.join(','));
                this.response.setHeader('X-Chunk-Size', this.objectRecord.chunk_size);

                let object = new FileObject();
                await object.loadFromRecord(this.objectRecord);
                await object.prepareForRead();

                this.response.writeHead(200);

                object.pipe(this.response);

                object.on('error', err => {
                    object.unpipe();
                    this.response.write('\n\nSTRUBS encountered an error. transfer incomplete.');
                    this.response.end();
                    reject(err);
                });

                object.on('end', () => {
                    this.response.end();
                    resolve();
                });
            }

            catch (err) {
                console.log('caught error');
                this.response.headersSent || this.response.writeHead(500);
                this.response.write('\n\nSTRUBS encountered an error. transfer incomplete.');
                this.response.end();
                reject(err);
            }
        });
    }

    log() {
        this.log = Log('http-server:get-request');
        this.log.apply(this.log, arguments);
    }
}

module.exports = ObjectGetRequest;