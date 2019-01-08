class ObjectHeadRequest {
    constructor(requestId, objectRecord, request, response) {
        this.requestId = requestId;
        this.objectRecord = objectRecord;
        this.request = request;
        this.response = response;
    }

    async process() {
        this.response.setHeader('X-Object-Id', this.objectRecord.id);

        if (this.objectRecord.containerId)
            this.response.setHeader('X-Container-Id', this.objectRecord.containerId);

        if (this.objectRecord.isContainer) {
            this.response.setHeader('X-Is-Container', true);
        }

        else {
            this.response.setHeader('Content-Length', this.objectRecord.size);
            this.response.setHeader('Content-MD5', this.objectRecord.md5.toString('hex'));

            if (this.objectRecord.mime)
                this.response.setHeader('Content-Type', this.objectRecord.mime);

            this.response.setHeader('X-Data-Slice-Count', this.objectRecord.dataVolumes.length);
            this.response.setHeader('X-Data-Slice-Volumes', this.objectRecord.dataVolumes.join(','));
            this.response.setHeader('X-Parity-Slice-Count', this.objectRecord.parityVolumes.length);
            this.response.setHeader('X-Parity-Slice-Volumes', this.objectRecord.parityVolumes.join(','));
            this.response.setHeader('X-Chunk-Size', this.objectRecord.chunkSize);
        }

        this.response.end();
    }
}

module.exports = ObjectHeadRequest;