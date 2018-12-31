class ObjectHeadRequest {
    constructor(objectRecord, request, response) {
        this.objectRecord = objectRecord;
        this.request = request;
        this.response = response;
    }

    async process() {
        this.response.setHeader('X-Object-Id', this.objectRecord._id);

        if (this.objectRecord.container_id)
            this.response.setHeader('X-Container-Id', this.objectRecord.container_id);

        if (this.objectRecord.is_container) {
            this.response.setHeader('X-Is-Container', true);
        }

        else {
            this.response.setHeader('Content-Length', this.objectRecord.size);
            this.response.setHeader('Content-MD5', this.objectRecord.md5.toString('hex'));

            if (this.objectRecord.mime)
                this.response.setHeader('Content-Type', this.objectRecord.mime);

            this.response.setHeader('X-Data-Slice-Count', this.objectRecord.data_slices.length);
            this.response.setHeader('X-Data-Slice-Volumes', this.objectRecord.data_slices.join(','));
            this.response.setHeader('X-Parity-Slice-Count', this.objectRecord.parity_slices.length);
            this.response.setHeader('X-Parity-Slice-Volumes', this.objectRecord.parity_slices.join(','));
            this.response.setHeader('X-Chunk-Size', this.objectRecord.chunk_size);
        }

        this.response.end();
    }
}

module.exports = ObjectHeadRequest;