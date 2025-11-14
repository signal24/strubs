import { type StoredObjectRecord } from '../../io/file-object';
import { fileObjectService, type FileObjectService } from '../../io/file-object/service';
import type { HttpRequest, HttpResponse } from './server';
import { applyObjectIdentityHeaders, applySliceHeaders } from './object-response-headers';

type ObjectDeleteRequestDeps = {
    fileObjectService: FileObjectService;
};

const defaultDeps: ObjectDeleteRequestDeps = {
    fileObjectService
};

export class ObjectDeleteRequest {
    private readonly objectRecord: StoredObjectRecord;
    private readonly response: HttpResponse;
    private readonly deps: ObjectDeleteRequestDeps;

    constructor(
        private readonly _requestId: number,
        objectRecord: StoredObjectRecord,
        private readonly _request: HttpRequest,
        response: HttpResponse,
        deps: ObjectDeleteRequestDeps = defaultDeps
    ) {
        this.objectRecord = objectRecord;
        this.response = response;
        this.deps = deps;
    }

    async process(): Promise<void> {
        try {
            applyObjectIdentityHeaders(this.response, this.objectRecord);
            applySliceHeaders(this.response, this.objectRecord);

            const object = await this.deps.fileObjectService.loadForDelete(this.objectRecord, { requestId: `http-${this._requestId}` });
            await object.delete();

            this.response.writeHead(200);
            this.response.end();
        }

        catch (err) {
            this.response.headersSent || this.response.writeHead(500);
            this.response.write('\n\nSTRUBS encountered an error.');
            this.response.destroy();
            throw err;
        }
    }
}
