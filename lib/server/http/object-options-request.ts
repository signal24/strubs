import type { StoredObjectRecord } from '../../io/file-object';
import type { HttpRequest, HttpResponse } from './server';
import { applyObjectIdentityHeaders, applySliceHeaders } from './object-response-headers';

const ALLOWED_METHODS = 'GET,HEAD,OPTIONS,PUT,DELETE';

export class ObjectOptionsRequest {
    constructor(
        private readonly _requestId: number,
        private readonly objectRecord: StoredObjectRecord,
        private readonly _request: HttpRequest,
        private readonly response: HttpResponse
    ) {}

    async process(): Promise<void> {
        applyObjectIdentityHeaders(this.response, this.objectRecord);
        applySliceHeaders(this.response, this.objectRecord);

        this.response.setHeader('Allow', ALLOWED_METHODS);
        this.response.setHeader('Access-Control-Allow-Methods', ALLOWED_METHODS);
        this.response.setHeader('Access-Control-Allow-Headers', 'Content-Type, Content-MD5, Content-Length');
        this.response.setHeader('Access-Control-Allow-Origin', '*');

        this.response.writeHead(204);
        this.response.end();
    }
}
