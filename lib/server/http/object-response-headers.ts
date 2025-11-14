import type { StoredObjectRecord } from '../../io/file-object';
import type { HttpResponse } from './server';

export function applyObjectIdentityHeaders(res: HttpResponse, record: StoredObjectRecord): void {
    res.setHeader('X-Object-Id', record.id);

    if (record.containerId)
        res.setHeader('X-Container-Id', record.containerId);
}

export function applyFileMetadataHeaders(res: HttpResponse, record: StoredObjectRecord): void {
    if (record.md5)
        res.setHeader('Content-MD5', record.md5.toString('hex'));

    if (record.mime)
        res.setHeader('Content-Type', record.mime);
}

export function applySliceHeaders(res: HttpResponse, record: StoredObjectRecord): void {
    res.setHeader('X-Data-Slice-Count', record.dataVolumes.length);
    res.setHeader('X-Data-Slice-Volumes', record.dataVolumes.join(','));
    res.setHeader('X-Parity-Slice-Count', record.parityVolumes.length);
    res.setHeader('X-Parity-Slice-Volumes', record.parityVolumes.join(','));
    res.setHeader('X-Chunk-Size', record.chunkSize);
}
