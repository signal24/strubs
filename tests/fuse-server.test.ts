import { EventEmitter } from 'events';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { StoredObjectRecord } from '../lib/io/file-object';

const FUSE_ERRORS = {
    EISDIR: 1,
    EROFS: 2,
    EPERM: 3,
    EINVAL: 4,
    EREMOTEIO: 5,
    ECONNRESET: 6,
    EIO: 7,
};

const createLoggerMock = vi.fn(() => {
    const logger = vi.fn();
    logger.error = vi.fn();
    return logger;
});

vi.mock('../lib/log', () => ({
    createLogger: createLoggerMock,
}));

const fuseInstances: Array<{
    mountPath: string;
    handlers: Record<string, (...args: unknown[]) => void>;
    opts: Record<string, unknown>;
    mount: ReturnType<typeof vi.fn>;
}> = [];

vi.mock('fuse-native', () => {
    class FakeFuse {
        static EISDIR = FUSE_ERRORS.EISDIR;
        static EROFS = FUSE_ERRORS.EROFS;
        static EPERM = FUSE_ERRORS.EPERM;
        static EINVAL = FUSE_ERRORS.EINVAL;
        static EREMOTEIO = FUSE_ERRORS.EREMOTEIO;
        static ECONNRESET = FUSE_ERRORS.ECONNRESET;
        static EIO = FUSE_ERRORS.EIO;

        mountPath: string;
        handlers: Record<string, (...args: unknown[]) => void>;
        opts: Record<string, unknown>;
        mount = vi.fn((cb?: (err?: Error | null) => void) => cb?.(null));

        constructor(mountPath: string, handlers: Record<string, (...args: unknown[]) => void>, opts: Record<string, unknown>) {
            this.mountPath = mountPath;
            this.handlers = handlers;
            this.opts = opts;
            fuseInstances.push(this);
        }
    }

    return {
        default: FakeFuse,
    };
});

const databaseMock = {
    getObjectByPath: vi.fn(),
    getObjectById: vi.fn(),
    getObjectsInContainerPath: vi.fn(),
    getTimestampFromId: vi.fn(),
};

vi.mock('../lib/database', () => ({
    database: databaseMock,
}));

class FileObjectStub extends EventEmitter {
    size = 1024;
    acquireIOLock = vi.fn().mockResolvedValue(undefined);
    releaseIOLock = vi.fn().mockResolvedValue(undefined);
    setReadRange = vi.fn();
    loadFromRecord = vi.fn().mockImplementation(async (record: StoredObjectRecord) => {
        this.size = record.size;
    });
    prepareForRead = vi.fn().mockResolvedValue(undefined);
    close = vi.fn().mockResolvedValue(undefined);
    requestId: string | null = null;

    setRequestId(requestId: string | null): void {
        this.requestId = requestId ?? null;
    }

    getRequestId(): string | null {
        return this.requestId;
    }

    getLoggerPrefix(): string {
        const idPart = 'object';
        return this.requestId ? `${this.requestId}:file:${idPart}` : `file:${idPart}`;
    }
}

const fileObjectInstances: FileObjectStub[] = [];
const fileObjectServiceMock = {
    openForRead: vi.fn(async (record: StoredObjectRecord, options?: { requestId?: string | number }) => {
        const instance = new FileObjectStub();
        instance.size = record.size;
        if (options && options.requestId !== undefined)
            instance.setRequestId(options.requestId);
        fileObjectInstances.push(instance);
        return instance;
    }),
    load: vi.fn(),
    createWritable: vi.fn()
};

vi.mock('../lib/io/file-object/service', () => ({
    fileObjectService: fileObjectServiceMock
}));

const createFileRecord = () => ({
    isContainer: false,
    id: 'object',
    size: 128,
    chunkSize: 64,
    dataVolumes: [1],
    parityVolumes: [2],
});

describe('FuseServer', () => {
    beforeEach(() => {
        vi.resetModules();
        fuseInstances.length = 0;
        fileObjectInstances.length = 0;
        Object.values(databaseMock).forEach(mock => mock.mockReset());
        fileObjectServiceMock.openForRead.mockClear();
        fileObjectServiceMock.load.mockClear();
        fileObjectServiceMock.createWritable.mockClear();
    });

    const importServer = async () => {
        const { FuseServer } = await import('../lib/server/fuse/server');
        return new FuseServer();
    };

    it('mounts the filesystem on start', async () => {
        const server = await importServer();
        await server.start();

        expect(fuseInstances).toHaveLength(1);
        const instance = fuseInstances[0];
        expect(instance.mountPath).toBe('/run/strubs/data');
        expect(instance.opts).toMatchObject({ force: true, mkdir: true });
        expect(Object.keys(instance.handlers)).toContain('getattr');
    });

    it('returns directory stats for the root path', async () => {
        const server = await importServer();
        const cb = vi.fn();

        await server.fuse_getattr('/', cb);

        expect(cb).toHaveBeenCalledWith(0, expect.objectContaining({ mode: 0o40755 }));
    });

    it('returns file stats for object paths', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockResolvedValueOnce(createFileRecord());
        databaseMock.getTimestampFromId.mockReturnValueOnce(Date.now());
        const cb = vi.fn();

        await server.fuse_getattr('/object', cb);

        expect(databaseMock.getObjectByPath).toHaveBeenCalledWith('object');
        expect(cb).toHaveBeenCalledWith(0, expect.objectContaining({ size: 128 }));
    });

    it('lists container contents in readdir', async () => {
        const server = await importServer();
        databaseMock.getObjectsInContainerPath.mockResolvedValueOnce([{ name: 'foo' }, { name: 'bar' }]);
        const cb = vi.fn();

        await server.fuse_readdir('/', cb);

        expect(cb).toHaveBeenCalledWith(0, ['foo', 'bar']);
    });

    it('opens files read-only and stores descriptors', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockResolvedValue(createFileRecord());
        const cb = vi.fn();

        await server.fuse_open('/object', 0, cb);

        expect(fileObjectServiceMock.openForRead).toHaveBeenCalledTimes(1);
        const options = fileObjectServiceMock.openForRead.mock.calls[0]?.[1];
        expect(options?.requestId).toMatch(/^fuse-/);
        expect(fileObjectInstances[0]?.getRequestId()).toBe(options?.requestId ?? null);
        expect(cb).toHaveBeenCalledWith(0, 0);
    });

    it('rejects write attempts in fuse_open', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockResolvedValue(createFileRecord());
        const cb = vi.fn();

        await server.fuse_open('/object', 1, cb);

        expect(cb).toHaveBeenCalledWith(FUSE_ERRORS.EROFS);
    });

    it('releases file descriptors and closes objects', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockResolvedValue(createFileRecord());
        const openCb = vi.fn();
        await server.fuse_open('/object', 0, openCb);
        const fd = openCb.mock.calls[0][1] as number;
        const releaseCb = vi.fn();

        await server.fuse_release('/object', fd, releaseCb);

        expect(fileObjectInstances[0].close).toHaveBeenCalledTimes(1);
        expect(releaseCb).toHaveBeenCalledWith(0);
    });

    it('streams data through fuse_read into the provided buffer', async () => {
        const server = await importServer();
        const record = createFileRecord();
        record.size = 8;
        databaseMock.getObjectByPath.mockResolvedValue(record);
        const openCb = vi.fn();
        await server.fuse_open('/object', 0, openCb);
        const fd = openCb.mock.calls[0][1] as number;
        const buffer = Buffer.alloc(8);
        const readCb = vi.fn();

        server.fuse_read('/object', fd, buffer, 8, 0, readCb);
        const fileObject = fileObjectInstances[0];

        await new Promise(resolve => setImmediate(resolve));
        fileObject.emit('data', Buffer.from('1234'));
        fileObject.emit('data', Buffer.from('5678'));

        await new Promise(resolve => setImmediate(resolve));
        expect(readCb).toHaveBeenCalledWith(8);
        expect(buffer.toString()).toBe('12345678');
        expect(fileObject.acquireIOLock).toHaveBeenCalledTimes(1);
        expect(fileObject.releaseIOLock).toHaveBeenCalledTimes(1);
    });

    it('reports read errors as Fuse.EREMOTEIO', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockResolvedValue(createFileRecord());
        const openCb = vi.fn();
        await server.fuse_open('/object', 0, openCb);
        const fd = openCb.mock.calls[0][1] as number;
        const buffer = Buffer.alloc(4);
        const readCb = vi.fn();

        server.fuse_read('/object', fd, buffer, 4, 0, readCb);
        const fileObject = fileObjectInstances[0];

        await new Promise(resolve => setImmediate(resolve));
        fileObject.emit('error', new Error('boom'));

        await new Promise(resolve => setImmediate(resolve));
        expect(readCb).toHaveBeenCalledWith(FUSE_ERRORS.EREMOTEIO);
    });

    it('returns 0 bytes when read starts beyond EOF', async () => {
        const server = await importServer();
        const record = createFileRecord();
        record.size = 4;
        databaseMock.getObjectByPath.mockResolvedValue(record);
        const openCb = vi.fn();
        await server.fuse_open('/object', 0, openCb);
        const fd = openCb.mock.calls[0][1] as number;
        const buffer = Buffer.alloc(4);
        const readCb = vi.fn();

        await server.fuse_read('/object', fd, buffer, 4, 10, readCb);
        await new Promise(resolve => setImmediate(resolve));

        expect(readCb).toHaveBeenCalledWith(0);
        expect(fileObjectInstances[0].acquireIOLock).not.toHaveBeenCalled();
    });

    it('translates acquire lock failures into Fuse error codes', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockResolvedValue(createFileRecord());
        const openCb = vi.fn();
        await server.fuse_open('/object', 0, openCb);
        const fd = openCb.mock.calls[0][1] as number;
        const buffer = Buffer.alloc(4);
        const readCb = vi.fn();
        const fileObject = fileObjectInstances[0];
        fileObject.acquireIOLock.mockRejectedValueOnce({ code: 'EIO' });

        await server.fuse_read('/object', fd, buffer, 4, 0, readCb);

        expect(readCb).toHaveBeenCalledWith(FUSE_ERRORS.EIO);
    });

    it('maps Fuse error codes via _translateError', async () => {
        const server = await importServer();
        const translate = (server as unknown as { _translateError: (err: unknown) => number })._translateError.bind(server);

        expect(translate({ code: 'EISDIR' })).toBe(FUSE_ERRORS.EISDIR);
        expect(translate({ code: 'UNKNOWN' })).toBe(FUSE_ERRORS.ECONNRESET);
    });

    it('validates file metadata in _ensureFileRecord', async () => {
        const server = await importServer();
        const ensure = (server as unknown as { _ensureFileRecord: (obj: unknown) => unknown })._ensureFileRecord.bind(server);

        expect(() => ensure({ id: 'x' })).toThrow('object is missing file metadata');
        expect(() => ensure(createFileRecord())).not.toThrow();
    });

    it('propagates database errors through fuse_getattr callbacks', async () => {
        const server = await importServer();
        databaseMock.getObjectByPath.mockRejectedValueOnce({ code: 'EIO' });
        const cb = vi.fn();

        await server.fuse_getattr('/object', cb);

        expect(cb).toHaveBeenCalledWith(FUSE_ERRORS.EIO);
    });

    it('propagates database errors through fuse_readdir callbacks', async () => {
        const server = await importServer();
        databaseMock.getObjectsInContainerPath.mockRejectedValueOnce({ code: 'EIO' });
        const cb = vi.fn();

        await server.fuse_readdir('/', cb);

        expect(cb).toHaveBeenCalledWith(FUSE_ERRORS.EIO);
    });
});
