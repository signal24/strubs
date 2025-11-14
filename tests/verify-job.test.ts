import { describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'events';

vi.mock('../lib/io/file-object/service', () => ({
    fileObjectService: {}
}));

import { VerifyJob } from '../lib/jobs/verify-job';

class FileObjectStub extends EventEmitter {
    size = 1;
    private readonly error?: Error;
    private requestId: string | number | null = null;
    private priority: 'normal' | 'low' = 'normal';

    constructor(error?: Error) {
        super();
        this.error = error;
    }

    setReadRange(): void {
        // no-op for tests
    }

    resume(): void {
        if (this.error) {
            setImmediate(() => this.emit('error', this.error as Error));
            return;
        }

        setImmediate(() => {
            this.emit('data', Buffer.alloc(1));
            this.emit('end');
        });
    }

    async close(): Promise<void> {
        // no-op
    }

    setRequestId(requestId: string | null): void {
        this.requestId = requestId ?? null;
    }

    getRequestId(): string | null {
        return this.requestId;
    }

    setPriority(priority: 'normal' | 'low'): void {
        this.priority = priority;
    }

    getPriority(): 'normal' | 'low' {
        return this.priority;
    }

    getLoggerPrefix(): string {
        const idPart = this.id ?? 'file';
        return this.requestId ? `${this.requestId}:file:${idPart}` : `file:${idPart}`;
    }
}

const createLoggerFactory = () => {
    const loggerInstance = Object.assign(vi.fn(), { error: vi.fn() });
    return vi.fn(() => loggerInstance);
};

const createDeps = () => {
    const database = {
        findObjectsNeedingVerification: vi.fn(),
        updateObjectVerificationState: vi.fn().mockResolvedValue(undefined),
        setVolumeVerifyErrors: vi.fn().mockResolvedValue(undefined)
    };
    const runtimeConfig = {
        get: vi.fn(),
        set: vi.fn().mockResolvedValue(undefined),
        delete: vi.fn().mockResolvedValue(undefined)
    };
    const fileObjectService = {
        openForRead: vi.fn()
    };
    const volumeStub = { setVerifyErrors: vi.fn(), verifyErrors: { checksum: 2, total: 3 } };
    const ioManager = {
        getVolumeEntries: vi.fn().mockReturnValue([[1, volumeStub]]),
        getVolume: vi.fn().mockReturnValue(volumeStub)
    };

    return {
        database,
        runtimeConfig,
        fileObjectService,
        ioManager,
        createLogger: createLoggerFactory()
    };
};

describe('VerifyJob', () => {
    it('verifies objects and records lastVerify metadata', async () => {
        const deps = createDeps();
        const record = {
            id: 'abc',
            size: 1,
            dataVolumes: [1],
            parityVolumes: [],
            chunkSize: 1,
            dataSliceVolumeIds: [1],
            paritySliceVolumeIds: [],
            unavailableSlices: [],
            damagedSlices: [],
            isFile: true,
            name: 'file',
            md5: null
        };

        deps.runtimeConfig.get.mockResolvedValueOnce(null);
        deps.database.findObjectsNeedingVerification
            .mockResolvedValueOnce([record])
            .mockResolvedValueOnce([]);
        deps.fileObjectService.openForRead.mockResolvedValue(new FileObjectStub());

        const job = new VerifyJob(deps);
        const startPromise = job.start();
        const { startedAt } = await startPromise;
        const running = (job as unknown as { running: Promise<void> | null }).running;
        if (running)
            await running;

        expect(deps.database.updateObjectVerificationState).toHaveBeenCalledWith(record.id, {
            lastVerifiedAt: Date.parse(startedAt),
            sliceErrors: null
        });
        expect(deps.database.setVolumeVerifyErrors).toHaveBeenCalledTimes(1);
        expect(deps.database.setVolumeVerifyErrors).toHaveBeenNthCalledWith(1, 1, { checksum: 0, total: 0 });
        expect(deps.runtimeConfig.set).toHaveBeenNthCalledWith(1, 'verifyStartedAt', startedAt);
        expect(deps.runtimeConfig.set).toHaveBeenNthCalledWith(2, 'lastVerify', expect.objectContaining({
            startedAt,
            checksumErrors: 0,
            totalErrors: 0
        }));
    });

    it('records checksum failures and per-volume counts', async () => {
        const deps = createDeps();
        const record = {
            id: 'def',
            size: 1,
            dataVolumes: [1],
            parityVolumes: [],
            chunkSize: 1,
            dataSliceVolumeIds: [1],
            paritySliceVolumeIds: [],
            unavailableSlices: [],
            damagedSlices: [],
            isFile: true,
            name: 'file2',
            md5: null
        };

        deps.runtimeConfig.get.mockResolvedValueOnce(null);
        deps.database.findObjectsNeedingVerification
            .mockResolvedValueOnce([record])
            .mockResolvedValueOnce([]);

        const checksumError = Object.assign(new Error('checksum mismatch'), {
            code: 'ECHECKSUM',
            sliceIndex: 0,
            volumeId: 1
        });
        deps.fileObjectService.openForRead.mockResolvedValue(new FileObjectStub(checksumError));

        const job = new VerifyJob(deps);
        const startPromise = job.start();
        const { startedAt } = await startPromise;
        const running = (job as unknown as { running: Promise<void> | null }).running;
        if (running)
            await running;

        expect(deps.database.updateObjectVerificationState).toHaveBeenCalledWith(record.id, {
            lastVerifiedAt: Date.parse(startedAt),
            sliceErrors: { '0': { checksum: true } }
        });
        expect(deps.database.setVolumeVerifyErrors).toHaveBeenCalledTimes(2);
        expect(deps.database.setVolumeVerifyErrors).toHaveBeenNthCalledWith(1, 1, { checksum: 0, total: 0 });
        expect(deps.database.setVolumeVerifyErrors).toHaveBeenNthCalledWith(2, 1, { checksum: 1, total: 1 });
        expect(deps.runtimeConfig.set).toHaveBeenNthCalledWith(2, 'lastVerify', expect.objectContaining({
            checksumErrors: 1,
            totalErrors: 1
        }));
    });
});
