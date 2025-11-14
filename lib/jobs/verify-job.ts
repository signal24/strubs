import { database, type SliceErrorInfo } from '../database';
import { runtimeConfig } from '../runtime-config';
import { fileObjectService, type FileObjectService } from '../io/file-object/service';
import type { StoredObjectRecord, FileObject } from '../io/file-object';
import { ioManager } from '../io/manager';
import { createLogger } from '../log';
import type { Volume } from '../io/volume';

type VolumeErrorCounters = {
    checksum: number;
    total: number;
};

type VerifyJobDeps = {
    database: typeof database;
    runtimeConfig: typeof runtimeConfig;
    fileObjectService: FileObjectService;
    ioManager: typeof ioManager;
    createLogger: typeof createLogger;
};

type VerifyErrorSnapshot = {
    total: number;
    volumes: Record<string, number>;
};

type VerifyObjectResult = {
    checksumErrors: number;
    totalErrors: number;
    volumeImpacts: Map<number, VolumeErrorCounters>;
};

const defaultDeps: VerifyJobDeps = {
    database,
    runtimeConfig,
    fileObjectService,
    ioManager,
    createLogger
};

const VERIFY_BATCH_SIZE = 25;

export class VerifyJob {
    private readonly deps: VerifyJobDeps;
    private readonly log: ReturnType<typeof createLogger>;
    private running: Promise<void> | null = null;
    private cancelRequested = false;
    private startedAt: string | null = null;
    private progress = {
        objectsVerified: 0,
        errors: {
            total: 0,
            volumes: {} as Record<string, number>
        }
    };
    private progressLogger: NodeJS.Timeout | null = null;

    constructor(deps?: Partial<VerifyJobDeps>) {
        this.deps = { ...defaultDeps, ...deps };
        this.log = this.deps.createLogger('verify-job');
    }

    async start(): Promise<{ startedAt: string }> {
        if (this.running)
            return { startedAt: this.startedAt as string };

        const existing = await this.deps.runtimeConfig.get('verifyStartedAt');
        if (typeof existing === 'string' && existing.length) {
            this.launch(existing, true);
            return { startedAt: existing };
        }

        const startedAt = new Date().toISOString();
        await this.deps.runtimeConfig.set('verifyStartedAt', startedAt);
        this.launch(startedAt, false);
        return { startedAt };
    }

    async resumePendingJob(): Promise<void> {
        if (this.running)
            return;
        const existing = await this.deps.runtimeConfig.get('verifyStartedAt');
        if (typeof existing !== 'string' || !existing.length)
            return;
        this.log('resuming verify job started at %s', existing);
        this.launch(existing, true);
    }

    async stop(): Promise<void> {
        const running = this.running;
        if (!running) {
            await this.deps.runtimeConfig.delete('verifyStartedAt');
            return;
        }
        this.log('stop requested');
        this.cancelRequested = true;
        await running;
    }

    isRunning(): boolean {
        return Boolean(this.running);
    }

    getStatus(): { running: boolean; startedAt: string | null; objectsVerified: number; errors: VerifyErrorSnapshot } {
        return {
            running: this.isRunning(),
            startedAt: this.startedAt,
            objectsVerified: this.progress.objectsVerified,
            errors: this.progress.errors
        };
    }

    private launch(startedAt: string, isResume: boolean): void {
        if (this.running)
            return;

        this.startedAt = startedAt;
        this.cancelRequested = false;
        this.progress.objectsVerified = 0;
        this.progress.errors = { total: 0, volumes: {} };
        this.startProgressLogger();
        this.running = this.execute(startedAt, isResume)
            .catch(err => {
                this.log.error('verify job failed', err);
            })
            .finally(() => {
                this.stopProgressLogger();
                this.startedAt = null;
                this.running = null;
                this.cancelRequested = false;
            });
    }

    private async execute(startedAt: string, isResume: boolean): Promise<void> {
        const startedAtMs = Date.parse(startedAt);
        if (!Number.isFinite(startedAtMs))
            throw new Error('invalid verify start time');

        const volumeCounts = this.initializeVolumeCounters();
        if (!isResume)
            await this.resetVolumeCounters(volumeCounts);
        let checksumErrors = 0;
        let totalErrors = 0;

        try {
            while (!this.cancelRequested) {
                const batch = await this.fetchBatch(startedAtMs);
                if (!batch.length)
                    break;

                for (const record of batch) {
                    if (this.cancelRequested)
                        break;
                    const result = await this.verifyObject(record, startedAtMs);
                    if (!result)
                        break;
                    this.progress.objectsVerified++;
                    checksumErrors += result.checksumErrors;
                    totalErrors += result.totalErrors;
                    await this.mergeVolumeResults(volumeCounts, result.volumeImpacts);
                    this.logProgress(totalErrors, volumeCounts);
                }

                if (this.cancelRequested)
                    break;
            }
        }
        finally {
            await this.deps.runtimeConfig.delete('verifyStartedAt');
            if (!this.cancelRequested) {
                await this.deps.runtimeConfig.set('lastVerify', {
                    startedAt,
                    finishedAt: new Date().toISOString(),
                    checksumErrors,
                    totalErrors
                });
            }
        }
    }

    private initializeVolumeCounters(): Map<number, VolumeErrorCounters> {
        const counts = new Map<number, VolumeErrorCounters>();
        for (const [id, volume] of this.deps.ioManager.getVolumeEntries()) {
            const existing = volume.verifyErrors ?? { checksum: 0, total: 0 };
            counts.set(id, { checksum: existing.checksum, total: existing.total });
        }
        return counts;
    }

    private async fetchBatch(startedAt: number): Promise<StoredObjectRecord[]> {
        const objects = await this.deps.database.findObjectsNeedingVerification(startedAt, VERIFY_BATCH_SIZE);
        return objects as StoredObjectRecord[];
    }

    private async verifyObject(record: StoredObjectRecord, startedAtMs: number): Promise<VerifyObjectResult | null> {
        let object: FileObject | null = null;
        try {
            object = await this.deps.fileObjectService.openForRead(record, { requestId: `verify` });
            if (this.cancelRequested)
                return null;
            await this.consumeObject(object);
            await this.deps.database.updateObjectVerificationState(record.id, {
                lastVerifiedAt: startedAtMs,
                sliceErrors: null
            });
            return {
                checksumErrors: 0,
                totalErrors: 0,
                volumeImpacts: new Map()
            };
        }
        catch (err) {
            const normalized = this.normalizeSliceError(record, err);
            const sliceErrors = normalized ? { [normalized.sliceKey]: normalized.info } : null;
            await this.deps.database.updateObjectVerificationState(record.id, {
                lastVerifiedAt: startedAtMs,
                sliceErrors
            });

            const volumeImpacts = new Map<number, VolumeErrorCounters>();
            if (normalized?.volumeId !== null && normalized?.volumeId !== undefined) {
                volumeImpacts.set(normalized.volumeId, {
                    checksum: normalized.isChecksum ? 1 : 0,
                    total: 1
                });
            }

            const message = err instanceof Error ? err.message : String(err);
            this.log.error('object %s verification failed: %s', record.id, message);

            return {
                checksumErrors: normalized?.isChecksum ? 1 : 0,
                totalErrors: normalized ? 1 : 0,
                volumeImpacts
            };
        }
        finally {
            if (object) {
                try {
                    await object.close();
                }
                catch (closeErr) {
                    this.log.error('failed closing file object %s', record.id, closeErr);
                }
            }
        }
    }

    private consumeObject(object: FileObject): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            let settled = false;
            const cancellationError = new Error('verification canceled');

            const cleanup = (): void => {
                object.removeListener('data', onData);
                object.removeListener('end', onEnd);
                object.removeListener('error', onError);
            };

            const abortIfCanceled = (): boolean => {
                if (!this.cancelRequested || settled)
                    return false;
                settled = true;
                cleanup();
                void object.close().catch(() => undefined);
                reject(cancellationError);
                return true;
            };

            const onData = (): void => {
                if (abortIfCanceled())
                    return;
                // intentionally discard data
            };

            const onEnd = (): void => {
                if (abortIfCanceled())
                    return;
                settled = true;
                cleanup();
                resolve();
            };

            const onError = (err: Error): void => {
                if (settled)
                    return;
                settled = true;
                cleanup();
                reject(err);
            };

            object.on('data', onData);
            object.once('end', onEnd);
            object.once('error', onError);

            try {
                object.setReadRange(0, object.size, true);
            }
            catch (err) {
                cleanup();
                reject(err as Error);
                return;
            }

            if (abortIfCanceled())
                return;

            object.resume();
            void abortIfCanceled();
        });
    }

    private normalizeSliceError(
        record: StoredObjectRecord,
        err: unknown
    ): { sliceKey: string; info: SliceErrorInfo; volumeId: number | null; isChecksum: boolean } | null {
        const errorObj = err as Error & { code?: string; sliceIndex?: number; volumeId?: number };
        const sliceIndex = typeof errorObj.sliceIndex === 'number' ? errorObj.sliceIndex : null;
        const isChecksum = errorObj.code === 'ECHECKSUM';
        const sliceKey = sliceIndex !== null ? String(sliceIndex) : 'unknown';
        const info: SliceErrorInfo = isChecksum
            ? { checksum: true }
            : { err: errorObj.message ?? String(err) };
        const volumeId = errorObj.volumeId ?? this.resolveVolumeId(record, sliceIndex);
        return {
            sliceKey,
            info,
            volumeId,
            isChecksum
        };
    }

    private resolveVolumeId(record: StoredObjectRecord, sliceIndex: number | null): number | null {
        if (sliceIndex === null)
            return null;
        if (sliceIndex < record.dataVolumes.length)
            return record.dataVolumes[sliceIndex] ?? null;
        const parityIndex = sliceIndex - record.dataVolumes.length;
        return record.parityVolumes[parityIndex] ?? null;
    }

    private async mergeVolumeResults(
        aggregate: Map<number, VolumeErrorCounters>,
        impacts: Map<number, VolumeErrorCounters>
    ): Promise<void> {
        const operations: Promise<void>[] = [];
        impacts.forEach((impact, volumeId) => {
            if (!impact.total)
                return;
            const entry = aggregate.get(volumeId) ?? { checksum: 0, total: 0 };
            entry.checksum += impact.checksum;
            entry.total += impact.total;
            aggregate.set(volumeId, entry);
            operations.push(this.persistVolumeError(volumeId, entry));
        });
        await Promise.all(operations);
    }

    private async resetVolumeCounters(counts: Map<number, VolumeErrorCounters>): Promise<void> {
        const operations: Promise<void>[] = [];
        counts.forEach((entry, volumeId) => {
            entry.checksum = 0;
            entry.total = 0;
            operations.push(this.persistVolumeError(volumeId, entry));
        });
        await Promise.all(operations);
    }

    private async persistVolumeError(volumeId: number, counters: VolumeErrorCounters): Promise<void> {
        const payload = { checksum: counters.checksum, total: counters.total };
        await this.deps.database.setVolumeVerifyErrors(volumeId, payload);
        const volume = this.deps.ioManager.getVolume(volumeId) as Volume | undefined;
        volume?.setVerifyErrors({ ...payload });
    }

    private logProgress(totalErrors: number, volumeCounts: Map<number, VolumeErrorCounters>): void {
        const volumes: Record<string, number> = {};
        for (const [volumeId, counters] of volumeCounts.entries()) {
            if (counters.total > 0)
                volumes[String(volumeId)] = counters.total;
        }
        this.progress.errors = { total: totalErrors, volumes };
    }

    private startProgressLogger(): void {
        if (this.progressLogger)
            return;
        this.progressLogger = setInterval(() => {
            const { objectsVerified, errors } = this.progress;
            let message = `verified:${objectsVerified} errors:${errors.total}`;
            const entries = Object.entries(errors.volumes).sort((a, b) => Number(a[0]) - Number(b[0]));
            for (const [volumeId, count] of entries)
                message += ` [vol-${volumeId}]:${count}`;
            this.log(message);
        }, 5000);
        this.progressLogger.unref?.();
    }

    private stopProgressLogger(): void {
        if (!this.progressLogger)
            return;
        clearInterval(this.progressLogger);
        this.progressLogger = null;
    }
}

export const verifyJob = new VerifyJob();
