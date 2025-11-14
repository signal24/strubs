import crypto from 'crypto';

import { createLogger } from '../../log';
import type { FileObject } from '../file-object';
import { Base } from './base';

export class FileObjectWriter extends Base {
    md5: Buffer | null = null;

    private _bytesWritten = 0;
    private _sliceWritePromises: Record<number, Promise<void> | null> = {};
    private _commitPromises: Promise<void>[] | null = null;
    private _isAborting = false;
    private _md5Hasher: crypto.Hash | null = crypto.createHash('md5');
    private _lastWriteLogTimestamp = 0;
    private logger: ReturnType<typeof createLogger>;

    constructor(fileObject: FileObject) {
        super(fileObject);
        this.logger = this.createLogger();
    }

    protected _configureInternals(): void {
        super._configureInternals();

        this._rsSourcesBits = 0;
        for (let index = 0; index < this.dataSliceCount; index++)
            this._rsSourcesBits |= (1 << index);

        this._rsTargetsBits = 0;
        for (let index = this.dataSliceCount; index < this._totalSliceCount; index++)
            this._rsTargetsBits |= (1 << index);
    }

    onRequestContextChanged(): void {
        this.logger = this.createLogger();
    }

    private createLogger(): ReturnType<typeof createLogger> {
        const prefix = `${this.fileObject.getLoggerPrefix()}:writer`;
        return createLogger(prefix);
    }

    async prepare(): Promise<void> {
        this._configureInternals();
        this._configureStartState();
        await this._instantiateSlices();

        await Promise.all(this._slices.map(slice => slice.create()));
    }

    async write(data: Buffer): Promise<void> {
        if (!this._chunkSetBuffer)
            throw new Error('writer not initialized');

        this._bytesWritten += data.length;
        this._logWriteProgress(this._bytesWritten);
        if (!this._md5Hasher)
            throw new Error('hash not initialized');
        this._md5Hasher.update(data);

        let buffer = data;
        while (buffer.length) {
            const bytesToWrite = Math.min(buffer.length, this._chunkSetBufferRemaining);
            buffer.copy(this._chunkSetBuffer, this._chunkSetBufferPosition, 0, bytesToWrite);

            this._chunkSetBufferPosition += bytesToWrite;
            this._chunkSetBufferRemaining -= bytesToWrite;

            this._dataOffset += bytesToWrite;

            if (this._chunkSetBufferRemaining === 0 || this._chunkSetBufferPosition >= this._chunkSetNextSliceOffset)
                await this._writeBuffer();

            buffer = buffer.slice(bytesToWrite);
        }
    }

    async finish(): Promise<void> {
        if (!this._chunkSetBuffer)
            throw new Error('writer not initialized');

        if (this._bytesWritten !== this.size)
            throw new Error(`writer expected ${this.size} bytes, received ${this._bytesWritten}`);

        if (this._chunkSetBufferPosition > 0) {
            this._chunkSetBuffer.fill(0, this._chunkSetBufferPosition);
            this._chunkSetBufferPosition = this._chunkSetDataSize;
            this._chunkSetBufferRemaining = 0;
            await this._writeBuffer();
        }

        for (const sliceIndex of Object.keys(this._sliceWritePromises)) {
            const index = Number(sliceIndex);
            const promise = this._sliceWritePromises[index];
            if (promise)
                await promise;
        }

        if (!this._md5Hasher)
            throw new Error('hash not initialized');
        this.md5 = this._md5Hasher.digest();
        this._md5Hasher = null;
    }

    async commit(): Promise<void> {
        this._commitPromises = this._slices.map(slice => slice.close());
        await Promise.all(this._commitPromises);
        this._commitPromises = null;

        if (this._isAborting)
            throw new Error('object write was aborted');

        this._commitPromises = this._slices.map(slice => slice.commit());
        await Promise.all(this._commitPromises);
        this._commitPromises = null;

        if (this._isAborting)
            throw new Error('object write was aborted');
    }

    private async _writeBuffer(): Promise<void> {
        if (!this._chunkSetBuffer)
            throw new Error('writer buffer not initialized');

        while (this._chunkSetNextSliceOffset <= this._chunkSetBufferPosition && this._chunkSetNextSliceIndex <= this.dataSliceCount) {
            const sliceIndexToWrite = this._chunkSetNextSliceIndex - 1;
            const start = this._chunkDataSize * sliceIndexToWrite;
            const end = this._chunkDataSize * this._chunkSetNextSliceIndex;
            const sliceData = this._chunkSetBuffer.slice(start, end);
            await this._queueSliceWrite(sliceIndexToWrite, sliceData);
            this._chunkSetNextSliceIndex++;
            this._chunkSetNextSliceOffset += this._chunkDataSize;
        }

        if (this._chunkSetNextSliceIndex > this.dataSliceCount) {
            await this._computeAndWriteParity();

            if (this._dataOffset === this._nextChunkGroupOffset)
                this._configureNextChunkGroup();
            else
                this._resetBufferPositions();
        }
    }

    private async _computeAndWriteParity(): Promise<void> {
        if (!this._chunkSetBuffer)
            throw new Error('writer buffer not initialized');

        await this._computeParity();

        for (let sliceIndex = this.dataSliceCount; sliceIndex < this._totalSliceCount; sliceIndex++) {
            const sliceOffset = this._chunkSetSliceOffsets[sliceIndex];
            const sliceData = this._chunkSetBuffer.slice(sliceOffset, sliceOffset + this._chunkDataSize);
            await this._queueSliceWrite(sliceIndex, sliceData);
        }
    }

    private async _queueSliceWrite(sliceIndex: number, data: Buffer): Promise<void> {
        const existingPromise = this._sliceWritePromises[sliceIndex];
        if (existingPromise)
            await existingPromise;

        if (this._isAborting)
            return;

        const promise = this._slices[sliceIndex].writeChunk(data);
        this._sliceWritePromises[sliceIndex] = promise;

        promise.then(() => {
            this._sliceWritePromises[sliceIndex] = null;
        });
    }

    async abort(): Promise<void> {
        this._isAborting = true;

        if (this._commitPromises)
            await Promise.all(this._commitPromises);

        const deletePromises = this._slices.map(async (slice, sliceIndex) => {
            const pendingWrite = this._sliceWritePromises[sliceIndex];
            if (pendingWrite)
                await pendingWrite;
            await slice.delete();
        });

        await Promise.all(deletePromises);
    }

    private _logWriteProgress(bytesWritten: number): void {
        const now = Date.now();
        if (now - this._lastWriteLogTimestamp < 2000)
            return;

        this._lastWriteLogTimestamp = now;
        const total = this.fileObject.size;
        const committed = Math.min(bytesWritten, total);
        this.logger('wrote %d bytes (cur offset: %d)', committed, bytesWritten);
    }
}
