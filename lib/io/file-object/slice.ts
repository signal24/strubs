import type { FileHandle } from 'fs/promises';

import { hash } from '../../async-bridges/crypto-async';
import { constants } from '../../constants';
import { ioManager } from '../manager';
import type { FileObject } from '../file-object';
import type { Volume } from '../volume';
import type { Base } from './base';

type SliceMode = 'write' | 'read' | null;

interface SliceChecksumError extends Error {
    code: string;
    objectId: string;
    sliceIndex: number;
    volumeId: number;
}

export class Slice {
    private readonly fileObject: FileObject;
    private readonly ioClass: Base;
    private readonly sliceIndex: number;
    private readonly _volumeId: number;
    private readonly _volume: Volume;
    private readonly _fileName: string;
    private readonly _reservedBytes: number;
    private readonly _hasReservation: boolean;

    private _size = 0;
    private _mode: SliceMode = null;
    private _cursorOffset = 0;
    private _isPerformingIO = false;
    private _isCommitted = false;
    private _writeBuf: Buffer | null = null;
    private _readBuf: Buffer | null = null;
    private _hashBuf: Buffer | null = null;
    private _outputFh: FileHandle | null = null;
    private _inputFh: FileHandle | null = null;

    constructor(fileObject: FileObject, ioClass: Base, sliceIndex: number) {
        if (!fileObject.id || !fileObject.idBuf)
            throw new Error('file object is not initialized');

        this.fileObject = fileObject;
        this.ioClass = ioClass;
        this.sliceIndex = sliceIndex;

        if (sliceIndex < this.fileObject.dataSliceCount)
            this._volumeId = this.fileObject.dataSliceVolumeIds[sliceIndex];
        else if (sliceIndex < this.fileObject.dataSliceCount + this.fileObject.paritySliceCount)
            this._volumeId = this.fileObject.paritySliceVolumeIds[sliceIndex - this.fileObject.dataSliceCount];
        else
            throw new Error('invalid slice index');

        const volume = ioManager.getVolume(this._volumeId);
        if (!volume)
            throw new Error(`volume ${this._volumeId} not found`);

        this._volume = volume;
        this._fileName = `${fileObject.id}.${sliceIndex}`;
        this._reservedBytes = this.fileObject.plan?.sliceSize ?? this.fileObject.chunkSize;
        this._hasReservation = this.fileObject.hasVolumeReservations();
    }

    async create(): Promise<void> {
        const writeBuf = this._writeBuf = Buffer.allocUnsafe(this.fileObject.chunkSize);
        const idBuf = this.fileObject.idBuf;
        if (!idBuf)
            throw new Error('file object id buffer is not initialized');

        this._size = constants.FILE_HEADER_SIZE;
        this._cursorOffset = constants.FILE_HEADER_SIZE;
        this._isPerformingIO = true;

        this._outputFh = await this._volume.createTemporaryFh(this._fileName);

        /* 00-03 */ writeBuf.write('\x01\xfb\x02\xfb', 0); // magic header
        /* 04-04 */ writeBuf.writeUInt8(1, 4); // version
        /* 05-06 */ writeBuf.writeUInt16LE(constants.FILE_HEADER_SIZE, 5); // header length
        /* 07-22 */ ; // header checksum (will populate after its computed)
        /* 23-34 */ idBuf.copy(writeBuf, 23, 0, 12); // file ID
        /* 35-39 */ writeBuf.writeIntLE(this.fileObject.size, 35, 5); // file size
        /* 40-40 */ writeBuf.writeUInt8(this.fileObject.dataSliceCount, 40); // data slice count
        /* 41-41 */ writeBuf.writeUInt8(this.fileObject.paritySliceCount, 41); // parity slice count
        /* 42-42 */ writeBuf.writeUInt8(this.sliceIndex, 42); // slice index
        /* 43-45 */ writeBuf.writeIntLE(this.fileObject.chunkSize, 43, 3); // chunk size
        /* 46-47 */ writeBuf.fill(0, 46); // end padding to make the header length a multiple of 8

        await hash('md5', writeBuf, 23, 25, writeBuf, 7);

        const outputFh = this._outputFh;
        if (!outputFh)
            throw new Error('output file handle is not initialized');

        await outputFh.write(writeBuf, 0, constants.FILE_HEADER_SIZE);

        this._isPerformingIO = false;
        this._mode = 'write';
    }

    async writeChunk(data: Buffer): Promise<void> {
        if (this._isPerformingIO)
            throw new Error('slice already writing');
        if (this._mode !== 'write')
            throw new Error('slice not opened for writing');
        if (!this._writeBuf || !this._outputFh)
            throw new Error('slice not prepared for writing');

        this._isPerformingIO = true;

        const writeBuf = this._writeBuf;
        const dataLen = data.length;
        const chunkLen = data.length + constants.CHUNK_HEADER_SIZE;

        data.copy(writeBuf, constants.CHUNK_HEADER_SIZE, 0, dataLen);
        await hash(constants.CHUNK_HEADER_ALGO, writeBuf, constants.CHUNK_HEADER_SIZE, dataLen, writeBuf, 0);
        await this._outputFh.write(writeBuf, 0, chunkLen);

        this._size += chunkLen;
        this._isPerformingIO = false;
    }

    isAvailable(): boolean {
        return this._volume.isReadable && !this.fileObject.unavailableSliceIdxs?.includes(this.sliceIndex);
    }

    async open(): Promise<void> {
        this._readBuf = Buffer.allocUnsafe(this.fileObject.chunkSize);
        this._hashBuf = Buffer.allocUnsafe(constants.CHUNK_HEADER_SIZE);

        this._isPerformingIO = true;

        this._inputFh = await this._volume.openCommittedFh(this._fileName);

        const inputFh = this._inputFh;
        if (!inputFh)
            throw new Error('input file handle is not initialized');

        try {
            await inputFh.read(this._readBuf, 0, constants.FILE_HEADER_SIZE);
        } catch (err) {
            const throwErr = new Error('failed to read slice header') as Error & { cause?: unknown; fileName?: string; volumeId?: number };
            throwErr.cause = err;
            throwErr.fileName = this._fileName;
            throwErr.volumeId = this._volumeId;
            throw throwErr;
        }

        this._cursorOffset = constants.FILE_HEADER_SIZE;
        this._isPerformingIO = false;
        this._mode = 'read';
        this._isCommitted = true;
    }

    seekToHead(): void {
        this._cursorOffset = constants.FILE_HEADER_SIZE;
    }

    seekToChunkIndex(chunkIndex: number): void {
        this._cursorOffset = constants.FILE_HEADER_SIZE;

        const headerChunkCount = chunkIndex > 0 ? 1 : 0;
        this._cursorOffset += headerChunkCount * (constants.CHUNK_HEADER_SIZE + this.ioClass._startChunkDataSize);

        const standardChunkCount = Math.max(0, chunkIndex - 1);
        this._cursorOffset += standardChunkCount * (constants.CHUNK_HEADER_SIZE + this.ioClass._standardChunkDataSize);
    }

    async readChunk(): Promise<Buffer> {
        if (this._isPerformingIO)
            throw new Error('slice already reading');
        if (this._mode !== 'read')
            throw new Error('slice not opened for reading');
        if (!this._readBuf || !this._hashBuf || !this._inputFh)
            throw new Error('slice not prepared for reading');

        this._isPerformingIO = true;

        const readBuf = this._readBuf;
        const hashBuf = this._hashBuf;
        const readDataLen = this.ioClass._chunkDataSize;
        const readLen = constants.CHUNK_HEADER_SIZE + readDataLen;

        await this._inputFh.read(readBuf, 0, readLen, this._cursorOffset);

        await hash(constants.CHUNK_HEADER_ALGO, readBuf, constants.CHUNK_HEADER_SIZE, readDataLen, hashBuf, 0);

        if (!readBuf.slice(0, constants.CHUNK_HEADER_SIZE).equals(hashBuf))
            this.throwChecksumError();

        this._cursorOffset += readLen;
        this._isPerformingIO = false;

        return readBuf.slice(constants.CHUNK_HEADER_SIZE, readLen);
    }

    async skipChunk(): Promise<void> {
        this._cursorOffset += constants.CHUNK_HEADER_SIZE + this.ioClass._chunkDataSize;
    }

    async close(): Promise<void> {
        if (this._isPerformingIO)
            throw new Error('slice busy');

        this._isPerformingIO = true;

        if (this._mode === 'write' && this._outputFh) {
            await this._outputFh.sync();
            await this._outputFh.close();
            this._outputFh = null;
        }
        else if (this._mode === 'read' && this._inputFh) {
            await this._inputFh.close();
            this._inputFh = null;
        }
        else if (this._mode === null) {
            this._isPerformingIO = false;
            throw new Error('slice is not open');
        }

        this._mode = null;
        this._isPerformingIO = false;
    }

    async commit(): Promise<void> {
        if (this._isPerformingIO)
            throw new Error('slice busy');

        this._isPerformingIO = true;
        await this._volume.commitTemporaryFile(this._fileName);
        if (this._hasReservation)
            this._volume.applyCommittedBytes(this._reservedBytes, this._size, this.sliceIndex < this.fileObject.dataSliceCount ? 'data' : 'parity');
        this._isPerformingIO = false;

        this._isCommitted = true;
        this._mode = null;
    }

    async delete(): Promise<void> {
        if (this._isPerformingIO)
            throw new Error('slice busy');

        if (this._mode === 'write' && this._outputFh) {
            await this._outputFh.close();
            this._outputFh = null;
        }
        else if (this._mode === 'read') {
            // nothing to do
        }

        this._isPerformingIO = true;
        const sliceType: 'data' | 'parity' = this.sliceIndex < this.fileObject.dataSliceCount ? 'data' : 'parity';

        if (this._isCommitted) {
            await this._volume.deleteCommittedFile(this._fileName);
            this._volume.releaseCommittedBytes(this._size, sliceType);
        }
        else {
            await this._volume.deleteTemporaryFile(this._fileName);
            if (this._hasReservation)
                this._volume.releaseReservation(this._reservedBytes);
        }
        this._isPerformingIO = false;

        this._mode = null;
    }

    markAsCommitted(): void {
        this._isCommitted = true;
    }

    private throwChecksumError(): never {
        const err = new Error('checksum mismatch at ' + this._fileName + ':' + this._cursorOffset) as SliceChecksumError;
        err.code = 'ECHECKSUM';
        if (!this.fileObject.id)
            throw new Error('file object is not initialized');
        err.objectId = this.fileObject.id;
        err.sliceIndex = this.sliceIndex;
        err.volumeId = this._volumeId;
        throw err;
    }
}
