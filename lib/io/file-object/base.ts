import type { FileObject } from '../file-object';
import { constants } from '../../constants';
import { create as createReedSolomonContext, encode as encodeReedSolomon } from '../../async-bridges/reed-solomon';
import { Slice } from './slice';

type ReedSolomonContext = ReturnType<typeof createReedSolomonContext>;

export class Base {
    protected readonly fileObject: FileObject;

    protected readonly size: number;
    protected readonly chunkSize: number;
    protected readonly dataSliceCount: number;
    protected readonly dataSliceVolumeIds: number[];
    protected readonly paritySliceCount: number;
    protected readonly paritySliceVolumeIds: number[];

    protected readonly _totalSliceCount: number;

    protected _slices: Slice[] = [];
    protected _rsSourcesBits: number | null = null;
    protected _rsTargetsBits: number | null = null;
    protected _rsContext: ReedSolomonContext | null = null;
    protected _dataOffset = 0;
    protected _chunkSetBuffer: Buffer | null = null;
    protected _chunkSetBufferPosition = 0;
    protected _chunkSetBufferRemaining = 0;
    public _startChunkDataSize = 0;
    protected _standardChunkSetOffset = 0;
    public _standardChunkDataSize = 0;
    protected _standardChunkSetDataSize = 0;
    public _standardChunkCountPerSlice = 0;
    protected _endChunkSetDataOffset = 0;
    public _endChunkDataSize = 0;
    protected _nextChunkGroupOffset = 0;
    public _chunkDataSize = 0;
    public _chunkSetDataSize = 0;
    protected _chunkSetParityOffset = 0;
    protected _chunkSetParitySize = 0;
    protected _chunkSetSliceOffsets: number[] = [];
    protected _chunkSetNextSliceIndex = 0;
    protected _chunkSetNextSliceOffset = 0;

    constructor(fileObject: FileObject) {
        this.fileObject = fileObject;

        this.size = this.fileObject.size;
        this.chunkSize = this.fileObject.chunkSize;
        this.dataSliceCount = this.fileObject.dataSliceCount;
        this.dataSliceVolumeIds = this.fileObject.dataSliceVolumeIds;
        this.paritySliceCount = this.fileObject.paritySliceCount;
        this.paritySliceVolumeIds = this.fileObject.paritySliceVolumeIds;

        this._totalSliceCount = this.dataSliceCount + this.paritySliceCount;
    }

    protected _configureInternals(): void {
        this._rsContext = createReedSolomonContext(this.dataSliceCount, this.paritySliceCount);

        this._dataOffset = 0;

        // how many bytes are useable in a data chunk?
        const plan = this.fileObject.plan;
        if (!plan || plan.standardChunkDataSize === null || plan.startChunkDataSize === null || plan.standardChunkCountPerSlice === null || plan.endChunkDataSize === null || plan.standardChunkSetOffset === null || plan.endChunkSetDataOffset === null)
            throw new Error('plan not configured');

        this._standardChunkDataSize = plan.standardChunkDataSize;
        this._standardChunkSetDataSize = this._standardChunkDataSize * this.dataSliceCount;

        // prep a buffer to be used
        const maxBufferSize = this._standardChunkDataSize * this._totalSliceCount;
        this._chunkSetBuffer = Buffer.allocUnsafe(maxBufferSize);

        // use precomputed sizes/offsets
        this._startChunkDataSize = plan.startChunkDataSize;
        this._standardChunkSetOffset = plan.standardChunkSetOffset;
        this._endChunkSetDataOffset = plan.endChunkSetDataOffset;
        this._endChunkDataSize = plan.endChunkDataSize;
        this._standardChunkCountPerSlice = plan.standardChunkCountPerSlice;
    }

    protected _configureStartState(): void {
        this._chunkDataSize = this._startChunkDataSize;
        this._nextChunkGroupOffset = this._standardChunkSetOffset;
        this._configureCommonState();
    }

    protected _configureMiddleState(): void {
        this._chunkDataSize = this._standardChunkDataSize;
        this._nextChunkGroupOffset = this._endChunkSetDataOffset;
        this._configureCommonState();
    }

    protected _configureEndState(): void {
        this._chunkDataSize = this._endChunkDataSize;
        this._nextChunkGroupOffset = Number.MAX_SAFE_INTEGER;
        this._configureCommonState();
    }

    protected _configureCommonState(): void {
        this._chunkSetDataSize = this._chunkDataSize * this.dataSliceCount;
        this._chunkSetParityOffset = this._chunkSetDataSize;
        this._chunkSetParitySize = this._chunkDataSize * this.paritySliceCount;

        this._chunkSetSliceOffsets = [];
        for (let index = 0; index < this._totalSliceCount; index++)
            this._chunkSetSliceOffsets.push(index * this._chunkDataSize);

        this._resetBufferPositions();
    }

    protected _resetBufferPositions(): void {
        this._chunkSetNextSliceIndex = 1;
        this._chunkSetNextSliceOffset = this._chunkSetSliceOffsets[1] ?? 0;

        this._chunkSetBufferPosition = 0;
        this._chunkSetBufferRemaining = this._chunkSetDataSize;
    }

    protected _configureNextChunkGroup(): void {
        if (this._dataOffset === this._endChunkSetDataOffset)
            this._configureEndState();
        else if (this._dataOffset === this._standardChunkSetOffset)
            this._configureMiddleState();
        else
            throw new Error('data offset not valid for this operation');
    }

    protected async _computeParity(): Promise<void> {
        if (!this._rsContext || !this._chunkSetBuffer)
            throw new Error('RS context not configured');

        await encodeReedSolomon(
            this._rsContext,
            this._rsSourcesBits,
            this._rsTargetsBits,
            this._chunkSetBuffer,
            0,
            this._chunkSetDataSize,
            this._chunkSetBuffer,
            this._chunkSetParityOffset,
            this._chunkSetParitySize
        );
    }

    protected async _instantiateSlices(): Promise<void> {
        for (let index = 0; index < this._totalSliceCount; index++) {
            const slice = new Slice(this.fileObject, this, index);
            this._slices.push(slice);
        }
    }
}
