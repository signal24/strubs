import { constants } from '../constants';

// TODO: move FileObjectBase's calculations into Plan.
// have Planner create a plan with fileSize, dataSliceCount, paritySizeCount, and chunkSize.
// do the rest of the math in here. copy the relevant values out into FileObjectBase.

export class Plan {
    fileSize: number | null = null;
    dataSliceCount: number | null = null;
    paritySliceCount: number | null = null;
    dataVolumes: number[] = [];
    parityVolumes: number[] = [];
    chunkSize: number | null = null;
    sliceSize: number | null = null;
    startChunkDataSize: number | null = null;
    standardChunkDataSize: number | null = null;
    endChunkDataSize: number | null = null;
    standardChunkCountPerSlice: number | null = null;
    standardChunkSetOffset: number | null = null;
    endChunkSetDataOffset: number | null = null;

    get totalSliceCount(): number {
        return (this.dataSliceCount ?? 0) + (this.paritySliceCount ?? 0);
    }

    computeSliceSize(): void {
        populatePlanDerivedFields(this);
    }
}

export function populatePlanDerivedFields(plan: Plan): void {
    if (plan.fileSize === null || plan.dataSliceCount === null || plan.chunkSize === null)
        throw new Error('plan is not configured');

    const sliceDataSize = Math.ceil(plan.fileSize / plan.dataSliceCount);
    const startChunkSize = plan.chunkSize - constants.FILE_HEADER_SIZE;
    const chunkCount = Math.ceil(1 + ((sliceDataSize - startChunkSize + constants.CHUNK_HEADER_SIZE) / (plan.chunkSize - constants.CHUNK_HEADER_SIZE)));
    plan.sliceSize = constants.FILE_HEADER_SIZE + sliceDataSize + (constants.CHUNK_HEADER_SIZE * chunkCount);
    plan.standardChunkDataSize = plan.chunkSize - constants.CHUNK_HEADER_SIZE;
    plan.startChunkDataSize = Math.min(plan.standardChunkDataSize - constants.FILE_HEADER_SIZE, Math.floor(plan.fileSize / plan.dataSliceCount));
    plan.startChunkDataSize = Math.max(1, plan.startChunkDataSize);
    plan.startChunkDataSize = Math.ceil(plan.startChunkDataSize / 8) * 8;
    plan.standardChunkSetOffset = plan.startChunkDataSize * plan.dataSliceCount;
    const bytesRemaining = Math.max(0, plan.fileSize - plan.standardChunkSetOffset);
    const bytesRemainingPerSlice = Math.ceil(bytesRemaining / plan.dataSliceCount);
    plan.standardChunkCountPerSlice = Math.floor(bytesRemainingPerSlice / plan.standardChunkDataSize);
    const totalBytesInStandardChunks = plan.standardChunkCountPerSlice * plan.standardChunkDataSize * plan.dataSliceCount;
    const totalBytesBeforeEndChunkSet = plan.standardChunkSetOffset + totalBytesInStandardChunks;
    const endChunkSetBytes = plan.fileSize - totalBytesBeforeEndChunkSet;
    plan.endChunkSetDataOffset = plan.fileSize - endChunkSetBytes;
    plan.endChunkDataSize = Math.ceil(endChunkSetBytes / plan.dataSliceCount);
    plan.endChunkDataSize = Math.ceil(plan.endChunkDataSize / 8) * 8;
}
