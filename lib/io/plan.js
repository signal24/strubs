// TODO: move FileObjectBase's calculations into Plan.
// have Planner create a plan with fileSize, dataSliceCount, paritySizeCount, and chunkSize.
// do the rest of the math in here. copy the relevant values out into FileObjectBase.

class Plan {
    constructor() {
        this.fileSize = null;

        this.dataSliceCount = null;
        this.paritySliceCount = null;
        
        this.dataVolumes = [];
        this.parityVolumes = [];

        this.chunkSize = null;
    }

    get totalSliceCount() {
        return this.dataSliceCount + this.paritySliceCount;
    }

    computeSliceSize() {
        // TODO: make more accurate later. maybe base it on feedback from FileObject instead of computing here.

        /*
        slice_data_size = ceil(file_size / data_slice_count)
        first_chunkSize = chunkSize - file_header_size
        chunk_count = 1 + ((slice_data_size - first_chunkSize + chunk_header_size) / (chunkSize - chunk_header_size))
        chunk_count = ceil(chunk_count)
        slice_size = file_header_size + slice_data_size + (chunk_header_size * chunk_count)
        */

        let sliceDataSize = Math.ceil(this.fileSize / this.dataSliceCount);
        let startChunkSize = this.chunkSize - constants.FILE_HEADER_SIZE;
        let chunkCount = Math.ceil(1 + ((sliceDataSize - startChunkSize + constants.CHUNK_HEADER_SIZE) / (this.chunkSize - constants.CHUNK_HEADER_SIZE)));
        let sliceSize = constants.FILE_HEADER_SIZE + sliceDataSize + (constants.CHUNK_HEADER_SIZE * chunkCount);

        this.sliceSize = sliceSize;
    }
}

module.exports = Plan;