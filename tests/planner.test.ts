import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Volume } from '../lib/io/volume';

const ioManagerMock = {
    getWritableVolumes: vi.fn<[], Volume[]>(),
};

const configMock = {
    dataSliceCount: 2,
    paritySliceCount: 1,
    chunkSize: 4096
};

vi.mock('../lib/io/manager', () => ({
    ioManager: ioManagerMock
}));

vi.mock('../lib/config', () => ({
    config: configMock
}));

vi.mock('lodash', () => ({
    default: {
        shuffle: <T>(items: T[]): T[] => items
    }
}));

let planner: typeof import('../lib/io/planner').planner;

beforeAll(async () => {
    ({ planner } = await import('../lib/io/planner'));
});

beforeEach(() => {
    vi.clearAllMocks();
    configMock.dataSliceCount = 2;
    configMock.paritySliceCount = 1;
    configMock.chunkSize = 4096;
});

const createVolume = (id: number, overrides: Partial<Volume> = {}): Volume => ({
    id,
    uuid: `uuid-${id}`,
    blockPath: null,
    fsType: null,
    mountPoint: null,
    mountOptions: null,
    isMounted: true,
    isVerified: true,
    isStarted: true,
    isEnabled: true,
    isHealthy: true,
    isReadOnly: false,
    deviceSerial: null,
    partitionUuid: null,
    bytesTotal: 1024,
    bytesUsedData: 0,
    bytesUsedParity: 0,
    bytesFree: 1024,
    bytesPending: 0,
    deviceName: null,
    deviceGroup: null,
    reserveSpace(this: any, bytes: number) {
        this.bytesPending += bytes;
    },
    releaseReservation(this: any, bytes: number) {
        this.bytesPending = Math.max(0, this.bytesPending - bytes);
    },
    applyCommittedBytes: vi.fn(),
    stop: vi.fn().mockResolvedValue(undefined),
    ...overrides
} as Volume);

describe('Planner.generatePlan', () => {
    it('distributes slices across the highest-capacity volumes and updates bytesPending', () => {
        const volumes = [
            createVolume(1, { bytesFree: 900 }),
            createVolume(2, { bytesFree: 800 }),
            createVolume(3, { bytesFree: 700 }),
            createVolume(4, { bytesFree: 100 })
        ];
        ioManagerMock.getWritableVolumes.mockReturnValue(volumes);

        const fileSize = 1500;
        const plan = planner.generatePlan(fileSize);
        const reservedBytes = plan.sliceSize ?? 0;

        expect(plan.fileSize).toBe(fileSize);
        expect(plan.chunkSize).toBe(configMock.chunkSize);
        expect(plan.dataSliceCount).toBe(configMock.dataSliceCount);
        expect(plan.paritySliceCount).toBe(configMock.paritySliceCount);
        expect(plan.dataVolumes).toEqual([1, 2]);
        expect(plan.parityVolumes).toEqual([3]);
        expect(volumes[0].bytesPending).toBe(reservedBytes);
        expect(volumes[1].bytesPending).toBe(reservedBytes);
        expect(volumes[2].bytesPending).toBe(reservedBytes);
        expect(volumes[3].bytesPending).toBe(0);
    });

    it('throws when insufficient writable volumes are available', () => {
        const volumes = [
            createVolume(1, { bytesFree: 900 }),
            createVolume(2, { bytesFree: 800 })
        ];
        ioManagerMock.getWritableVolumes.mockReturnValue(volumes);

        expect(() => planner.generatePlan(512)).toThrow('not enough volumes available for planned slice count');
    });

    it('falls back to simple size sorting when alternating groups are insufficient', () => {
        const volumes = [
            createVolume(1, { bytesFree: 900 }),
            createVolume(2, { bytesFree: 800 }),
            createVolume(3, { bytesFree: 700 }),
        ];
        ioManagerMock.getWritableVolumes.mockReturnValue(volumes);

        const alternatingSpy = vi.spyOn(planner as any, '_getSortedVolumesByBytesFreeWithAlternatingGroups')
            .mockReturnValue(volumes.slice(0, 2));
        const sizeSpy = vi.spyOn(planner as any, '_getSortedVolumesByBytesFree')
            .mockReturnValue(volumes);

        const plan = planner.generatePlan(1500);

        expect(alternatingSpy).toHaveBeenCalled();
        expect(sizeSpy).toHaveBeenCalled();
        expect(plan.dataVolumes).toEqual([1, 2]);
        expect(plan.parityVolumes).toEqual([3]);

        alternatingSpy.mockRestore();
        sizeSpy.mockRestore();
    });

    it('throws when fallback sorting still lacks enough volumes', () => {
        const volumes = [
            createVolume(1, { bytesFree: 900 }),
            createVolume(2, { bytesFree: 800 }),
            createVolume(3, { bytesFree: 700 }),
        ];
        ioManagerMock.getWritableVolumes.mockReturnValue(volumes);

        const alternatingSpy = vi.spyOn(planner as any, '_getSortedVolumesByBytesFreeWithAlternatingGroups')
            .mockReturnValue(volumes.slice(0, 2));
        const sizeSpy = vi.spyOn(planner as any, '_getSortedVolumesByBytesFree')
            .mockReturnValue(volumes.slice(0, 2));

        expect(() => planner.generatePlan(1500)).toThrow('not enough volumes available for planned slice count');

        alternatingSpy.mockRestore();
        sizeSpy.mockRestore();
    });
});
