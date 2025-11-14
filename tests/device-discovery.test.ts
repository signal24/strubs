import { beforeEach, describe, expect, it, vi } from 'vitest';

const lsblkMock = vi.fn();

vi.mock('../lib/io/helpers', () => ({
    lsblk: lsblkMock,
    smartctl: vi.fn(),
    formatBytes: vi.fn()
}));

vi.mock('../lib/io/smart-info', () => ({
    smartInfoService: {
        fetch: vi.fn()
    }
}));

describe('listRawBlockDevices', () => {
    let listRawBlockDevices: typeof import('../lib/io/device-discovery').listRawBlockDevices;

    beforeEach(async () => {
        vi.resetModules();
        ({ listRawBlockDevices } = await import('../lib/io/device-discovery'));
        lsblkMock.mockReset();
    });

    it('sanitizes lsblk results to the defined RawBlockDevice fields', async () => {
        lsblkMock.mockResolvedValue({
            blockdevices: [
                {
                    name: 'sda',
                    path: '/dev/sda',
                    type: 'disk',
                    size: '1024',
                    model: 'DiskModel',
                    vendor: 'DiskVendor',
                    serial: 'SER123',
                    pttype: 'gpt',
                    ptuuid: 'PT-UUID',
                    smartInfo: { serial_number: 'SER123' },
                    extraField: 'should be dropped',
                    children: [
                        {
                            type: 'part',
                            name: 'sda1',
                            size: '512',
                            uuid: 'PART-UUID',
                            fstype: 'ext4',
                            mountpoint: '/mnt/data',
                            something: 'else'
                        },
                        {
                            type: 'rom',
                            name: 'sr0',
                            size: '2048'
                        }
                    ]
                },
                {
                    name: 'sr0',
                    path: '/dev/sr0',
                    type: 'rom',
                    size: '2048'
                }
            ]
        });

        const blockDevices = await listRawBlockDevices();

        expect(blockDevices).toEqual([
            {
                name: 'sda',
                path: '/dev/sda',
                type: 'disk',
                size: 1024,
                model: 'DiskModel',
                vendor: 'DiskVendor',
                serial: 'SER123',
                pttype: 'gpt',
                ptuuid: 'PT-UUID',
                smartInfo: { serial_number: 'SER123' },
                children: [
                    {
                        type: 'part',
                        name: 'sda1',
                        path: '/dev/sda1',
                        size: 512,
                        uuid: 'PART-UUID',
                        fstype: 'ext4',
                        mountpoint: '/mnt/data'
                    }
                ]
            }
        ]);
    });
});
