import { describe, expect, it } from 'vitest';

import { buildVolumeIdentityBuffer } from '../lib/io/volume-identity';

describe('buildVolumeIdentityBuffer', () => {
    it('builds a complete identity buffer using provided metadata', () => {
        const identitySeed = Buffer.from('00112233445566778899aabbccddeeff', 'hex');
        const buffer = buildVolumeIdentityBuffer({
            volumeId: 5,
            volumeUuid: 'aabb-ccdd-1122-3344',
            identityBuffer: identitySeed,
            status: 'X'
        });

        expect(buffer).toHaveLength(41);
        expect(buffer.readUInt8(0)).toBe(0x1F);
        expect(buffer.readUInt8(4)).toBe(1);
        expect(buffer.subarray(5, 21).equals(identitySeed)).toBe(true);
        expect(buffer.readUInt8(37)).toBe(5);
        expect(buffer.toString('utf8', 38, 39)).toBe('X');
    });
});
