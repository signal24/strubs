import { describe, expect, it } from 'vitest';

import { formatBytes, generateObjectId } from '../lib/io/helpers';

describe('io/helpers generateObjectId', () => {
    it('creates unique object ids with incrementing counters', () => {
        const first = generateObjectId();
        const second = generateObjectId();

        expect(first).toBeInstanceOf(Buffer);
        expect(first.length).toBe(12);
        expect(second.length).toBe(12);

        const firstHostPid = first.slice(4, 9).toString('hex');
        const secondHostPid = second.slice(4, 9).toString('hex');
        expect(firstHostPid).toBe(secondHostPid);

        const firstCounter = first.readUIntBE(9, 3);
        const secondCounter = second.readUIntBE(9, 3);
        const diff = (secondCounter - firstCounter + 0x1000000) % 0x1000000;
        expect(diff).toBe(1);
    });
});

describe('io/helpers formatBytes', () => {
    it('formats values using the largest appropriate unit', () => {
        expect(formatBytes(512)).toBe('512 b');
        expect(formatBytes(1536)).toBe('1.50 KB');
        expect(formatBytes(10 * 1024 * 1024)).toBe('10.00 MB');
        expect(formatBytes(3 * 1024 * 1024 * 1024)).toBe('3.00 GB');
        expect(formatBytes(5 * 1024 * 1024 * 1024 * 1024)).toBe('5.00 TB');
    });
});
