import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const databaseMock = {
    getObjectById: vi.fn(),
    getObjectByPath: vi.fn()
};

vi.mock('../lib/database', () => ({
    database: databaseMock
}));

let HttpHelpers: typeof import('../lib/server/http/helpers').HttpHelpers;

beforeAll(async () => {
    ({ HttpHelpers } = await import('../lib/server/http/helpers'));
});

beforeEach(() => {
    vi.clearAllMocks();
});

describe('HttpHelpers.getObjectMeta', () => {
    it('looks up objects by id when the path matches the special pattern', async () => {
        const object = { id: 'abc123' };
        databaseMock.getObjectById.mockResolvedValue(object);

        const meta = await HttpHelpers.getObjectMeta('/$0123456789abcdef01234567');

        expect(meta).toBe(object);
        expect(databaseMock.getObjectById).toHaveBeenCalledWith('0123456789abcdef01234567');
        expect(databaseMock.getObjectByPath).not.toHaveBeenCalled();
    });

    it('falls back to path lookups for all other URLs', async () => {
        const object = { id: 'def456' };
        databaseMock.getObjectByPath.mockResolvedValue(object);

        const meta = await HttpHelpers.getObjectMeta('/photos/2024/cats.png');

        expect(meta).toBe(object);
        expect(databaseMock.getObjectByPath).toHaveBeenCalledWith('photos/2024/cats.png');
        expect(databaseMock.getObjectById).not.toHaveBeenCalled();
    });

    it('returns null when the database reports a missing object', async () => {
        const missingError = Object.assign(new Error('missing'), { code: 'ENOENT' });
        databaseMock.getObjectByPath.mockRejectedValue(missingError);

        const meta = await HttpHelpers.getObjectMeta('/missing/object');

        expect(meta).toBeNull();
    });

    it('rethrows unexpected database errors', async () => {
        const boom = new Error('boom');
        databaseMock.getObjectByPath.mockRejectedValue(boom);

        await expect(HttpHelpers.getObjectMeta('/whatever')).rejects.toBe(boom);
    });
});
