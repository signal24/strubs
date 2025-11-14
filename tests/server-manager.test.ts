import { beforeEach, describe, expect, it, vi } from 'vitest';

const loggerFactory = vi.fn(() => {
    const logger = vi.fn();
    logger.error = vi.fn();
    return logger;
});

vi.mock('../lib/log', () => ({
    createLogger: loggerFactory,
}));

const httpStartMock = vi.fn();
const httpStopMock = vi.fn();
const fuseStartMock = vi.fn();
const fuseStopMock = vi.fn();

const HttpServerMock = vi.fn(function () {
    return {
        start: httpStartMock,
        stop: httpStopMock,
    };
});

const FuseServerMock = vi.fn(function () {
    return {
        start: fuseStartMock,
        stop: fuseStopMock,
    };
});

vi.mock('../lib/server/http/server', () => ({
    HttpServer: HttpServerMock,
}));

vi.mock('../lib/server/fuse/server', () => ({
    FuseServer: FuseServerMock,
}));

describe('serverManager', () => {
    beforeEach(() => {
        vi.resetModules();
        httpStartMock.mockClear();
        httpStopMock.mockClear();
        fuseStartMock.mockClear();
        fuseStopMock.mockClear();
        HttpServerMock.mockClear();
        FuseServerMock.mockClear();
    });

    it('starts both HTTP and FUSE servers', async () => {
        const { serverManager } = await import('../lib/server/manager');

        await serverManager.start();

        expect(HttpServerMock).toHaveBeenCalledTimes(1);
        expect(FuseServerMock).toHaveBeenCalledTimes(1);
        expect(httpStartMock).toHaveBeenCalledTimes(1);
        expect(fuseStartMock).toHaveBeenCalledTimes(1);
    });

    it('stops all managed servers', async () => {
        const { serverManager } = await import('../lib/server/manager');

        await serverManager.start();
        await serverManager.stop();

        expect(httpStopMock).toHaveBeenCalledTimes(1);
        expect(fuseStopMock).toHaveBeenCalledTimes(1);
    });

    it('does not restart servers when start is called twice', async () => {
        const { serverManager } = await import('../lib/server/manager');

        await serverManager.start();
        await serverManager.start();

        expect(HttpServerMock).toHaveBeenCalledTimes(1);
        expect(FuseServerMock).toHaveBeenCalledTimes(1);
    });
});
