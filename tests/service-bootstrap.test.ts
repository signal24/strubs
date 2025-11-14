import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const createLoggerMock = vi.fn(() => {
    const logger = vi.fn();
    logger.error = vi.fn();
    return logger;
});

vi.mock('../lib/log', () => ({
    createLogger: createLoggerMock,
}));

const coreConstructor = vi.fn();
const coreStartMock = vi.fn().mockResolvedValue(undefined);
const coreStopMock = vi.fn().mockResolvedValue(undefined);
class CoreStub {
    constructor() {
        coreConstructor();
    }

    start = coreStartMock;
    stop = coreStopMock;
}

vi.mock('../lib/core', () => ({
    Core: CoreStub,
}));

type Listener = (...args: unknown[]) => void;

const captureListeners = (event: string): Set<Listener> => new Set(process.listeners(event));

let initialUncaught: Set<Listener>;
let initialUnhandled: Set<Listener>;
let initialSigint: Set<Listener>;
let initialSigterm: Set<Listener>;
const addedListeners: Array<{ event: string; listener: Listener }> = [];
const originalExit = process.exit;
const exitMock = vi.fn();

describe('service bootstrap', () => {
    beforeEach(() => {
        vi.resetModules();
        coreConstructor.mockClear();
        coreStartMock.mockClear();
        coreStopMock.mockClear();
        exitMock.mockClear();
        // @ts-expect-error override for tests
        process.exit = exitMock;
        createLoggerMock.mockClear();
        initialUncaught = captureListeners('uncaughtException');
        initialUnhandled = captureListeners('unhandledRejection');
        initialSigint = captureListeners('SIGINT');
        initialSigterm = captureListeners('SIGTERM');
    });

    afterEach(() => {
        process.exit = originalExit;
        addedListeners.forEach(({ event, listener }) => {
            process.removeListener(event, listener);
        });
        addedListeners.length = 0;
    });

    it('registers fatal handlers and boots the core', async () => {
        await import('../service');

        expect(coreConstructor).toHaveBeenCalledTimes(1);
        expect(createLoggerMock).toHaveBeenCalledWith('bootstrap');
        expect(coreStartMock).toHaveBeenCalledTimes(1);

        const newUncaught = process.listeners('uncaughtException').filter(listener => !initialUncaught.has(listener));
        const newUnhandled = process.listeners('unhandledRejection').filter(listener => !initialUnhandled.has(listener));
        const newSigint = process.listeners('SIGINT').filter(listener => !initialSigint.has(listener));
        const newSigterm = process.listeners('SIGTERM').filter(listener => !initialSigterm.has(listener));

        newUncaught.forEach(listener => addedListeners.push({ event: 'uncaughtException', listener }));
        newUnhandled.forEach(listener => addedListeners.push({ event: 'unhandledRejection', listener }));
        newSigint.forEach(listener => addedListeners.push({ event: 'SIGINT', listener }));
        newSigterm.forEach(listener => addedListeners.push({ event: 'SIGTERM', listener }));

        expect(newUncaught).toHaveLength(1);
        expect(newUnhandled).toHaveLength(1);
        expect(newSigint).toHaveLength(1);
        expect(newSigterm).toHaveLength(1);
    });

    it('stops the core when receiving SIGINT', async () => {
        await import('../service');
        const listeners = process.listeners('SIGINT').filter(listener => !initialSigint.has(listener));
        expect(listeners).toHaveLength(1);
        await listeners[0]?.();
        expect(coreStopMock).toHaveBeenCalledTimes(1);
        expect(exitMock).toHaveBeenCalledWith(0);
    });

    it('stops the core when receiving SIGTERM', async () => {
        await import('../service');
        const listeners = process.listeners('SIGTERM').filter(listener => !initialSigterm.has(listener));
        expect(listeners).toHaveLength(1);
        await listeners[0]?.();
        expect(coreStopMock).toHaveBeenCalledTimes(1);
        expect(exitMock).toHaveBeenCalledWith(0);
    });
});
