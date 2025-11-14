import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { redMock } = vi.hoisted(() => ({
    redMock: vi.fn((message: string) => `colored:${message}`)
}));

vi.mock('colors', () => ({
    default: {
        red: redMock
    }
}));

import { createLogger } from '../lib/log';

describe('createLogger', () => {
    let logSpy: ReturnType<typeof vi.spyOn>;
    let errorSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));
        logSpy = vi.spyOn(console, 'log').mockImplementation(() => undefined);
        errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        redMock.mockClear();
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('logs formatted info messages with additional arguments', () => {
        const logger = createLogger('test');
        const payload = { hello: 'world' };
        logger('hello', payload);

        expect(logSpy).toHaveBeenCalledWith('[2024-01-01T00:00:00.000Z] [test] hello', payload);
    });

    it('ignores log calls without arguments', () => {
        const logger = createLogger('noop');
        logger();
        logger.error();
        expect(logSpy).not.toHaveBeenCalled();
        expect(errorSpy).not.toHaveBeenCalled();
    });

    it('logs errors with colored prefix and stack traces', () => {
        const logger = createLogger('errors');
        const err = new Error('boom');
        err.stack = 'STACK';

        logger.error('oops', err);

        expect(redMock).toHaveBeenCalledWith('[2024-01-01T00:00:00.000Z] [errors] oops');
        expect(logSpy).toHaveBeenCalledWith('STACK');
        expect(errorSpy).toHaveBeenCalledWith('colored:[2024-01-01T00:00:00.000Z] [errors] oops', err);
    });

    it('does not attempt to log stacks for non-error arguments', () => {
        const logger = createLogger('errors');
        logger.error('oops', 'details');
        expect(logSpy).not.toHaveBeenCalled();
    });
});
