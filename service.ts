import { Core } from './lib/core';
import { createLogger } from './lib/log';
import { ioShutdown } from './lib/io/io-shutdown';

const log = createLogger('bootstrap');

const handleFatal = (err: unknown, context: string): void => {
    log.error(`!! ${context}`, err);
    log.error('terminating');
    process.exit(-2);
};

process.on('uncaughtException', err => handleFatal(err, 'an uncaught exception has occurred'));
process.on('unhandledRejection', err => handleFatal(err as Error, 'an uncaught promise rejection has occurred'));

const core = new Core();

const start = async (): Promise<void> => {
    try {
        await core.start();
    }
    catch (err) {
        log.error('failed to start STRUBS core', err);
        process.exit(-1);
    }
};

void start();

const handleShutdown = async (signal: NodeJS.Signals): Promise<void> => {
    log('received %s, stopping core...', signal);
    try {
        await core.stop();
        process.exit(0);
    }
    catch (err) {
        log.error('failed to stop cleanly', err);
        process.exit(1);
    }
};

const registerSignalHandler = (signal: NodeJS.Signals): void => {
    process.on(signal, () => {
        ioShutdown.abort(signal);
        void handleShutdown(signal);
    });
};

registerSignalHandler('SIGINT');
registerSignalHandler('SIGTERM');
