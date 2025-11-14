import { promises as fs } from 'fs';

import { createLogger } from '../log';

const log = createLogger('mount-root');

export class MountRootManager {
    constructor(private readonly mountRootPath = '/run/strubs/mounts') {}

    async ensureExists(): Promise<void> {
        log('creating mount root...');

        try {
            await fs.mkdir(this.mountRootPath);
            log('mount root created');
        }
        catch (err: any) {
            if (err?.code === 'EEXIST') {
                log('mount root exists');
                return;
            }
            throw err;
        }
    }
}

export const mountRootManager = new MountRootManager();
