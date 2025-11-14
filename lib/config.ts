import dotenv from 'dotenv';
import { promises as fs } from 'fs';

import { createLogger } from './log';

dotenv.config();

const log = createLogger('config');

export class Config {
    mongoUrl: string;
    dataSliceCount: number;
    paritySliceCount: number;
    chunkSize = 16384;
    identity: string | null = null;
    identityBuffer: Buffer | null = null;

    constructor() {
        this.mongoUrl = process.env.STRUBS_MONGO_URL || 'mongodb://strubs:strubs@127.0.0.1:27017/strubs?authSource=admin';
        this.dataSliceCount = process.env.STRUBS_DATA_SLICES ? parseInt(process.env.STRUBS_DATA_SLICES, 10) : 4;
        this.paritySliceCount = process.env.STRUBS_PARITY_SLICES ? parseInt(process.env.STRUBS_PARITY_SLICES, 10) : 2;
    }

    async loadIdentity(): Promise<void> {
        log('loading identity');

        const data = await fs.readFile('/var/lib/strubs/identity');

        this.identity = data.toString().trim();
        this.identityBuffer = Buffer.from(this.identity.replace(/[^0-9a-f]/g, ''), 'hex');

        log('loaded identity:', this.identity);
    }
}

export const config = new Config();
