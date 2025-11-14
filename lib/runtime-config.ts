import { database } from './database';

export class RuntimeConfig {
    async get(key: string): Promise<unknown> {
        return database.getRuntimeConfig(key);
    }

    async set(key: string, value: unknown): Promise<void> {
        await database.setRuntimeConfig(key, value);
    }

    async delete(key: string): Promise<void> {
        await database.deleteRuntimeConfig(key);
    }
}

export const runtimeConfig = new RuntimeConfig();
