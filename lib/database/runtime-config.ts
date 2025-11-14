import type { Collection } from 'mongodb';

export type RuntimeConfigEntry = {
    key: string;
    value: unknown;
};

export class RuntimeConfigRepository {
    constructor(private readonly collection: Collection<RuntimeConfigEntry>) {}

    async get(key: string): Promise<unknown> {
        const entry = await this.collection.findOne({ key });
        return entry?.value ?? null;
    }

    async set(key: string, value: unknown): Promise<void> {
        await this.collection.updateOne(
            { key },
            { $set: { value } },
            { upsert: true }
        );
    }

    async delete(key: string): Promise<void> {
        await this.collection.deleteOne({ key });
    }
}
