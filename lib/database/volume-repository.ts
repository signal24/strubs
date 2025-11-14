import type { Collection } from 'mongodb';

export type VolumeVerifyErrors = {
    checksum: number;
    total: number;
};

export class VolumeRepository {
    constructor(private readonly collection: Collection<any>) {}

    async getVolumes(): Promise<any[]> {
        return this.collection.find({}).toArray();
    }

    async createVolume(volume: any): Promise<void> {
        await this.collection.insertOne(volume);
    }

    async deleteVolume(id: number): Promise<void> {
        await this.collection.deleteOne({ id });
    }

    async softDeleteVolume(id: number): Promise<void> {
        await this.collection.updateOne(
            { id },
            { $set: { enabled: false, is_deleted: true } }
        );
    }

    async updateVolumeFlags(id: number, changes: { isEnabled?: boolean; isReadOnly?: boolean; isDeleted?: boolean }): Promise<void> {
        const set: Record<string, unknown> = {};
        if (changes.isEnabled !== undefined)
            set.enabled = changes.isEnabled;
        if (changes.isReadOnly !== undefined)
            set.read_only = changes.isReadOnly;
        if (changes.isDeleted !== undefined)
            set.is_deleted = changes.isDeleted;
        if (!Object.keys(set).length)
            return;
        await this.collection.updateOne({ id }, { $set: set });
    }

    async setVerifyErrors(id: number, errors: VolumeVerifyErrors | null): Promise<void> {
        if (errors) {
            await this.collection.updateOne({ id }, { $set: { verifyErrors: errors } });
            return;
        }
        await this.collection.updateOne({ id }, { $unset: { verifyErrors: '' } });
    }
}
