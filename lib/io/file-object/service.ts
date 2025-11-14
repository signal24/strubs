import { FileObject, type FileObjectDependencies, type StoredObjectRecord } from '../file-object';

type FileObjectFactory = (deps?: Partial<FileObjectDependencies>) => FileObject;

type FileObjectServiceDeps = {
    createFileObject: FileObjectFactory;
};

type LoadOptions = {
    requestId?: string;
};

type OpenForReadOptions = LoadOptions;

const defaultDeps: FileObjectServiceDeps = {
    createFileObject: (deps?: Partial<FileObjectDependencies>) => new FileObject(deps)
};

export class FileObjectService {
    private readonly deps: FileObjectServiceDeps;

    constructor(deps?: Partial<FileObjectServiceDeps>) {
        this.deps = { ...defaultDeps, ...deps };
    }

    async createWritable(size: number, options?: LoadOptions): Promise<FileObject> {
        const object = this.deps.createFileObject();
        await object.createWithSize(size);
        this.applyRequestContext(object, options);
        return object;
    }

    async load(record: StoredObjectRecord, options?: LoadOptions): Promise<FileObject> {
        const object = this.deps.createFileObject();
        await object.loadFromRecord(record);
        this.applyRequestContext(object, options);
        return object;
    }

    async openForRead(record: StoredObjectRecord, options?: OpenForReadOptions): Promise<FileObject> {
        const object = await this.load(record, options);
        await object.prepareForRead();
        return object;
    }

    async loadForDelete(record: StoredObjectRecord, options?: LoadOptions): Promise<FileObject> {
        return this.load(record, options);
    }

    private applyRequestContext(object: FileObject, options?: LoadOptions): void {
        if (options?.requestId)
            object.setRequestId(options.requestId);
    }
}

export const fileObjectService = new FileObjectService();
