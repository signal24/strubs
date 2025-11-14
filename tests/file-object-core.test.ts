import { EventEmitter } from 'events';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { hydratePlan } from './helpers/plan';

const databaseMock = {
    createObjectRecord: vi.fn(),
    deleteObjectById: vi.fn()
};

const planMock = {
    generatePlan: vi.fn()
};

type WriterInstance = {
    prepare: ReturnType<typeof vi.fn>;
    write: ReturnType<typeof vi.fn>;
    finish: ReturnType<typeof vi.fn>;
    commit: ReturnType<typeof vi.fn>;
    abort: ReturnType<typeof vi.fn>;
    md5: Buffer | null;
};

type ReaderInstance = {
    prepare: ReturnType<typeof vi.fn>;
    setReadRange: ReturnType<typeof vi.fn>;
    readChunk: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
};

type DestroyerInstance = {
    destroy: ReturnType<typeof vi.fn>;
};

const writerInstances: WriterInstance[] = [];
const readerInstances: ReaderInstance[] = [];
const destroyerInstances: DestroyerInstance[] = [];

let nextWriterConfigurator: ((instance: WriterInstance) => void) | null = null;
const configureNextWriter = (fn: (instance: WriterInstance) => void): void => {
    nextWriterConfigurator = fn;
};

const createWriterInstance = (): WriterInstance => ({
    prepare: vi.fn().mockResolvedValue(undefined),
    write: vi.fn().mockResolvedValue(undefined),
    finish: vi.fn().mockResolvedValue(undefined),
    commit: vi.fn().mockResolvedValue(undefined),
    abort: vi.fn().mockResolvedValue(undefined),
    md5: Buffer.from('beef', 'hex')
});

const createReaderInstance = (): ReaderInstance => ({
    prepare: vi.fn().mockResolvedValue(undefined),
    setReadRange: vi.fn(),
    readChunk: vi.fn().mockResolvedValue(Buffer.from('chunk')),
    close: vi.fn().mockResolvedValue(undefined)
});

const createDestroyerInstance = (): DestroyerInstance => ({
    destroy: vi.fn().mockResolvedValue(undefined)
});

vi.mock('stream', () => {
    class BasicEmitter {
        private listeners: Record<string, Array<(...args: unknown[]) => void>> = {};

        on(event: string, listener: (...args: unknown[]) => void): this {
            (this.listeners[event] ||= []).push(listener);
            return this;
        }

        addListener(event: string, listener: (...args: unknown[]) => void): this {
            return this.on(event, listener);
        }

        once(event: string, listener: (...args: unknown[]) => void): this {
            const wrapper = (...args: unknown[]) => {
                this.off(event, wrapper);
                listener(...args);
            };
            return this.on(event, wrapper);
        }

        off(event: string, listener: (...args: unknown[]) => void): this {
            const items = this.listeners[event];
            if (!items) return this;
            this.listeners[event] = items.filter(fn => fn !== listener);
            return this;
        }

        removeListener(event: string, listener: (...args: unknown[]) => void): this {
            return this.off(event, listener);
        }

        removeAllListeners(event?: string): this {
            if (event)
                delete this.listeners[event];
            else
                this.listeners = {};
            return this;
        }

        emit(event: string, ...args: unknown[]): boolean {
            const items = this.listeners[event];
            if (!items || items.length === 0) return false;
            for (const fn of [ ...items ])
                fn(...args);
            return true;
        }
    }

    class MockDuplex extends BasicEmitter {
        push(chunk: any): boolean {
            if (chunk === null) {
                this.emit('end');
                return false;
            }
            this.emit('data', chunk);
            return true;
        }

        destroy(error?: Error | null): this {
            if (error)
                this.emit('error', error);
            this.emit('close');
            return this;
        }

        pipe(): this {
            return this;
        }
    }

    return { Duplex: MockDuplex };
});

vi.mock('../lib/database', () => ({
    database: databaseMock
}));

vi.mock('../lib/io/planner', async () => {
    const actual = await vi.importActual<typeof import('../lib/io/planner')>('../lib/io/planner');
    planMock.generatePlan.mockImplementation(async (size: number) => {
        const plan = {
            fileSize: size,
            chunkSize: 64,
            dataSliceCount: 2,
            paritySliceCount: 1,
            dataVolumes: [1, 2],
            parityVolumes: [3],
            sliceSize: null,
            startChunkDataSize: null,
            standardChunkDataSize: null,
            endChunkDataSize: null,
            standardChunkCountPerSlice: null,
            standardChunkSetOffset: null,
            endChunkSetDataOffset: null
        } as any;
        hydratePlan(plan);
        return plan;
    });
    return {
        ...actual,
        planner: planMock
    };
});

vi.mock('../lib/io/file-object/writer', () => ({
    FileObjectWriter: vi.fn(function (this: WriterInstance) {
        Object.assign(this, createWriterInstance());
        writerInstances.push(this);
        nextWriterConfigurator?.(this);
        nextWriterConfigurator = null;
    })
}));

vi.mock('../lib/io/file-object/reader', () => ({
    FileObjectReader: vi.fn(function (this: ReaderInstance) {
        Object.assign(this, createReaderInstance());
        readerInstances.push(this);
    })
}));

vi.mock('../lib/io/file-object/destroyer', () => ({
    FileObjectDestroyer: vi.fn(function (this: DestroyerInstance) {
        Object.assign(this, createDestroyerInstance());
        destroyerInstances.push(this);
    })
}));

vi.mock('../lib/io/helpers', async () => {
    const actual = await vi.importActual<typeof import('../lib/io/helpers')>('../lib/io/helpers');
    return {
        ...actual,
        generateObjectId: vi.fn(() => Buffer.from('00112233445566778899aabb', 'hex'))
    };
});

vi.mock('../lib/log', () => ({
    createLogger: () => Object.assign(() => {}, { error: () => {} })
}));

const { FileObject } = await import('../lib/io/file-object');

const resetState = (): void => {
    databaseMock.createObjectRecord.mockReset();
    databaseMock.deleteObjectById.mockReset();
    writerInstances.length = 0;
    readerInstances.length = 0;
    destroyerInstances.length = 0;
    nextWriterConfigurator = null;
    (planMock.generatePlan as ReturnType<typeof vi.fn>).mockReset();
    planMock.generatePlan.mockResolvedValue(hydratePlan({
        chunkSize: 32,
        dataSliceCount: 2,
        paritySliceCount: 1,
        dataVolumes: [ 1, 2 ],
        parityVolumes: [ 3 ]
    }));
};

const waitForEvent = <T>(emitter: EventEmitter, event: string): Promise<T> =>
    new Promise(resolve => emitter.once(event, resolve as any));

describe('FileObject', () => {
    beforeEach(() => resetState());

    const createRecord = () => ({
        id: 'abc',
        containerId: null,
        isFile: true,
        name: 'doc.bin',
        size: 8,
        md5: Buffer.from('beef', 'hex'),
        mime: 'application/octet-stream',
        chunkSize: 32,
        dataVolumes: [ 1, 2 ],
        parityVolumes: [ 3 ]
    });

    it('generates metadata using planner and prepares the writer', async () => {
        const object = new FileObject();
        await object.createWithSize(10);
        expect(writerInstances).toHaveLength(1);
        expect(writerInstances[0]?.prepare).toHaveBeenCalled();
        expect(object.id).toBe('00112233445566778899aabb');
        expect(object.dataSliceVolumeIds).toEqual([ 1, 2 ]);
        expect(object.paritySliceVolumeIds).toEqual([ 3 ]);
    });

    it('commits metadata to the database after a successful write', async () => {
        const object = new FileObject();
        await object.createWithSize(4);
        const writer = writerInstances[0];
        object.name = 'cat.jpg';
        object.containerId = 'root';
        object.mime = null;
        writer && (writer.md5 = Buffer.from('ca11ab1e', 'hex'));
        await object.commit();
        expect(writer?.commit).toHaveBeenCalled();
        expect(databaseMock.createObjectRecord).toHaveBeenCalledWith(expect.objectContaining({
            id: object.id,
            containerId: 'root',
            name: 'cat.jpg'
        }));
    });

    it('loads from an existing record and prepares the reader', async () => {
        const object = new FileObject();
        const record = createRecord();
        await object.loadFromRecord(record);
        await object.prepareForRead();
        expect(readerInstances).toHaveLength(1);
        expect(readerInstances[0]?.prepare).toHaveBeenCalled();
        object.setReadRange(0, record.size, true);
        expect(readerInstances[0]?.setReadRange).toHaveBeenCalledWith(0, record.size);
    });

    it('rejects read operations when not in read mode', async () => {
        const object = new FileObject();
        await expect(() => object.setReadRange(0, 1)).toThrow('file object is not in a readable state');
        const errorPromise = waitForEvent<Error>(object, 'error');
        await (object as any)._read();
        await expect(errorPromise).resolves.toBeInstanceOf(Error);
    });

    it('delegates writes and finalization to the writer', async () => {
        const object = new FileObject();
        await object.createWithSize(4);
        const writer = writerInstances[0];
        await new Promise<void>((resolve, reject) => {
            (object as any)._write(Buffer.from('data'), 'buffer', err => err ? reject(err) : resolve());
        });
        expect(writer?.write).toHaveBeenCalled();
        await new Promise<void>((resolve, reject) => {
            (object as any)._final(err => err ? reject(err) : resolve());
        });
        expect(writer?.finish).toHaveBeenCalled();
        expect(object.md5?.toString('hex')).toBe('beef');
    });

    it('reads data through the reader when in read mode', async () => {
        const object = new FileObject();
        const record = createRecord();
        await object.loadFromRecord(record);
        await object.prepareForRead();
        object.setReadRange(0, record.size, true);
        const dataPromise = waitForEvent<Buffer>(object, 'data');
        await (object as any)._read();
        const chunk = await dataPromise;
        expect(chunk.toString()).toBe('chunk');
    });

    it('closes the reader and resets state', async () => {
        const object = new FileObject();
        const record = createRecord();
        await object.loadFromRecord(record);
        await object.prepareForRead();
        await object.close();
        expect(readerInstances[0]?.close).toHaveBeenCalled();
    });

    it('aborts active writes when deleting before persistence', async () => {
        const object = new FileObject();
        await object.createWithSize(4);
        await object.delete();
        expect(writerInstances[0]?.abort).toHaveBeenCalled();
        expect(databaseMock.deleteObjectById).not.toHaveBeenCalled();
    });

    it('destroys stored slices and removes the record when persisted', async () => {
        const object = new FileObject();
        const record = createRecord();
        await object.loadFromRecord(record);
        await object.delete();
        expect(destroyerInstances[0]?.destroy).toHaveBeenCalled();
        expect(databaseMock.deleteObjectById).toHaveBeenCalledWith(record.id);
    });

    it('serializes IO locks to guard concurrent access', async () => {
        const object = new FileObject();
        const firstLock = object.acquireIOLock();
        const secondLock = object.acquireIOLock();
        let secondResolved = false;
        void secondLock.then(() => { secondResolved = true; });
        await firstLock;
        expect(secondResolved).toBe(false);
        object.releaseIOLock();
        await secondLock;
        expect(secondResolved).toBe(true);
    });

    it('rejects create when the planner response is incomplete', async () => {
        planMock.generatePlan.mockResolvedValueOnce({
            chunkSize: undefined,
            dataSliceCount: undefined,
            paritySliceCount: undefined,
            dataVolumes: [],
            parityVolumes: []
        });
        const object = new FileObject();
        await expect(object.createWithSize(1)).rejects.toThrow('plan is incomplete');
    });

    it('aborts slice allocation when writer preparation fails', async () => {
        configureNextWriter(instance => {
            instance.prepare.mockRejectedValueOnce(new Error('prep fail'));
        });
        const failing = new FileObject();
        await expect(failing.createWithSize(4)).rejects.toThrow('failed to create file object');
        expect(writerInstances.at(-1)?.abort).toHaveBeenCalled();
    });

    it('guards stream helpers when not in the proper mode', async () => {
        const object = new FileObject();
        await expect(object.commit()).rejects.toThrow('file object is not in a writable state');
        await new Promise<void>(resolve => {
            (object as any)._write(Buffer.from('x'), 'buffer', err => {
                expect(err?.message).toBe('file object is not in a writable state');
                resolve();
            });
        });
        await new Promise<void>(resolve => {
            (object as any)._final(err => {
                expect(err?.message).toBe('file object is not in a writable state');
                resolve();
            });
        });
        expect(() => object.setReadRange(0, 1)).toThrow('file object is not in a readable state');
        await expect(object.close()).rejects.toThrow('file object is not in a readable state');
        const errorPromise = waitForEvent<Error>(object, 'error');
        await (object as any)._read();
        await expect(errorPromise).resolves.toBeInstanceOf(Error);
    });
});
