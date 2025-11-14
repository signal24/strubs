declare module '@ronomon/reed-solomon' {
    type ReedSolomonContext = unknown;
    type Callback = (error: Error | null) => void;

    interface ReedSolomonStatic {
        create(dataShards: number, parityShards: number, options?: unknown): ReedSolomonContext;
        encode(
            context: ReedSolomonContext,
            sourcesBits: number | null,
            targetsBits: number | null,
            input: Buffer,
            inputOffset: number,
            inputSize: number,
            output: Buffer,
            outputOffset: number,
            outputSize: number,
            callback: Callback
        ): void;
        search(...args: any[]): unknown;
        XOR(target: Buffer, source: Buffer): void;
    }

    const ReedSolomon: ReedSolomonStatic;
    export = ReedSolomon;
}

declare module 'fuse-native' {
    type FuseCallback = (...args: any[]) => void;

    interface FuseHandlers {
        [key: string]: FuseCallback;
    }

    interface FuseOptions {
        force?: boolean;
        mkdir?: boolean;
        [key: string]: unknown;
    }

    class Fuse {
        static [key: string]: number;
        static ECONNRESET: number;
        static EOPNOTSUPP: number;
        static EISDIR: number;

        constructor(mountPath: string, handlers: FuseHandlers, options?: FuseOptions);
        mount(callback: (err?: Error) => void): void;
        unmount(callback: (err?: Error) => void): void;
    }

    export = Fuse;
}
