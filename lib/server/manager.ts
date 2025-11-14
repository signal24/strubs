import { HttpServer } from './http/server';
import { FuseServer } from './fuse/server';
import { createLogger } from '../log';

const log = createLogger('server-manager');

type ServerLifecycle = {
    start: () => void | Promise<void>;
    stop?: () => void | Promise<void>;
};

type ServerManagerDeps = {
    createHttpServer: () => ServerLifecycle;
    createFuseServer: () => ServerLifecycle;
};

const defaultDeps: ServerManagerDeps = {
    createHttpServer: () => new HttpServer(),
    createFuseServer: () => new FuseServer()
};

export class ServerManager {
    private readonly deps: ServerManagerDeps;
    private servers: ServerLifecycle[] = [];
    private starting: Promise<void> | null = null;
    private stopping: Promise<void> | null = null;

    constructor(deps?: Partial<ServerManagerDeps>) {
        this.deps = { ...defaultDeps, ...deps };
    }

    async start(): Promise<void> {
        if (this.servers.length)
            return;
        if (this.starting)
            return this.starting;
        this.starting = this._startServers();
        try {
            await this.starting;
        }
        finally {
            this.starting = null;
        }
    }

    private async _startServers(): Promise<void> {
        log('starting server manager');
        const httpServer = this.deps.createHttpServer();
        const fuseServer = this.deps.createFuseServer();
        this.servers = [httpServer, fuseServer];
        for (const server of this.servers)
            await Promise.resolve(server.start());
    }

    async stop(): Promise<void> {
        if (!this.servers.length)
            return;
        if (this.stopping)
            return this.stopping;

        this.stopping = (async () => {
            for (const server of [...this.servers].reverse()) {
                if (server.stop)
                    await Promise.resolve(server.stop());
            }
            this.servers = [];
        })();

        try {
            await this.stopping;
        }
        finally {
            this.stopping = null;
        }
    }
}

export const serverManager = new ServerManager();
