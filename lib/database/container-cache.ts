type CacheKey = string;

interface CacheEntry {
    id: string;
    parentId: string | null;
    name: string;
    lastUsed: number;
}

const DEFAULT_MAX_ENTRIES = 2048;
const DEFAULT_TTL_MS = 5 * 60 * 1000;

export class ContainerCache {
    private readonly entries = new Map<CacheKey, CacheEntry>();

    constructor(
        private readonly maxEntries = DEFAULT_MAX_ENTRIES,
        private readonly ttlMs = DEFAULT_TTL_MS
    ) {}

    get(name: string, parentId: string | null): string | null {
        const key = this.toKey(name, parentId);
        const entry = this.entries.get(key);
        if (!entry)
            return null;

        if (this.isExpired(entry)) {
            this.entries.delete(key);
            return null;
        }

        entry.lastUsed = Date.now();
        return entry.id;
    }

    remember(id: string, name: string, parentId: string | null): void {
        const key = this.toKey(name, parentId);
        this.entries.set(key, {
            id,
            name,
            parentId,
            lastUsed: Date.now()
        });
        this.evictOverflow();
    }

    sweep(): void {
        const now = Date.now();
        for (const [key, entry] of this.entries.entries()) {
            if (now - entry.lastUsed > this.ttlMs)
                this.entries.delete(key);
        }
        this.evictOverflow();
    }

    private toKey(name: string, parentId: string | null): CacheKey {
        return `${parentId ?? 'root'}::${name}`;
    }

    private isExpired(entry: CacheEntry): boolean {
        return Date.now() - entry.lastUsed > this.ttlMs;
    }

    private evictOverflow(): void {
        if (this.entries.size <= this.maxEntries)
            return;

        const entries = Array.from(this.entries.entries());
        entries.sort((a, b) => a[1].lastUsed - b[1].lastUsed);
        const removeCount = this.entries.size - this.maxEntries;
        for (let index = 0; index < removeCount; index++)
            this.entries.delete(entries[index]![0]);
    }
}
