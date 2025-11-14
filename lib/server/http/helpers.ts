import { database } from '../../database';
import type { ContentDocument } from '../../database';

export class HttpHelpers {
    static async getObjectMeta(path: string): Promise<ContentDocument | null> {
        try {
            let objectMeta: ContentDocument;
            if (path.length === 26 && /^\/\$[0-9a-f]{24}$/i.test(path))
                objectMeta = await database.getObjectById(path.slice(2));
            else
                objectMeta = await database.getObjectByPath(path.slice(1));
            return objectMeta;
        }
        catch (err) {
            if ((err as { code?: string })?.code === 'ENOENT')
                return null;
            else
                throw err;
        }
    }
}
