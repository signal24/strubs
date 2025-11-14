import { createLogger } from '../../log';
import type { FileObject } from '../file-object';
import { Base } from './base';

export class FileObjectDestroyer extends Base {
    private readonly logger;

    constructor(fileObject: FileObject) {
        super(fileObject);
        this.logger = createLogger(`${this.fileObject.getLoggerPrefix()}:destroyer`);
        void this._instantiateSlices();
    }

    // TODO: standardize destroy vs delete
    async destroy(): Promise<void> {
        const destroyPromises = this._slices.map(async slice => {
            slice.markAsCommitted();
            await slice.delete();
        });

        try {
            await Promise.all(destroyPromises);
        }
        catch (err) {
            this.logger.error('slice encountered error during destroy', err);
        }
    }
}
