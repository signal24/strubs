import crypto from 'crypto';
import os from 'os';

import { spawnHelper } from '../helpers/spawn';

const hostname = os.hostname();
const hostId = crypto.createHash('md5').update(hostname).digest().slice(13);

const objectIdPid = process.pid & 0xff;
let objectIdCounter = 0;

// NOTE: this follows the Mongo spec, but it's for our objects, so it's here.
// we need IDs for our files long before the database assigns them.
export function generateObjectId(): Buffer {
    const objectIndex = ++objectIdCounter & 0xffffff;
    const time = Math.floor(Date.now() / 1000);

    const result = Buffer.allocUnsafe(12);
    result.writeInt32BE(time, 0);
    hostId.copy(result, 4);
    result.writeInt16BE(objectIdPid, 7);
    result.writeIntBE(objectIndex, 9, 3);

    return result;
}

export async function lsblk(additionalParams?: string[]): Promise<any> {
    const params = ['-OJb'];

    if (additionalParams)
        params.push(...additionalParams);

    const { code, stdout } = await spawnHelper('lsblk', params);

    if (code !== 0)
        throw new Error('lsblk exited with code ' + code);

    return JSON.parse(stdout);
}

export async function smartctl(...args: string[]): Promise<any> {
    args.unshift('--json=c');

    const { code, stdout } = await spawnHelper('smartctl', args);

    if (code !== 0)
        throw new Error('smartctl exited with code ' + code);

    return JSON.parse(stdout);
}

export async function mount(blockPath: string, mountPath: string, fsType: string, options?: Record<string, string | number | boolean>): Promise<void> {
    const params = [ blockPath, '-t', fsType, mountPath ];

    if (options) {
        const optionsStr = Object.entries(options)
            .map(([key, value]) => `${key}=${value}`)
            .join(',');
        params.splice(3, 0, '-o', optionsStr);
    }

    const { code, stdout } = await spawnHelper('mount', params);

    if (code !== 0)
        throw new Error('mount exited with code ' + code + (stdout ? ': ' + stdout : ''));
}

export async function unmount(mountPath: string): Promise<void> {
    const { code, stdout } = await spawnHelper('umount', [ mountPath ]);

    if (code !== 0)
        throw new Error('umount exited with code ' + code + (stdout ? ': ' + stdout : ''));
}

export function formatBytes(bytes: number): string {
    if (bytes >= 1099511627776)
        return (bytes / 1099511627776).toFixed(2) + ' TB';
    if (bytes >= 1073741824)
        return (bytes / 1073741824).toFixed(2) + ' GB';
    if (bytes >= 1048576)
        return (bytes / 1048576).toFixed(2) + ' MB';
    if (bytes >= 1024)
        return (bytes / 1024).toFixed(2) + ' KB';
    return bytes + ' b';
}
