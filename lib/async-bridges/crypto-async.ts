import { promisify } from 'util';
import cryptoAsync from '@ronomon/crypto-async';

type HashCallback = (error: Error | undefined, targetSize: number) => void;
type HashWithOffsets = (
    algorithm: string,
    source: Buffer,
    sourceOffset: number,
    sourceSize: number,
    target: Buffer,
    targetOffset: number,
    cb: HashCallback
) => void;

export const cipher = promisify(cryptoAsync.cipher);
export const hash = promisify(cryptoAsync.hash as HashWithOffsets) as (
    algorithm: string,
    source: Buffer,
    sourceOffset: number,
    sourceSize: number,
    target: Buffer,
    targetOffset: number
) => Promise<number>;
export const hmac = promisify(cryptoAsync.hmac);
