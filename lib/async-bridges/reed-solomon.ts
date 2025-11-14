import { promisify } from 'util';
import ReedSolomon from '@ronomon/reed-solomon';

export const create = ReedSolomon.create;
export const encode = promisify(ReedSolomon.encode);
export const search = ReedSolomon.search;
export const XOR = ReedSolomon.XOR;
