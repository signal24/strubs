import type { ObjectId } from 'mongodb';

export type ObjectIdentifier = string | ObjectId | Buffer | null | undefined;
export type ContainerPath = string | string[];

export interface ContentDocument {
    _id?: ObjectId;
    id?: string;
    containerId?: ObjectId | string | null;
    name: string;
    isContainer?: boolean;
    isFile?: boolean;
    size?: number;
    chunkSize?: number;
    dataVolumes?: number[];
    parityVolumes?: number[];
    unavailableSlices?: number[];
    damagedSlices?: number[];
    md5?: Buffer | null;
    mime?: string | null;
    lastVerifiedAt?: number | null;
    sliceErrors?: Record<string, SliceErrorInfo>;
    [key: string]: any;
}

export type SliceErrorInfo = {
    checksum?: boolean;
    err?: string;
};
