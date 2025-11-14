export const constants = {
    FILE_HEADER_SIZE: 48,
    CHUNK_HEADER_SIZE: 16,
    CHUNK_HEADER_ALGO: 'md5'
} as const;

export type Constants = typeof constants;
