export interface VolumeIdentityParams {
    volumeId: number;
    volumeUuid: string;
    status?: string;
    identityBuffer: Buffer;
}

export function buildVolumeIdentityBuffer(params: VolumeIdentityParams): Buffer {
    const { volumeId, volumeUuid, status = 'O', identityBuffer } = params;

    if (!identityBuffer)
        throw new Error('STRUBS identity buffer is not configured');

    const buffer = Buffer.alloc(41);

    buffer.writeUInt8(0x1F, 0);
    buffer.writeUInt8(0xFB, 1);
    buffer.writeUInt8(0x01, 2);
    buffer.writeUInt8(0xFB, 3);

    buffer.writeUInt8(1, 4);

    identityBuffer.copy(buffer, 5);

    Buffer.from(volumeUuid.replace(/[^0-9a-f]/g, ''), 'hex').copy(buffer, 21);
    buffer.writeUInt8(volumeId, 37);
    buffer.write(status, 38, 1);
    buffer.writeUInt8(0x19, 39);
    buffer.writeUInt8(0xFB, 40);
    return buffer;
}
