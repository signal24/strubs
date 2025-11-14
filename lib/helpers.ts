import util from 'util';

export interface StrubsError extends Error {
    code?: string;
    previousError?: Error;
}

export function createError(code: string, message: string, previousError?: Error): StrubsError {
    const err = new Error(message) as StrubsError;
    err.code = code;
    err.previousError = previousError;
    return err;
}

export function createErrorWithParams(code: string, message: string, params: unknown[], previousError?: Error): StrubsError {
    const formatted = util.format(message, ...params);
    return createError(code, formatted, previousError);
}
