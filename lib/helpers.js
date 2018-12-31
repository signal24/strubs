const util = require('util');

function createError(code, message, previousError) {
    let err = new Error(message);
    err.code = code;
    err.previousError = previousError;
    return err;
}

function createErrorWithParams(code, message, params, previousError) {
    params.unshift(message);
    message = util.format.apply(util, params);
    return createError(code, message, previousError);
}

module.exports = {
    createError,
    createErrorWithParams
};