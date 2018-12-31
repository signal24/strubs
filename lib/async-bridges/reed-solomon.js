const promisify = require('util').promisify;
const ReedSolomon = require('@ronomon/reed-solomon');

module.exports = {
    create: ReedSolomon.create,
    encode: promisify(ReedSolomon.encode),
    search: ReedSolomon.search,
    XOR: ReedSolomon.XOR
}