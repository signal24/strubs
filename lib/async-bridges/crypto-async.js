const promisify = require('util').promisify;
const cryptoAsync = require('@ronomon/crypto-async');

module.exports = {
    cipher: promisify(cryptoAsync.cipher),
    hash: promisify(cryptoAsync.hash),
    hmac: promisify(cryptoAsync.hmac)
}