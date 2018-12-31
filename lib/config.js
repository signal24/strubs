const fs = require('fs').promises;

const log = require('./log')('config');

class Config {
    constructor() {
        this.identity = null;
        this.identityBuffer = null;
    }

    load(cb) {
        return new Promise(async (resolve, reject) => {
            log('loading identity');

            let data = await fs.readFile('/var/lib/strubs/identity');

            this.identity = String(data).trim();
            this.identityBuffer = Buffer.from(this.identity.replace(/[^0-9a-f]/g, ''), 'hex');

            log('loaded identity:', this.identity);
            resolve();
        });
    }
}

module.exports = new Config();