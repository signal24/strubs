const fs = require('fs').promises;

const config = require('./config');
const database = require('./database');
const ioManager = require('./io/manager');
const server = require('./server/manager');
const log = require('./log')('core');

class Core {
    constructor() {
        this.start();
    }

    async start() {
        try {
            log('starting up STRUBS...');

            await config.load();
            await this.createRunDirectory();
            await database.connect();
            await ioManager.init();
            await server.init();

            log('STRUBS started.');
        }

        catch(err) {
            log.error('STRUBS startup failed:', err);
            process.exit(-1);
        }
    }

    createRunDirectory() {
        return new Promise(async (resolve, reject) => {
            log('creating runtime directory...');

            try {
                await fs.mkdir('/var/run/strubs');
            }
            catch (err) {
                if (err.code == 'EEXIST') {
                    log('runtime directory exists');
                    return resolve();
                }

                return reject(err);
            }

            log('runtime directory created');
            resolve();
        });
    }
}

module.exports = Core;