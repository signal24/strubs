const spawn = require('child_process').spawn;

module.exports = function(path, args, cb) {
    let proc = spawn(path, args);

    let out = '';
    proc.stdout.on('data', data => {
        out += data;
    });

    proc.on('error', err => {
        cb(err);
    });

    proc.on('exit', code => {
        cb(null, code, out);
    });
};