const Fuse = require('fuse-native');
const fs = require('fs');
const database = require('../../database');
const FileObject = require('../../io/file-object');
const log = require('../../log')('fuse-server');

class FuseServer {
    constructor() {
        this.mountPath = '/var/run/strubs/data';

        this._fdCache = {};
    }

    async start() {
        log('mounting at ' + this.mountPath + ' ...');

        let opts = {
            force: true,
            mkdir: true
        };

        let handlers = {};
        let fns = Reflect.ownKeys(Reflect.getPrototypeOf(this));
        for (let fn of fns)
            if (/^fuse_/.test(fn))
                handlers[fn.substr(5)] = this[fn].bind(this);

        this.fuse = new Fuse(this.mountPath, handlers, opts);
        this.fuse.mount(err => {
            if (err)
                log('failed to mount:', err);
            else
                log('mounted');
        });

        /*
        process.on('SIGINT', () => {
            log('unmounting...');
            fuse.unmount(this.mountPath, err => {
                if (err)
                    log.error('failed to unmount:', err);
                else
                    log('unmounted');
            });
        });
        */
    }

    // async fuse_init() {
    //     log('init');
    // }

    // async fuse_access(path, mode, cb) {
    //     log('access', path, mode);
    //     cb(0);
    // }

    // async fuse_statfs(path, cb) {
    //     log('statfs', path);
    //     cb(0, {

    //     });
    // }

    async fuse_getattr(path, cb) {
        log('getattr', path);

        if (path == '/')
            return this._fuse_getattrForDirectory(cb);

        try {
            path = path.substr(1);

            let object;
            if (path.substr(0, 1) == '$')
                object = await database.getObjectById(path.substr(1));
            else
                object = await database.getObjectByPath(path);

            if (object.isContainer)
                return this._fuse_getattrForDirectory(cb);
            else
                return this._fuse_getattrForObject(object, cb);
        }
        catch (err) {
            cb(fuse[err.code] || Fuse.ECONNRESET);
        }
    }

    _fuse_getattrForDirectory(cb) {
        cb(0, {
            mtime: new Date(),
            atime: new Date(),
            ctime: new Date(),
            // nlink: 1,
            size: 100,
            mode: 0o40755,
            uid: process.getuid(),
            gid: process.getgid()
        });
    }

    _fuse_getattrForObject(object, cb) {
        let ts = database.getTimestampFromId(object.id);

        cb(0, {
            mtime: new Date(ts),
            atime: new Date(ts),
            ctime: new Date(ts),
            size: object.size,
            mode: 0o100644,
            uid: process.getuid(),
            gid: process.getgid()
        });
    }

    // async fuse_fgetattr(path, fd, cb) {
    //     log('fgetattr', path, fd);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_flush(path, fd, cb) {
    //     log('flush', path, fd);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_fsync(path, fd, datasync, cb) {
    //     log('fsync', path, fd, datasync);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_fsyncdir(path, fd, datasync, cb) {
    //     log('fsyncdir', path, fd, datasync);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    async fuse_readdir(path, cb) {
        log('readdir', path);

        try {
            let objects = await database.getObjectsInContainerPath(path.substr(1));
            objects = objects.map(object => object.name);
            return cb(0, objects);
        }
        catch (err) {
            cb(Fuse[err.code] || Fuse.ECONNRESET);
        }

        // cb(Fuse.EOPNOTSUPP);
    }

    // async fuse_truncate(path, size, cb) {
    //     log('truncate', path, size);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_ftruncate(path, fd, size, cb) {
    //     log('ftruncate', path, fd, size);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_readlink(path, cb) {
    //     log('readlink', path);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_chown(path, uid, gid, cb) {
    //     log('chown', path, uid, gid);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_chmod(path, mode, cb) {
    //     log('chmod', path, mode);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_mknod(path, mode, dev, cb) {
    //     log('mknod', path, mode, dev);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_setxattr(path, name, buffer, length, offset, flags, cb) {
    //     log('setxattr', path, name, buffer, length, offset, flags);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_getxattr(path, name, buffer, length, offset, cb) {
    //     log('getxattr', path, name, buffer, length, offset);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_listxattr(path, buffer, length) {
    //     log('flush', path, fd);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_removexattr(path, name, cb) {
    //     log('removexattr', path, name);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    async fuse_open(path, flags, cb) {
        log('open', path, flags);

        try {
            path = path.substr(1);

            let object;
            if (path.substr(0, 1) == '$')
                object = await database.getObjectById(path.substr(1));
            else
                object = await database.getObjectByPath(path);

            if (object.isContainer) return cb(Fuse.EISDIR);

            flags = flags & 3;
            if (flags === 1) return cb(Fuse.EROFS);
            if (flags !== 0) return cb(Fuse.EPERM);

            let fd = 0;
            while (this._fdCache[fd])
                fd++;

            let fileObject = new FileObject();
            this._fdCache[fd] = fileObject;

            await fileObject.loadFromRecord(object);
            await fileObject.prepareForRead();

            cb(0, fd);
        }

        catch (err) {
            cb(fuse[err.code] || Fuse.ECONNRESET);
        }

        // cb(0, 42);
        // cb(Fuse.EOPNOTSUPP);
    }

    // async fuse_opendir(path, flags, cb) {
    //     log('opendir', path, flags);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    async fuse_read(path, fd, buffer, length, position, cb) {
        // log('read', path, fd, length, position);

        let object = this._fdCache[fd];
        let endPosition = Math.min(object.size, position + length);

        if (position == endPosition)
            return cb(0);

        object.setReadRange(position, endPosition);

        let bufferOffset = 0;
        let bytesRemaining = endPosition - position;

        object.on('data', data => {
            data.copy(buffer, bufferOffset, 0, data.length);
            bufferOffset += data.length;
            bytesRemaining -= data.length;

            if (bytesRemaining == 0) {
                object.removeAllListeners('data');
                cb(bufferOffset);
            }
        });

        // cb(Fuse.EOPNOTSUPP);
    }

    // async fuse_write(path, fd, buffer, length, position, cb) {
    //     log('write', path, fd, buffer, length, position);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    async fuse_release(path, fd, cb) {
        log('release', path, fd);

        let object = this._fdCache[fd];
        object.close();

        delete this._fdCache[fd];

        cb(0);
        // cb(Fuse.EOPNOTSUPP);
    }

    // async fuse_releasedir(path, fd, cb) {
    //     log('releasedir', path, fd);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_create(path, mode, cb) {
    //     log('create', path, mode);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_utimens(path, atime, mtime, cb) {
    //     log('utimens', path, atime, mtime);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_unlink(path, cb) {
    //     log('unlink', path);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_rename(src, dest, cb) {
    //     log('rename', src, dest);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_link(src, dest, cb) {
    //     log('link', src, dest);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_symlink(src, dest, cb) {
    //     log('symlink', src, dest);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_mkdir(path, mode, cb) {
    //     log('mkdir', path, mode);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_rmdir(path, cb) {
    //     log('rmdir', path);
    //     cb(Fuse.EOPNOTSUPP);
    // }

    // async fuse_destroy(cb) {
    //     log('destroy');
    //     cb(Fuse.EOPNOTSUPP);
    // }
}

module.exports = FuseServer;
