const crypto = require('crypto');

const database = require('../../database');
const FileObject = require('../../io/file-object');
const Log = require('../../log');

// TODO: move MD5 into FileObject

class ObjectPutRequest {
    constructor(requestId, request, response) {
        this.requestId = requestId;
        this.request = request;
        this.response = response;
    }

    async process() {
        return new Promise(async (resolve, reject) => {
            let bytesExpected = parseInt(this.request.headers['content-length']);
            let bytesReceived = 0;

            let object = new FileObject();
            try {
                await object.createWithSize(bytesExpected);
            }
            catch (err) {
                return reject(err);
            }

            /*=====================*/
            /* request callbacks    */
            /*=====================*/

            // as we receive data
            this.request.on('data', data => {
                // increment the number of bytes we've received, to later ensure the length matches the Content-Length header
                bytesReceived += data.length;

                // write the data to the object. if we get "false" back, it means
                // there's backpressure, and we should pause the request's inbound
                // data stream to prevent memory from filling up. object will emit
                // a 'drain' event later and we will resume the request then.
                object.write(data) || this.request.pause();
            });

            // if the request is aborted
            this.request.on('aborted', async () => {
                // log the abort
                this.log('client aborted request to store object at ' + this.request.url);

                // delete the object and do nothing else
                object.delete();
            });

            // when the request finishes uploading data
            this.request.on('end', async () => {
                // TODO: is this even possible or will "end" never get called if the full data size isn't uploaded?
                // TODO: should we just use "pipe" instead?
                // make sure we got as much data as we were expecting
                if (bytesExpected != bytesReceived) {
                    object.delete();
                    this.response.writeHead(455, 'Length Mismatch', {
                        'X-Received-Bytes': bytesReceived,
                        'X-Expected-Bytes': bytesExpected
                    });
                    this.response.end('length mismatch');
                    return resolve();
                }

                // end the stream. when it finishes processing, we'll continue down in the finish callback
                object.end();
            });

            // when the request encounters an error in-flight
            // TODO: figure out what causes this?
            this.request.on('error', err => {
                console.log('request error!', err);
                object.delete();
                // reject(err);???
            });


            /*=====================*/
            /* object callbacks    */
            /*=====================*/

            // when the object catches up on writing data out to disk, we can resume
            // the request's input stream to continue receiving data from the client
            object.on('drain', () => {
                this.request.resume();
            });

            // if the object encounters an error, we need to bail on the request
            // rejecting the promise will result in a 500 being returned by the server
            object.on('error', err => {
                console.log('object error!', err);
                reject(err);
            });

            // when the object finishes dumping all its data
            object.on('finish', async () => {
                // get the object's MD5 as a string
                let md5Hex = object.md5.toString('hex');

                // first, if the client provided an MD5 header and it doesn't match what the object computed,
                // something must've been corrupted along the way, and we need to bail now
                if (this.request.headers['content-md5'] && this.request.headers['content-md5'] != md5Hex) {
                    object.delete();
                    this.response.writeHead(456, 'MD5 Mismatch', {
                        'X-Received-MD5': md5Hex
                    });
                    this.response.end('MD5 mismatch');
                    return resolve();
                }

                // if a content-type was specified, let's set a mime type on the object
                if (this.request.headers['content-type'])
                    object.mime = this.request.headers['content-type'];

                // split the path and file name
                let pathComponents = this.request.url.replace(/^\//, '').split('/');
                let fileName = pathComponents.pop();
                
                // set up the container tree
                if (pathComponents.length) {
                    let containerId = await database.getOrCreateContainer(pathComponents);
                    object.containerId = containerId;
                }

                // set the name on the object
                object.name = fileName;
                
                // and now commit it. this will flush it to disk, move all the files
                // into place, and then write the object to the database
                try {
                    await object.commit();
                }
                catch (err) {
                    return this.request.aborted || reject(err);
                }

                // sweet. we can tell the client we are done!
                this.response.writeHead(201, 'Created', {
                    'content-md5': md5Hex,
                    'x-object-id': object.id,
                    'x-container-id': object.containerId
                });
                this.response.end();

                // and we're done with this request
                resolve();
            });
        });
    }

    log() {
        this.log = Log('http-server:request-' + this.requestId);
        this.log.apply(this.log, arguments);
    }
}

module.exports = ObjectPutRequest;