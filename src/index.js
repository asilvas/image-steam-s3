import { storage } from 'image-steam';
import path from 'path';
import fs from 'fs';
import assert from 'assert';
import knox from 'knox';
import once from 'once';
import defaultOptions from './s3-default-options';

const StorageBase = storage.Base;

export default class StorageS3 extends StorageBase
{
  constructor(opts) {
    super(opts);

    this.options = Object.assign({}, defaultOptions, this.options);
    
    if (!this.options.accessKey) {
      throw new Error('StorageS3.accessKey is required');
    }
    
    if (!this.options.secretKey) {
      throw new Error('StorageS3.secretKey is required');
    }
  }

  getOptions(opts) {
    return Object.assign({}, this.options, opts);
  }

  fetch(opts, originalPath, stepsHash, cb) {
    const options = this.getOptions(opts);
    const pathInfo = getPathInfo(originalPath, options);
    if (!pathInfo) {
      return void cb(new Error('Invalid S3 path'));
    }
  
    const imagePath = stepsHash
      ? 'isteam/' + pathInfo.imagePath + '/' + stepsHash
      : pathInfo.imagePath
    ;
  
    let client;
    try {
      client = getClient(pathInfo.bucket, options);
    } catch (ex) {
      return void cb(ex);
    }
  
    var bufs = [];
    client.getFile(imagePath, function(err, res) {
      if (err) {
        return void cb(err);
      }
      var info = Object.assign(
        { path: encodeURIComponent(originalPath), stepsHash: stepsHash },
        getMetaFromHeaders(res.headers)
      );

      res.on('data', function(chunk) {
        bufs.push(chunk);
      });
  
      res.on('end', function() {
        if (res.statusCode !== 200) {
          return void cb(new Error('storage.s3.fetch.error: '
            + res.statusCode + ' for ' + (pathInfo.bucket + '/' + imagePath))
          );
        }
  
        cb(null, info, Buffer.concat(bufs));
      });
    });
  }

  store(opts, originalPath, stepsHash, image, cb) {
    if (!stepsHash) {
      return void cb(new Error('StorageS3: Cannot store an image over the original'));
    }

    const options = this.getOptions(opts);
    const pathInfo = getPathInfo(originalPath, options);
    if (!pathInfo) {
      return void cb(new Error('Invalid S3 path'));
    }
  
    if (!stepsHash) {
      return void cb(new Error('Cannot store an image over the original'));
    }
  
    const imagePath = 'isteam/' + pathInfo.imagePath + '/' + stepsHash;
  
    image.info.stepsHash = stepsHash;
  
    let client;
    try {
      client = getClient(pathInfo.bucket, options);
    } catch (ex) {
      return void cb(ex);
    }
  
    const headers = Object.assign({
        'Content-Type': image.contentType || 'application/octet-stream' // default to binary if unknown
      },
      getHeadersFromMeta(image.info)
    );
  
    client.putBuffer(image.buffer, imagePath, headers, (err, res) => {
      if (err) {
        return void cb(err);
      }
  
      res.resume(); // free up memory since we don't care about body
      
      if (res.statusCode !== 200) {
        return void cb(new Error('storage.s3.store.error: '
            + res.statusCode + ' for ' + (pathInfo.bucket + '/' + imagePath))
        );
      }
  
      cb();
    });
  }

  deleteCache(opts, originalPath, cb) {
    const options = this.getOptions(opts);
    const pathInfo = getPathInfo(originalPath, options);
    if (!pathInfo) {
      return void cb(new Error('Invalid S3 path'));
    }
  
    const imagePath = 'isteam/' + pathInfo.imagePath + '/' + stepsHash;
  
    let client;
    try {
      client = getClient(pathInfo.bucket, options);
    } catch (ex) {
      return void cb(ex);
    }

    const _listAndDelete = (dir, lastKey, cb) => {
      list(client, dir, { maxKeys: 1000, delimiter: '', lastKey }, (err, files, lastKey) => {
        if (err) return void cb(err);

        if (files.length === 0) return void cb();

        client.deleteMultiple(files.map(f => f.Key), (err, res) => {
          if (err) return void cb(err);

          res.resume(); // discard body

          if (res.statusCode !== 200) {
            return void cb(new Error('storage.s3.removeDirectory.error: '
              + res.statusCode + ' for ' + (client.urlBase + '/' + dir))
            );
          }

          if (!lastKey) return cb(); // no more to delete 

          // continue recursive deletions
          _listAndDelete(dir, lastKey, cb);
        });
      });
    };

    _listAndDelete(imagePath, null, cb);    
  }
}

function getClient(bucket, opts) {
  /*
    limitation of knox is one bucket per client... not a huge overhead,
    but may switch s3 interface in future to avoid having to need one
    client for every request... isteam has its own throttling so that
    will help.
   */

  return knox.createClient({
    endpoint: opts.endpoint,
    port: opts.port,
    secure: opts.secure,
    style: opts.style,
    key: opts.accessKey,
    secret: opts.secretKey,
    bucket: bucket
  });
};

function getPathInfo(filePath, options) {
  var firstSlash = filePath.indexOf('/');
  var isBucketInPath = !options.bucket;
  if (firstSlash < 0 && isBucketInPath) {
    return null;
  }

  return {
    bucket: isBucketInPath ? filePath.substr(0, firstSlash) : options.bucket,
    imagePath: filePath.substr(isBucketInPath ? firstSlash + 1 : 0)
  };
}

function getMetaFromHeaders(headers) {
  var info = {};

  var header = headers['x-amz-meta-isteam'];
  if (header) {
    info = JSON.parse(header);
  }

  return info;
}

function getHeadersFromMeta(info) {
  var headers = {
    'x-amz-meta-isteam': JSON.stringify(info)
  };

  return headers;
}

/* supported options:
  client: knox client
  dir: Directory (prefix) to query
  opts: Options object
  opts.lastKey: if requesting beyond maxKeys (paging)
  opts.maxKeys: the max keys to return in one request
  opts.delimiter: can be used to control delimiter of query independent of deepQuery
  cb(err, files, lastKey) - Callback fn
  cb.err: Error if any
  cb.files: An array of files: { Key, LastModified, ETag, Size, ... }
  cb.lastKey: An identifier to permit retrieval of next page of results, ala: 'abc'
*/
function list(client, dir, opts, cb) {
  const params = {
    prefix: dir + ((dir.length === 0 || dir[dir.length - 1] === '/') ? '' : '/'), // prefix must always end with `/` if not root
    delimiter: typeof opts.delimiter === 'string' ? opts.delimiter : ''
  };
  if (opts.lastKey) params.marker = opts.lastKey;
  if (opts.maxKeys) params['max-keys'] = opts.maxKeys;

  cb = once(cb); // bad knox
  client.list(params, (err, data) => {
    data = data || {}; // default in case of error
    if (err) return void cb(err);
    // only return error if not related to key not found (commonly due to bucket deletion)
    if (data.Code && data.Code !== 'NoSuchKey') return cb(new Error(data.Code));

    const files = data.Contents || [];
    const lastKey = data.IsTruncated ? (data.NextMarker || data.Contents[data.Contents.length - 1].Key) : null;

    cb(null, files, lastKey);
  });
}
