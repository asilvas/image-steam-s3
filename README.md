# image-steam-blobby
S3 client for [Image Steam](https://github.com/asilvas/node-image-steam).


## Options

```ecmascript 6
import isteamS3 from 'image-steam-s3';

const s3 = new isteamS3({
  endpoint: 's3.amazonaws.com',
  accessKey: 'myAccessKey',
  secretKey: 'mySecretShhh'
});
```

| Option | Type | Default | Info |
| --- | --- | --- | --- |
| endpoint | `string` | `"s3.amazonaws.com"` | Endpoint of S3 service |
| port | `number` | `443` | Non-443 port will auto-default secure to `false` |
| secure | `boolean` | `true` only if port `443` | Override as needed |
| accessKey | `string` | ***required*** | S3 access key |
| secretKey | `string` | ***required*** | S3 secret key |
| style | `string` | `"path"` | May use `virtualHosted` if bucket is not in path |
| bucket | `string` | *optional* | S3 Bucket |


## Usage

Example:

```ecmascript 6
import isteam from 'image-steam';

const options = {
  storage: {
    app: {
      static: {
        driver: 'http',
        endpoint: 'https://some-endpoint.com'
      }
    },
    cache: {
      driverPath: 'image-steam-s3',
      bucket: 'myBucket',
      accessKey: 'myAccessKey',
      secretKey: 'mySecretShh'
    }
  }
}

http.createServer(new isteam.http.Connect(options).getHandler())
  .listen(13337, '127.0.0.1')
;
```
