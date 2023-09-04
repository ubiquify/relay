# O-O-O-O-O-O-O-R

Simple graph relay for [O7](https://github.com/dstanesc/O-O-O-O-O-O-O) library.

## Usage

Server api.

```ts
const blockStore: BlockStore = memoryBlockStoreFactory();
const linkResolver: LinkResolver = memoryBlockResolverFactory();
const httpsPort = 3003;
const graphRelay = createGraphRelay(blockStore, linkResolver);
graphRelay.startHttps(3000, getCertificate(), () => {
  console.log(`GraphRelay listening on https://localhost:${httpsPort}`);
});
const httpPort = 3001;
graphRelay.startHttp(httpPort, () => {
  console.log(`GraphRelay listening on http://localhost:${httpPort}`);
});
```

## SSL

The relay expects two files in the `ssl` folder:

- `server.key` - Private key.
- `server.crt` - Certificate.

A self signed certificate can be generated in linux with `openssl`:

```sh
cd ssl/
openssl req -nodes -new -x509 -keyout server.key -out server.crt
```

## Build

```sh
npm run clean
npm install
npm run build
npm run test
```

## Usage

```sh
npm start
```

## Licenses

Licensed under either [Apache 2.0](http://opensource.org/licenses/MIT) or [MIT](http://opensource.org/licenses/MIT) at your option.
