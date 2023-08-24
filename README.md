# O-O-O-O-O-O-O-R

Simple graph relay for [O7](https://github.com/dstanesc/O-O-O-O-O-O-O) library. _WIP_

## Usage

Server api.

```ts
const blockStore: BlockStore = memoryBlockStoreFactory();
const linkResolver: LinkResolver = memoryBlockResolverFactory();
const graphRelay = new GraphRelay(blockStore, linkResolver);
const server = graphRelay.start(3000, () => {}); // port 3000
```

Plumbing client api.

```ts
// FIXME - document plumbing client api
```

Basic client api.

```ts
const relayClient = relayClientBasicFactory(
  {
    chunk,
    chunkSize,
    linkCodec,
    valueCodec,
    blockStore,
    incremental: true, // false if unspecified
  },
  {
    baseURL: "http://localhost:3000",
  }
);
const versionStore: VersionStore = ...
const response: BasicPushResponse = await relayClient.push(
  versionStore.versionStoreRoot()
);
const versionStoreId = ...
const { versionStore, graphStore, graph } = await relayClient.pull(
        versionStoreId
);
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
