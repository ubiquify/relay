#!/usr/bin/env node
import { BlockStore, memoryBlockStoreFactory } from "@dstanesc/o-o-o-o-o-o-o";
import {
  LinkResolver,
  memoryBlockResolverFactory,
  getCertificate,
  createGraphRelay,
} from "./index";

const blockStore: BlockStore = memoryBlockStoreFactory();
const linkResolver: LinkResolver = memoryBlockResolverFactory();
const httpsPort = 3003;
const graphRelay = createGraphRelay(blockStore, linkResolver);
graphRelay.startHttps(httpsPort, getCertificate(), () => {
  console.log(`GraphRelay listening on https://localhost:${httpsPort}`);
});
// const httpPort = 3001;
// graphRelay.startHttp(httpPort, () => {
//   console.log(`GraphRelay listening on http://localhost:${httpPort}`);
// });
