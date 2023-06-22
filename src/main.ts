import { BlockStore, memoryBlockStoreFactory } from "@dstanesc/o-o-o-o-o-o-o";
import {
  GraphRelay,
  LinkResolver,
  memoryBlockResolverFactory,
} from "./graph-relay";

// Usage example, memory only persistence
const port = 3000;
const blockStore: BlockStore = memoryBlockStoreFactory();
const linkResolver: LinkResolver = memoryBlockResolverFactory();
const graphRelay = new GraphRelay(blockStore, linkResolver);
graphRelay.start(port, () => {
  console.log(`GraphRelay started on port ${port}`);
});
