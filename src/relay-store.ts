import {
  Block,
  BlockStore,
  MemoryBlockStore,
  LinkCodec,
  linkCodecFactory,
  valueCodecFactory,
  ValueCodec,
  graphPackerFactory,
  memoryBlockStoreFactory,
  versionStoreFactory,
  VersionStore,
  chunkerFactory,
  Link,
} from "@ubiquify/core";

import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node";

const linkCodec: LinkCodec = linkCodecFactory();
const valueCodec: ValueCodec = valueCodecFactory();
const {
  packVersionStore,
  restoreSingleIndex: restoreVersionStore,
  packGraphVersion,
  packRootIndex,
  packRandomBlocks,
  restoreGraphVersion,
  restoreRandomBlocks,
} = graphPackerFactory(linkCodec);

export interface LinkResolver {
  resolve(id: string): Promise<any>;
  update(id: string, cid: any): Promise<void>;
  contains(id: string): Promise<boolean>;
}

export interface Version {
  major: number;
  minor: number;
  patch: number;
}

export const memoryBlockResolverFactory = (): LinkResolver => {
  const store: Map<string, any> = new Map();
  return {
    resolve: async (id: string): Promise<any> => {
      return store.get(id);
    },
    update: async (id: string, cid: any): Promise<void> => {
      store.set(id, cid);
    },
    contains: async (id: string): Promise<boolean> => {
      return store.has(id);
    },
  };
};

export interface RelayStore {
  pushVersionStore(
    chunk: (buffer: Uint8Array) => Uint32Array,
    bundleBytes: Uint8Array
  ): Promise<{ storeRoot: Link; versionRoot: Link }>;
  pullVersionStore(
    chunk: (buffer: Uint8Array) => Uint32Array,
    id: string
  ): Promise<Uint8Array | undefined>;
  pushGraphVersion(bundleBytes: Uint8Array): Promise<{ versionRoot: Link }>;
  pullGraphVersion(versionRootString: string): Promise<Uint8Array | undefined>;
  pullRootIndex(versionRootString: string): Promise<Uint8Array | undefined>;
  pushRandomBlocks(bundleBytes: Uint8Array): Promise<{ blockCount: number }>;
  pullRandomBlocks(linkStrings: string[]): Promise<Uint8Array | undefined>;
  getProtocolVersion(): Version;
}

export const relayStoreFactory = (
  blockStore: BlockStore,
  resolver: LinkResolver
): RelayStore => {
  const storeVersion: Version = {
    major: 0,
    minor: 1,
    patch: 0,
  };
  const pushVersionStore = async (
    chunk: (buffer: Uint8Array) => Uint32Array,
    bundleBytes: Uint8Array
  ): Promise<{ storeRoot: Link; versionRoot: Link }> => {
    const transientStore: MemoryBlockStore = memoryBlockStoreFactory();
    const {
      root: versionStoreIncomingRoot,
      index,
      blocks,
    } = await restoreVersionStore(bundleBytes, transientStore);
    const versionStoreIncoming: VersionStore = await versionStoreFactory({
      storeRoot: versionStoreIncomingRoot,
      chunk,
      linkCodec,
      valueCodec,
      blockStore: transientStore,
    });
    const versionStoreId = versionStoreIncoming.id();
    if (await resolver.contains(versionStoreId)) {
      const versionStoreExistingRoot = await resolver.resolve(versionStoreId);
      if (
        versionStoreExistingRoot.toString() !==
        versionStoreIncomingRoot.toString()
      ) {
        // scope all merge relevant blocks into the transient store
        const versionStoreExistingBundle: Block = await packVersionStore(
          versionStoreExistingRoot,
          blockStore,
          chunk,
          valueCodec
        );
        const { root: storeRootExisting } = await restoreVersionStore(
          versionStoreExistingBundle.bytes,
          transientStore
        );
        const versionStoreExisting: VersionStore = await versionStoreFactory({
          storeRoot: versionStoreExistingRoot,
          chunk,
          linkCodec,
          valueCodec,
          blockStore: transientStore,
        });
        const graphStoreBundleExisting: Block = await packGraphVersion(
          versionStoreExisting.currentRoot(),
          blockStore
        );
        const { root: versionRootExisting } = await restoreGraphVersion(
          graphStoreBundleExisting.bytes,
          transientStore
        );
        const graphStoreBundleIncoming: Block = await packGraphVersion(
          versionStoreIncoming.currentRoot(),
          blockStore
        );
        const { root: versionRootIncoming } = await restoreGraphVersion(
          graphStoreBundleIncoming.bytes,
          transientStore
        );
        const {
          root: mergedRoot,
          index: mergedIndex,
          blocks: mergedBlocks,
        } = await versionStoreIncoming.mergeVersions(versionStoreExisting);
        const versionRoot = versionStoreIncoming.currentRoot();
        const storeRoot = versionStoreIncoming.versionStoreRoot();
        await transientStore.push(blockStore);
        await resolver.update(versionStoreId, storeRoot);
        return { storeRoot, versionRoot };
      } else {
        // version already exists
        const versionRoot = versionStoreIncoming.currentRoot();
        return { storeRoot: versionStoreIncomingRoot, versionRoot };
      }
    } else {
      // first time pushing this version store
      resolver.update(versionStoreId, versionStoreIncomingRoot);
      await transientStore.push(blockStore);
      const versionRoot = versionStoreIncoming.currentRoot();
      return { storeRoot: versionStoreIncomingRoot, versionRoot };
    }
  };

  const pullVersionStore = async (
    chunk: (buffer: Uint8Array) => Uint32Array,
    id: string
  ): Promise<Uint8Array | undefined> => {
    const versionStoreRoot = await resolver.resolve(id);
    if (versionStoreRoot !== undefined) {
      const bundle: Block = await packVersionStore(
        versionStoreRoot,
        blockStore,
        chunk,
        valueCodec
      );
      return bundle.bytes;
    } else {
      return undefined;
    }
  };

  const pushGraphVersion = async (
    bundleBytes: Uint8Array
  ): Promise<{ versionRoot: Link }> => {
    const memoryStore: MemoryBlockStore = memoryBlockStoreFactory();
    const { root: versionRoot } = await restoreGraphVersion(
      bundleBytes,
      memoryStore
    );
    await memoryStore.push(blockStore);
    return { versionRoot };
  };

  const pullGraphVersion = async (
    versionRootString: string
  ): Promise<Uint8Array | undefined> => {
    const root = linkCodec.parseString(versionRootString);
    if ((await blockStore.get(root)) !== undefined) {
      const bundle: Block = await packGraphVersion(root, blockStore);
      return bundle.bytes;
    } else {
      return undefined;
    }
  };

  const pullRootIndex = async (
    versionRootString: string
  ): Promise<Uint8Array | undefined> => {
    const root = linkCodec.parseString(versionRootString);
    if ((await blockStore.get(root)) !== undefined) {
      const bundle: Block = await packRootIndex(root, blockStore);
      return bundle.bytes;
    } else {
      return undefined;
    }
  };

  const pushRandomBlocks = async (
    bundleBytes: Uint8Array
  ): Promise<{ blockCount: number }> => {
    const memoryStore: MemoryBlockStore = memoryBlockStoreFactory();
    const blocks = await restoreRandomBlocks(bundleBytes, memoryStore);
    await memoryStore.push(blockStore);
    return { blockCount: blocks.length };
  };

  const pullRandomBlocks = async (
    linkStrings: string[]
  ): Promise<Uint8Array | undefined> => {
    const blocks: Block[] = [];
    try {
      const links: Link[] = linkStrings.map((linkString) =>
        linkCodec.parseString(linkString)
      );
      for (const link of links) {
        const bytes = await blockStore.get(link);
        blocks.push({ cid: link, bytes });
      }
      const bundle = await packRandomBlocks(blocks);
      return bundle.bytes;
    } catch (error) {
      console.error(error);
      return undefined;
    }
  };

  const getProtocolVersion = (): Version => {
    return storeVersion;
  };
  return {
    pushVersionStore,
    pullVersionStore,
    pushGraphVersion,
    pullGraphVersion,
    pullRootIndex,
    pushRandomBlocks,
    pullRandomBlocks,
    getProtocolVersion,
  };
};
