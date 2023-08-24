import { Request, Response } from "express";
import express from "express";
import * as http from "http";
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
} from "@dstanesc/o-o-o-o-o-o-o";

import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node";

const linkCodec: LinkCodec = linkCodecFactory();
const valueCodec: ValueCodec = valueCodecFactory();
const {
  packVersionStore,
  restoreSingleIndex: restoreVersionStore,
  packGraphVersion,
  packRootIndex,
  restoreGraphVersion,
  restoreRandomBlocks,
} = graphPackerFactory(linkCodec);

interface LinkResolver {
  resolve(id: string): Promise<any>;
  update(id: string, cid: any): Promise<void>;
  contains(id: string): Promise<boolean>;
}

class MemoryBlockResolver implements LinkResolver {
  private store: Map<string, any>;

  constructor() {
    this.store = new Map();
  }

  public async resolve(id: string): Promise<any> {
    return this.store.get(id);
  }

  public async update(id: string, cid: any): Promise<void> {
    this.store.set(id, cid);
  }

  public async contains(id: string): Promise<boolean> {
    return this.store.has(id);
  }
}

const memoryBlockResolverFactory = (): LinkResolver => {
  return new MemoryBlockResolver();
};

interface GraphRelayVersion {
  major: number;
  minor: number;
  patch: number;
}

class GraphRelay {
  private version: GraphRelayVersion = {
    major: 0,
    minor: 0,
    patch: 23,
  };
  private blockStore: BlockStore;
  private resolver: LinkResolver;
  private app: express.Express;
  private server: http.Server;

  constructor(blockStore: BlockStore, resolver: LinkResolver) {
    this.blockStore = blockStore;
    this.resolver = resolver;
    this.app = express();
  }

  public httpServer(): http.Server {
    return this.server;
  }

  public async handlePushVersionStoreRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const chunkSize = parseInt(req.query.chunkSize as string, 10);
    const { chunk } = chunkerFactory(chunkSize, compute_chunks);
    const bytes = new Uint8Array(req.body);
    try {
      const info: { storeRoot: Link; versionRoot: Link } =
        await this.pushVersionStore(chunk, bytes);
      res.json({
        storeRoot: info.storeRoot.toString(),
        versionRoot: info.versionRoot.toString(),
      });
    } catch (error) {
      console.error("Error handling PushVersionStore request:", error);
      res.sendStatus(500);
    }
  }

  public async handlePushGraphVersionRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const bytes = new Uint8Array(req.body);
    try {
      const info: { versionRoot: Link } = await this.pushGraphVersion(bytes);
      res.json({
        versionRoot: info.versionRoot.toString(),
      });
    } catch (error) {
      console.error("Error handling PushGraphVersion request:", error);
      res.sendStatus(500);
    }
  }

  public async handlePullVersionStoreRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const id = req.query.id as string;
    const chunkSize = req.query.chunkSize as string;
    const { chunk } = chunkerFactory(parseInt(chunkSize), compute_chunks);
    try {
      const bytes = await this.pullVersionStore(chunk, id);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullVersionStore request:", error);
      res.sendStatus(500);
    }
  }

  public async handlePullGraphVersionRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const id = req.query.id as string;
    try {
      const bytes = await this.pullGraphVersion(id);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullGraphVersion request:", error);
      res.sendStatus(500);
    }
  }

  public async handlePullRootIndexRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const id = req.query.id as string;
    try {
      const bytes = await this.pullRootIndex(id);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullRootIndex request:", error);
      res.sendStatus(500);
    }
  }

  public async handlePushBlocksRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const bytes = new Uint8Array(req.body);
    try {
      const info: { blockCount: number } = await this.pushRandomBlocks(bytes);
      res.json({
        blockCount: info.blockCount,
      });
    } catch (error) {
      console.error("Error handling PushBlock request:", error);
      res.sendStatus(500);
    }
  }

  public async handleVersionStoreResolveRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    const id = req.query.id as string;
    try {
      const result = await this.resolver.resolve(id);
      res.status(200).json(result);
    } catch (error) {
      console.error("Error handling RESOLVE request:", error);
      res.sendStatus(500);
    }
  }

  public async handleProtocolVersionRequest(
    req: Request,
    res: Response
  ): Promise<void> {
    try {
      res.status(200).json(this.version);
    } catch (error) {
      console.error("Error handling PROTOCOL VERSION request:", error);
      res.sendStatus(500);
    }
  }

  public start(port: number, callback: () => void): http.Server {
    // Enable CORS for all origins
    this.app.use((req, res, next) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type");
      next();
    });
    this.app.use(
      express.raw({ type: "application/octet-stream", limit: "1024mb" })
    );
    this.app.put("/store/push", (req: Request, res: Response) =>
      this.handlePushVersionStoreRequest(req, res)
    );
    this.app.get("/store/pull", (req: Request, res: Response) =>
      this.handlePullVersionStoreRequest(req, res)
    );
    this.app.get("/store/resolve", (req: Request, res: Response) =>
      this.handleVersionStoreResolveRequest(req, res)
    );
    this.app.put("/graph/version/push", (req: Request, res: Response) =>
      this.handlePushGraphVersionRequest(req, res)
    );
    this.app.get("/graph/version/pull", (req: Request, res: Response) =>
      this.handlePullGraphVersionRequest(req, res)
    );
    this.app.get("/graph/index/pull", (req: Request, res: Response) =>
      this.handlePullRootIndexRequest(req, res)
    );
    this.app.put("/blocks/push", (req: Request, res: Response) =>
      this.handlePushBlocksRequest(req, res)
    );
    this.app.get("/protocol/version", (req: Request, res: Response) =>
      this.handleProtocolVersionRequest(req, res)
    );
    this.server = this.app.listen(port, callback);
    return this.server;
  }

  public stop(callback: () => void): void {
    this.server.close(callback);
  }

  public async pushVersionStore(
    chunk: (buffer: Uint8Array) => Uint32Array,
    bundleBytes: Uint8Array
  ): Promise<{ storeRoot: Link; versionRoot: Link }> {
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
    if (await this.resolver.contains(versionStoreId)) {
      const versionStoreExistingRoot = await this.resolver.resolve(
        versionStoreId
      );
      if (
        versionStoreExistingRoot.toString() !==
        versionStoreIncomingRoot.toString()
      ) {
        // scope all merge relevant blocks into the transient store
        const versionStoreExistingBundle: Block = await packVersionStore(
          versionStoreExistingRoot,
          this.blockStore,
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
          this.blockStore
        );
        const { root: versionRootExisting } = await restoreGraphVersion(
          graphStoreBundleExisting.bytes,
          transientStore
        );
        const graphStoreBundleIncoming: Block = await packGraphVersion(
          versionStoreIncoming.currentRoot(),
          this.blockStore
        );
        const { root: versionRootIncoming } = await restoreGraphVersion(
          graphStoreBundleIncoming.bytes,
          transientStore
        );
        const {
          root: mergedRoot,
          index: mergedIndex,
          blocks: mergedBlocks,
        } = await versionStoreExisting.mergeVersions(versionStoreIncoming);
        await transientStore.push(this.blockStore);
        const versionRoot = versionStoreExisting.currentRoot();
        const storeRoot = versionStoreExisting.versionStoreRoot();
        await this.resolver.update(versionStoreId, storeRoot);
        return { storeRoot, versionRoot };
      } else {
        // version already exists
        const versionRoot = versionStoreIncoming.currentRoot();
        return { storeRoot: versionStoreIncomingRoot, versionRoot };
      }
    } else {
      // first time pushing this version store
      this.resolver.update(versionStoreId, versionStoreIncomingRoot);
      await transientStore.push(this.blockStore);
      const versionRoot = versionStoreIncoming.currentRoot();
      return { storeRoot: versionStoreIncomingRoot, versionRoot };
    }
  }

  public async pullVersionStore(
    chunk: (buffer: Uint8Array) => Uint32Array,
    id: string
  ): Promise<Uint8Array | undefined> {
    const versionStoreRoot = await this.resolver.resolve(id);
    if (versionStoreRoot !== undefined) {
      const bundle: Block = await packVersionStore(
        versionStoreRoot,
        this.blockStore,
        chunk,
        valueCodec
      );
      return bundle.bytes;
    } else {
      return undefined;
    }
  }

  public async pushGraphVersion(
    bundleBytes: Uint8Array
  ): Promise<{ versionRoot: Link }> {
    const memoryStore: MemoryBlockStore = memoryBlockStoreFactory();
    const { root: versionRoot } = await restoreGraphVersion(
      bundleBytes,
      memoryStore
    );
    await memoryStore.push(this.blockStore);
    return { versionRoot };
  }

  public async pullGraphVersion(
    versionRootString: string
  ): Promise<Uint8Array | undefined> {
    const root = linkCodec.parseString(versionRootString);
    if ((await this.blockStore.get(root)) !== undefined) {
      const bundle: Block = await packGraphVersion(root, this.blockStore);
      return bundle.bytes;
    } else {
      return undefined;
    }
  }

  public async pullRootIndex(
    versionRootString: string
  ): Promise<Uint8Array | undefined> {
    const root = linkCodec.parseString(versionRootString);
    if ((await this.blockStore.get(root)) !== undefined) {
      const bundle: Block = await packRootIndex(root, this.blockStore);
      return bundle.bytes;
    } else {
      return undefined;
    }
  }

  public async pushRandomBlocks(
    bundleBytes: Uint8Array
  ): Promise<{ blockCount: number }> {
    const memoryStore: MemoryBlockStore = memoryBlockStoreFactory();
    const blocks = await restoreRandomBlocks(bundleBytes, memoryStore);
    await memoryStore.push(this.blockStore);
    return { blockCount: blocks.length };
  }
}

export { GraphRelay, GraphRelayVersion, LinkResolver, memoryBlockResolverFactory };
