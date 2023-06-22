import axios, { AxiosInstance, AxiosResponse } from "axios";
import {
  LinkResolver,
  GraphRelay,
  memoryBlockResolverFactory,
} from "../graph-relay";
import {
  Block,
  BlockStore,
  Graph,
  GraphStore,
  LinkCodec,
  MemoryBlockStore,
  ValueCodec,
  VersionStore,
  chunkerFactory,
  graphPackerFactory,
  graphStoreFactory,
  linkCodecFactory,
  memoryBlockStoreFactory,
  valueCodecFactory,
  versionStoreFactory,
} from "@dstanesc/o-o-o-o-o-o-o";
import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node";

const chunkSize = 512;
const { chunk } = chunkerFactory(chunkSize, compute_chunks);
const linkCodec: LinkCodec = linkCodecFactory();
const valueCodec: ValueCodec = valueCodecFactory();
const {
  packVersionStore,
  restoreSingleIndex: restoreVersionStore,
  packGraphVersion,
  restoreGraphVersion,
} = graphPackerFactory(linkCodec);

enum ObjectTypes {
  FOLDER = 1,
  FILE = 2,
}
enum RlshpTypes {
  CONTAINS = 1,
}
enum PropTypes {
  META = 1,
  DATA = 2,
}
enum KeyTypes {
  NAME = 1,
  CONTENT = 2,
}

describe("BlockService", () => {
  let relayBlockStore: BlockStore;
  let linkResolver: LinkResolver;
  let server: any;
  let graphRelay: GraphRelay;
  let httpClient: AxiosInstance;

  beforeAll((done) => {
    relayBlockStore = memoryBlockStoreFactory();
    linkResolver = memoryBlockResolverFactory();
    graphRelay = new GraphRelay(relayBlockStore, linkResolver);
    server = graphRelay.start(3000, done); // Start the server
    httpClient = axios.create({
      baseURL: "http://localhost:3000",
    });
  });

  afterAll((done) => {
    graphRelay.stop(done); // Stop the server
  });

  describe("the graph relay", () => {
    let versionStoreId = "";
    it("should record complete graph history", async () => {
      const blockStore: MemoryBlockStore = memoryBlockStoreFactory();
      const versionStore: VersionStore = await versionStoreFactory({
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
      });
      const graphStore: GraphStore = graphStoreFactory({
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
      });
      const graph = new Graph(versionStore, graphStore);
      const tx = graph.tx();
      await tx.start();
      const v1 = tx.addVertex(ObjectTypes.FOLDER);
      const v2 = tx.addVertex(ObjectTypes.FOLDER);
      const v3 = tx.addVertex(ObjectTypes.FILE);
      const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS);
      const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS);
      await tx.addVertexProp(v1, KeyTypes.NAME, "root-folder", PropTypes.META);
      await tx.addVertexProp(
        v2,
        KeyTypes.NAME,
        "nested-folder",
        PropTypes.META
      );
      await tx.addVertexProp(v3, KeyTypes.NAME, "nested-file", PropTypes.META);
      await tx.addVertexProp(
        v2,
        KeyTypes.CONTENT,
        "hello world from v2",
        PropTypes.DATA
      );
      await tx.addVertexProp(
        v3,
        KeyTypes.CONTENT,
        "hello world from v3",
        PropTypes.DATA
      );
      const { root: original } = await tx.commit({});
      const bundle: Block = await packVersionStore(
        versionStore.versionStoreRoot(),
        blockStore,
        chunk,
        valueCodec
      );
      /**
       * Post version store bits
       */
      const response = await pushStoreBundle(
        httpClient,
        chunkSize,
        bundle.bytes
      );
      versionStoreId = versionStore.id();
      const { versionRoot } = response;
      expect(versionRoot).toEqual(
        "bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue"
      );
      const graphVersionBundle: Block = await packGraphVersion(
        original,
        blockStore
      );
      /**
       * Post graph version  bits
       */
      const response1 = await pushGraphVersionBundle(
        httpClient,
        graphVersionBundle.bytes
      );
      const { versionRoot: versionRoot1 } = response1;
      expect(versionRoot1).toEqual(versionRoot);
    });

    it("should return the version store bundle with pull", async () => {
      const bytes = await pullStoreBundle(
        httpClient,
        chunkSize,
        versionStoreId
      );
      expect(bytes).toBeDefined();
      const memoryStore: BlockStore = memoryBlockStoreFactory();
      const { root: versionStoreRoot } = await restoreVersionStore(
        bytes,
        memoryStore
      );
      const versionStore: VersionStore = await versionStoreFactory({
        storeRoot: versionStoreRoot,
        chunk,
        linkCodec,
        valueCodec,
        blockStore: memoryStore,
      });
      expect(versionStore.id()).toEqual(versionStoreId);
      expect(versionStore.currentRoot().toString()).toEqual(
        "bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue"
      );
    });

    it("should return the graph version bundle with pull", async () => {
      const bytes = await pullGraphVersionBundle(
        httpClient,
        "bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue"
      );
      expect(bytes).toBeDefined();
      const memoryStore: BlockStore = memoryBlockStoreFactory();
      const { root: graphVersionRoot } = await restoreGraphVersion(
        bytes,
        memoryStore
      );
      expect(graphVersionRoot.toString()).toEqual(
        "bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue"
      );
    });

    it("should update bundle when new version pushed", async () => {
      const bytes = await pullStoreBundle(
        httpClient,
        chunkSize,
        versionStoreId
      );
      expect(bytes).toBeDefined();
      const memoryStore: BlockStore = memoryBlockStoreFactory();
      const { root: versionStoreRoot } = await restoreVersionStore(
        bytes,
        memoryStore
      );
      const versionStore: VersionStore = await versionStoreFactory({
        storeRoot: versionStoreRoot,
        chunk,
        linkCodec,
        valueCodec,
        blockStore: memoryStore,
      });
      const graphStore = graphStoreFactory({
        chunk,
        linkCodec,
        valueCodec,
        blockStore: memoryStore,
      });
      const versionRoot = versionStore.currentRoot();

      const bytes2 = await pullGraphVersionBundle(
        httpClient,
        versionRoot.toString()
      );
      expect(bytes2).toBeDefined();
      const { root: restoredVersionRoot } = await restoreGraphVersion(
        bytes2,
        memoryStore
      );

      expect(restoredVersionRoot.toString()).toEqual(versionRoot.toString());
      const g1 = new Graph(versionStore, graphStore);
      const tx1 = g1.tx();
      await tx1.start();
      const v10 = await tx1.getVertex(0);
      const v11 = tx1.addVertex(ObjectTypes.FILE);
      const e11 = await tx1.addEdge(v10, v11, RlshpTypes.CONTAINS);
      await tx1.addVertexProp(
        v11,
        KeyTypes.NAME,
        "nested-file-user-1",
        PropTypes.META
      );
      await tx1.addVertexProp(
        v11,
        KeyTypes.CONTENT,
        "hello world from v11",
        PropTypes.DATA
      );
      const { root: first } = await tx1.commit({});
      const graphVersionBundle: Block = await packGraphVersion(
        first,
        memoryStore
      );
      const responseGraphVersionPush = await pushGraphVersionBundle(
        httpClient,
        graphVersionBundle.bytes
      );
      const { versionRoot: versionRootPushed } = responseGraphVersionPush;

      expect(versionRootPushed).toEqual(first.toString());

      const bundle: Block = await packVersionStore(
        versionStore.versionStoreRoot(),
        memoryStore,
        chunk,
        valueCodec
      );
      const response = await pushStoreBundle(
        httpClient,
        chunkSize,
        bundle.bytes
      );
      versionStoreId = versionStore.id();
      const { versionRoot: versionRootMerged, storeRoot: storeRootMerged } =
        response;
      console.log("versionRoot", versionRootMerged);
      console.log("storeRoot", storeRootMerged);
    });
  });
});

async function pushStoreBundle(
  httpClient: any,
  chunkSize: number,
  bytes: Uint8Array
): Promise<any> {
  const response = await httpClient.put("/store/push", bytes.buffer, {
    params: {
      chunkSize: chunkSize,
    },
    headers: {
      "Content-Type": "application/octet-stream",
    },
  });
  return response.data;
}

async function pullStoreBundle(
  httpClient: any,
  chunkSize: number,
  id: string
): Promise<Uint8Array | undefined> {
  const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
    "/store/pull",
    {
      responseType: "arraybuffer",
      params: {
        chunkSize: chunkSize,
        id: id,
      },
    }
  );
  if (response.data) {
    const bytes = new Uint8Array(response.data);
    return bytes;
  } else return undefined;
}

async function pushGraphVersionBundle(
  httpClient: any,
  bytes: Uint8Array
): Promise<any> {
  const response = await httpClient.put("/graph/version/push", bytes.buffer, {
    headers: {
      "Content-Type": "application/octet-stream",
    },
  });
  return response.data;
}

async function pullGraphVersionBundle(
  httpClient: any,
  id: string
): Promise<Uint8Array | undefined> {
  const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
    "/graph/version/pull",
    {
      responseType: "arraybuffer",
      params: {
        id: id,
      },
    }
  );
  if (response.data) {
    const bytes = new Uint8Array(response.data);
    return bytes;
  } else return undefined;
}
