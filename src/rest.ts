import { LinkResolver, RelayStore } from "./relay-store";
import { Request, Response } from "express";
import express from "express";
import bodyParser from "body-parser";
import {
  chunkerFactory,
  Link,
  LinkCodec,
  linkCodecFactory,
} from "@ubiquify/core";
import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node";

const linkCodec: LinkCodec = linkCodecFactory();

export const createRestApplication = (
  relayStore: RelayStore,
  linkResolver: LinkResolver
): express.Application => {
  const handlePushVersionStoreRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const chunkSize = parseInt(req.query.chunkSize as string, 10);
    const { chunk } = chunkerFactory(chunkSize, compute_chunks);
    const bytes = new Uint8Array(req.body);
    try {
      const info: { storeRoot: Link; versionRoot: Link } =
        await relayStore.pushVersionStore(chunk, bytes);
      res.json({
        storeRoot: info.storeRoot.toString(),
        versionRoot: info.versionRoot.toString(),
      });
    } catch (error) {
      console.error("Error handling PushVersionStore request:", error);
      res.sendStatus(500);
    }
  };

  const handlePushGraphVersionRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const bytes = new Uint8Array(req.body);
    try {
      const info: { versionRoot: Link } = await relayStore.pushGraphVersion(
        bytes
      );
      res.json({
        versionRoot: info.versionRoot.toString(),
      });
    } catch (error) {
      console.error("Error handling PushGraphVersion request:", error);
      res.sendStatus(500);
    }
  };

  const handlePullVersionStoreRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const id = req.query.id as string;
    const chunkSize = req.query.chunkSize as string;
    const { chunk } = chunkerFactory(parseInt(chunkSize), compute_chunks);
    try {
      const bytes = await relayStore.pullVersionStore(chunk, id);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullVersionStore request:", error);
      res.sendStatus(500);
    }
  };

  const handlePullGraphVersionRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const id = req.query.id as string;
    try {
      const bytes = await relayStore.pullGraphVersion(id);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullGraphVersion request:", error);
      res.sendStatus(500);
    }
  };

  const handlePullRootIndexRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const id = req.query.id as string;
    try {
      const bytes = await relayStore.pullRootIndex(id);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullRootIndex request:", error);
      res.sendStatus(500);
    }
  };

  const handlePushBlocksRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const bytes = new Uint8Array(req.body);
    try {
      const info: { blockCount: number } = await relayStore.pushRandomBlocks(
        bytes
      );
      res.json({
        blockCount: info.blockCount,
      });
    } catch (error) {
      console.error("Error handling PushBlock request:", error);
      res.sendStatus(500);
    }
  };

  const handlePullBlocksRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const linkStrings: string[] = req.body.links;
    try {
      const bytes = await relayStore.pullRandomBlocks(linkStrings);
      if (bytes !== undefined) {
        res.send(Buffer.from(bytes.buffer));
      } else {
        res.sendStatus(404);
      }
    } catch (error) {
      console.error("Error handling PullBlocks request:", error);
      res.sendStatus(500);
    }
  };

  const handleVersionStoreResolveRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    const id = req.query.id as string;
    try {
      const result: Link | undefined = await linkResolver.resolve(id);
      if (result === undefined) {
        res.sendStatus(404);
      } else {
        res.status(200).json(linkCodec.encodeString(result));
      }
    } catch (error) {
      console.error("Error handling RESOLVE request:", error);
      res.sendStatus(500);
    }
  };

  const handleProtocolVersionRequest = async (
    req: Request,
    res: Response
  ): Promise<void> => {
    try {
      res.status(200).json(relayStore.getProtocolVersion());
    } catch (error) {
      console.error("Error handling PROTOCOL VERSION request:", error);
      res.sendStatus(500);
    }
  };

  const configure = (): express.Application => {
    const app: express.Application = express();
    // Enable CORS for all origins
    app.use((req, res, next) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type");
      next();
    });

    app.use((req, res, next) => {
      if (req.is("application/json")) {
        bodyParser.json()(req, res, next);
      } else {
        next();
      }
    });

    app.use((req, res, next) => {
      if (req.is("application/octet-stream")) {
        express.raw({ type: "application/octet-stream", limit: "1024mb" })(
          req,
          res,
          next
        );
      } else {
        next();
      }
    });

    app.put("/store/push", (req: Request, res: Response) =>
      handlePushVersionStoreRequest(req, res)
    );
    app.get("/store/pull", (req: Request, res: Response) =>
      handlePullVersionStoreRequest(req, res)
    );
    app.get("/store/resolve", (req: Request, res: Response) =>
      handleVersionStoreResolveRequest(req, res)
    );
    app.put("/graph/version/push", (req: Request, res: Response) =>
      handlePushGraphVersionRequest(req, res)
    );
    app.get("/graph/version/pull", (req: Request, res: Response) =>
      handlePullGraphVersionRequest(req, res)
    );
    app.get("/graph/index/pull", (req: Request, res: Response) =>
      handlePullRootIndexRequest(req, res)
    );
    app.put("/blocks/push", (req: Request, res: Response) =>
      handlePushBlocksRequest(req, res)
    );
    app.put("/blocks/pull", (req: Request, res: Response) =>
      handlePullBlocksRequest(req, res)
    );
    app.get("/protocol/version", (req: Request, res: Response) =>
      handleProtocolVersionRequest(req, res)
    );
    return app;
  };
  return configure();
};
