import { BlockStore } from "@ubiquify/core";
import { LinkResolver, RelayStore, relayStoreFactory } from "./relay-store";
import { createRestApplication } from "./rest";
import https from "https";
import * as http from "http";

export interface GraphRelay {
  startHttp(port: number, callback: () => void): http.Server;
  startHttps(
    port: number,
    options: { key: string; cert: string },
    callback: () => void
  ): https.Server;
  stopHttp(callback: () => void): void;
  stopHttps(callback: () => void): void;
}

export const createGraphRelay = (
  blockStore: BlockStore,
  linkResolver: LinkResolver
): GraphRelay => {
  let httpServer: http.Server | undefined;
  let httpsServer: https.Server | undefined;

  const createApplication = () => {
    const relayStore: RelayStore = relayStoreFactory(blockStore, linkResolver);
    return createRestApplication(relayStore, linkResolver);
  };

  const startHttp = (port: number, callback: () => void): http.Server => {
    const restApp = createApplication();
    httpServer = http.createServer(restApp);
    httpServer.listen(port, callback);
    return httpServer;
  };

  const startHttps = (
    port: number,
    options: { key: string; cert: string },
    callback: () => void
  ): https.Server => {
    const restApp = createApplication();
    httpsServer = https.createServer(options, restApp);
    httpsServer.listen(port, callback);
    return httpsServer;
  };

  const stopHttp = (callback: () => void): void => {
    if (httpServer !== undefined) {
      httpServer.close(callback);
    }
  };

  const stopHttps = (callback: () => void): void => {
    if (httpsServer !== undefined) {
      httpsServer.close(callback);
    }
  };

  return {
    startHttp,
    startHttps,
    stopHttp,
    stopHttps,
  };
};
