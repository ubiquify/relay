import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export interface Certificate {
  key: string;
  cert: string;
}

export const getCertificate = (): Certificate => {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const privateKeyPath = path.join(__dirname, "..", "ssl", "server.key");
  const certificatePath = path.join(__dirname, "..", "ssl", "server.crt");
  const privateKey = fs.readFileSync(privateKeyPath, "utf8");
  const certificate = fs.readFileSync(certificatePath, "utf8");
  return { key: privateKey, cert: certificate };
};
