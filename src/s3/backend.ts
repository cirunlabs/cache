import {
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    S3Client
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as crypto from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Readable } from "stream";

import { S3Config } from "./inputs";

// Object naming layout inside the bucket:
//
//   <prefix>/<os-tag>/<key>/<version>.tzst
//
// `os-tag` defaults to ${runner.os}. enableCrossOsArchive=true drops it
// (single object name across OSes). `version` is a hash of the cache paths
// — same scheme @actions/cache uses, so two saves with the same `key` but
// different `path` inputs don't collide.

interface ObjectKey {
    key: string;       // user-supplied cache key
    objectName: string; // full S3 object key
    version: string;
}

function hashVersion(paths: string[], crossOs: boolean): string {
    const h = crypto.createHash("sha256");
    h.update(paths.join("\n"));
    h.update("\n");
    h.update(crossOs ? "cross-os" : process.platform);
    return h.digest("hex").slice(0, 16);
}

function osTag(crossOs: boolean): string {
    if (crossOs) return "any";
    // Match upstream RUNNER_OS env: Linux | Windows | macOS.
    return process.env.RUNNER_OS || process.platform;
}

function buildObjectName(
    cfg: S3Config,
    cacheKey: string,
    version: string,
    crossOs: boolean
): string {
    const parts: string[] = [];
    if (cfg.prefix) parts.push(cfg.prefix.replace(/^\/+|\/+$/g, ""));
    parts.push(osTag(crossOs));
    parts.push(cacheKey);
    return parts.join("/") + "/" + version + ".tzst";
}

function buildObjectPrefix(
    cfg: S3Config,
    keyPrefix: string,
    crossOs: boolean
): string {
    const parts: string[] = [];
    if (cfg.prefix) parts.push(cfg.prefix.replace(/^\/+|\/+$/g, ""));
    parts.push(osTag(crossOs));
    parts.push(keyPrefix);
    return parts.join("/") + "/";
}

function newClient(cfg: S3Config): S3Client {
    const opts: any = { region: cfg.region };
    if (cfg.endpoint) {
        const url = cfg.endpoint.startsWith("http")
            ? cfg.endpoint
            : `https://${cfg.endpoint}`;
        opts.endpoint = url;
        opts.forcePathStyle = true;
    }
    if (cfg.accessKeyId && cfg.secretAccessKey) {
        opts.credentials = {
            accessKeyId: cfg.accessKeyId,
            secretAccessKey: cfg.secretAccessKey
        };
    }
    return new S3Client(opts);
}

async function objectExists(
    client: S3Client,
    bucket: string,
    objectName: string
): Promise<boolean> {
    try {
        await client.send(
            new HeadObjectCommand({ Bucket: bucket, Key: objectName })
        );
        return true;
    } catch (err: any) {
        if (
            err?.$metadata?.httpStatusCode === 404 ||
            err?.name === "NotFound" ||
            err?.name === "NoSuchKey"
        ) {
            return false;
        }
        throw err;
    }
}

// findRestoreMatch resolves `restore-keys` semantics against S3:
//   1. Try exact match on (key, version).
//   2. For each restoreKey: list objects under the prefix, pick the most
//      recently modified entry that matches our `version` (so a partial-key
//      hit only counts when the path-hash also matches).
async function findRestoreMatch(
    client: S3Client,
    cfg: S3Config,
    primaryKey: string,
    restoreKeys: string[],
    version: string,
    crossOs: boolean
): Promise<ObjectKey | null> {
    const exactName = buildObjectName(cfg, primaryKey, version, crossOs);
    if (await objectExists(client, cfg.bucket, exactName)) {
        return { key: primaryKey, objectName: exactName, version };
    }

    for (const rk of restoreKeys) {
        const prefix = buildObjectPrefix(cfg, rk, crossOs);
        const out = await client.send(
            new ListObjectsV2Command({
                Bucket: cfg.bucket,
                Prefix: prefix
            })
        );
        const matches = (out.Contents || [])
            .filter(o => o.Key && o.Key.endsWith(`/${version}.tzst`))
            .sort((a, b) => {
                const ad = a.LastModified ? a.LastModified.getTime() : 0;
                const bd = b.LastModified ? b.LastModified.getTime() : 0;
                return bd - ad;
            });
        if (matches.length > 0 && matches[0].Key) {
            // Recover the original `key` (the segment between osTag and
            // version filename) from the matched object name.
            const parts = matches[0].Key.split("/");
            // last is `<version>.tzst`; second-last is `key`.
            const matchedKey = parts.length >= 2 ? parts[parts.length - 2] : rk;
            return {
                key: matchedKey,
                objectName: matches[0].Key,
                version
            };
        }
    }

    return null;
}

async function downloadToFile(
    client: S3Client,
    bucket: string,
    objectName: string,
    dest: string
): Promise<void> {
    const out = await client.send(
        new GetObjectCommand({ Bucket: bucket, Key: objectName })
    );
    const body = out.Body as Readable;
    await new Promise<void>((resolve, reject) => {
        const w = fs.createWriteStream(dest);
        body.pipe(w);
        body.on("error", reject);
        w.on("error", reject);
        w.on("close", resolve);
    });
}

async function uploadFile(
    client: S3Client,
    bucket: string,
    objectName: string,
    src: string
): Promise<void> {
    const stream = fs.createReadStream(src);
    const upload = new Upload({
        client,
        params: { Bucket: bucket, Key: objectName, Body: stream },
        // 64 MB part × 8 concurrent uploads — matches @actions/cache's
        // default Azure-SDK uploadOptions and lands in S3's sweet spot
        // for high-throughput multipart.
        partSize: 64 * 1024 * 1024,
        queueSize: 8
    });
    await upload.done();
}

async function makeArchive(paths: string[]): Promise<string> {
    const tmp = path.join(os.tmpdir(), `cirunlabs-cache-${Date.now()}.tzst`);
    // System tar with zstd. Available on every Linux runner; macOS uses bsdtar
    // which also accepts --use-compress-program. zstd binary is preinstalled
    // on github-hosted and cirun-provisioned runners.
    const args = [
        "-cf",
        tmp,
        "--use-compress-program=zstd -T0 --long=30",
        ...paths
    ];
    await exec.exec("tar", args);
    return tmp;
}

async function extractArchive(src: string): Promise<void> {
    await exec.exec("tar", [
        "-xf",
        src,
        "--use-compress-program=unzstd --long=30"
    ]);
}

export interface RestoreResult {
    matchedKey: string | null;
    isExactKeyMatch: boolean;
}

export async function restoreCache(
    cfg: S3Config,
    paths: string[],
    primaryKey: string,
    restoreKeys: string[],
    crossOs: boolean,
    lookupOnly: boolean
): Promise<RestoreResult> {
    const client = newClient(cfg);
    const version = hashVersion(paths, crossOs);

    core.info(
        `S3 cache: looking up ${primaryKey} (version ${version}) in s3://${cfg.bucket}/`
    );

    const match = await findRestoreMatch(
        client,
        cfg,
        primaryKey,
        restoreKeys,
        version,
        crossOs
    );

    if (!match) {
        core.info("S3 cache: miss");
        return { matchedKey: null, isExactKeyMatch: false };
    }

    core.info(`S3 cache: hit on ${match.objectName}`);

    if (lookupOnly) {
        return {
            matchedKey: match.key,
            isExactKeyMatch: match.key === primaryKey
        };
    }

    const archive = path.join(
        os.tmpdir(),
        `cirunlabs-cache-restore-${Date.now()}.tzst`
    );
    await downloadToFile(client, cfg.bucket, match.objectName, archive);
    await extractArchive(archive);
    try {
        await fs.promises.unlink(archive);
    } catch {
        /* best effort */
    }

    return {
        matchedKey: match.key,
        isExactKeyMatch: match.key === primaryKey
    };
}

export async function saveCache(
    cfg: S3Config,
    paths: string[],
    primaryKey: string,
    crossOs: boolean
): Promise<void> {
    const client = newClient(cfg);
    const version = hashVersion(paths, crossOs);
    const objectName = buildObjectName(cfg, primaryKey, version, crossOs);

    if (await objectExists(client, cfg.bucket, objectName)) {
        core.info(
            `S3 cache: ${objectName} already exists, skipping save (use a new key to overwrite)`
        );
        return;
    }

    core.info(`S3 cache: saving to s3://${cfg.bucket}/${objectName}`);
    const archive = await makeArchive(paths);
    try {
        await uploadFile(client, cfg.bucket, objectName, archive);
        core.info(`S3 cache: saved ${objectName}`);
    } finally {
        try {
            await fs.promises.unlink(archive);
        } catch {
            /* best effort */
        }
    }
}
