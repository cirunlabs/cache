import * as core from "@actions/core";
import * as exec from "@actions/exec";
import {
    GetBucketLifecycleConfigurationCommand,
    GetObjectCommand,
    HeadObjectCommand,
    type LifecycleRule,
    ListObjectsV2Command,
    PutBucketLifecycleConfigurationCommand,
    S3Client
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
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
    key: string; // user-supplied cache key
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
    // No trailing slash. S3 ListObjectsV2 Prefix is a literal substring
    // match — adding `/` would force the next character of the stored
    // object key to be a slash, which never happens because the saved
    // key embeds <key>/<version>.tzst (the slash is *between* key and
    // version filename). Concretely, restore-keys "Linux-X64-go-" must
    // prefix-match objects under "Linux-X64-go-<hash>/...", not require
    // the key to literally start with "Linux-X64-go-/".
    const parts: string[] = [];
    if (cfg.prefix) parts.push(cfg.prefix.replace(/^\/+|\/+$/g, ""));
    parts.push(osTag(crossOs));
    parts.push(keyPrefix);
    return parts.join("/");
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

// downloadToFile fans an object download into N parallel ranged GETs.
// Each worker owns a contiguous byte range and writes directly into the
// pre-allocated destination file at the right offset, so peak memory stays
// at workers × in-flight chunk (~not the whole object). On EC2 → S3 this
// pulls 5-10× the throughput of a single-stream GetObject; on EC2 → R2
// the win is smaller (R2 anycast already aggregates across PoPs) but still
// 2-3×.
async function downloadToFile(
    client: S3Client,
    bucket: string,
    objectName: string,
    dest: string,
    partSize: number = 32 * 1024 * 1024,
    workers: number = 8
): Promise<void> {
    const head = await client.send(
        new HeadObjectCommand({ Bucket: bucket, Key: objectName })
    );
    const size = head.ContentLength;
    if (typeof size !== "number" || size <= 0) {
        throw new Error(
            `S3 HEAD for ${objectName} returned no Content-Length; cannot parallelise download`
        );
    }

    // Pre-allocate the output file so workers can write at any offset.
    {
        const fh = await fs.promises.open(dest, "w");
        await fh.truncate(size);
        await fh.close();
    }

    const totalParts = Math.ceil(size / partSize);
    const ranges: { start: number; end: number; index: number }[] = [];
    for (let i = 0; i < totalParts; i++) {
        const start = i * partSize;
        const end = Math.min(start + partSize - 1, size - 1);
        ranges.push({ start, end, index: i });
    }

    let nextRange = 0;
    const errors: Error[] = [];

    async function worker(): Promise<void> {
        const fh = await fs.promises.open(dest, "r+");
        try {
            while (true) {
                const idx = nextRange++;
                if (idx >= ranges.length) return;
                const { start, end } = ranges[idx];
                const out = await client.send(
                    new GetObjectCommand({
                        Bucket: bucket,
                        Key: objectName,
                        Range: `bytes=${start}-${end}`
                    })
                );
                const body = out.Body as Readable;
                let offset = start;
                for await (const chunk of body) {
                    const buf = chunk as Buffer;
                    let written = 0;
                    while (written < buf.length) {
                        const w = await fh.write(
                            buf,
                            written,
                            buf.length - written,
                            offset + written
                        );
                        written += w.bytesWritten;
                    }
                    offset += buf.length;
                }
                if (offset !== end + 1) {
                    throw new Error(
                        `S3 ranged GET short read: expected ${end - start + 1} bytes for ${objectName} [${start}-${end}], got ${offset - start}`
                    );
                }
            }
        } catch (err) {
            errors.push(err as Error);
        } finally {
            await fh.close();
        }
    }

    const w = Math.min(workers, totalParts);
    await Promise.all(Array.from({ length: w }, () => worker()));

    if (errors.length > 0) {
        throw errors[0];
    }
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

// expandTilde resolves a leading `~/` to the user's home dir. The action
// receives paths verbatim from the workflow YAML (e.g. ~/.cache/go-build);
// tar is invoked via execve with no shell, so `~` would otherwise be
// passed as a literal character and stat-fail.
function expandTilde(p: string): string {
    if (p === "~") return os.homedir();
    if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
    return p;
}

async function makeArchive(paths: string[]): Promise<string> {
    const tmp = path.join(os.tmpdir(), `cirunlabs-cache-${Date.now()}.tzst`);
    // -P preserves absolute paths so caches under $HOME restore to their
    // original location. Relative paths (./demo-cache) are unaffected.
    // System tar with zstd. Available on every Linux runner; macOS uses
    // bsdtar which also accepts --use-compress-program. zstd binary is
    // preinstalled on github-hosted and cirun-provisioned runners.
    const args = [
        "-cf",
        tmp,
        "-P",
        "--use-compress-program=zstd -T0 --long=30",
        ...paths.map(expandTilde)
    ];
    await exec.exec("tar", args);
    return tmp;
}

async function extractArchive(src: string): Promise<void> {
    // `zstd -d -T0` enables multi-threaded decompression on multi-frame
    // archives (the same `zstd -T0 --long=30` we use to compress emits
    // multiple frames, so decompression parallelises). On 1+ GB caches
    // this saves another 30-50% of extract time vs single-threaded
    // unzstd. -P matches the save side so absolute paths land back where
    // they came from.
    await exec.exec("tar", [
        "-xf",
        src,
        "-P",
        "--use-compress-program=zstd -d -T0 --long=30"
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

    if (cfg.ttlDays > 0) {
        await ensureLifecycle(client, cfg).catch(err => {
            // Best-effort: a missing s3:PutLifecycleConfiguration permission
            // shouldn't fail the cache save. Log so the operator notices.
            core.warning(
                `S3 lifecycle: could not ensure ${cfg.ttlDays}-day expiry on ${cfg.prefix || "(root)"}: ${(err as Error).message}`
            );
        });
    }
}

// ensureLifecycle is idempotent: it reads the bucket's existing lifecycle
// configuration, replaces only the rule keyed by our deterministic ID
// (one per prefix), and PUTs the merged set back. Other rules — including
// rules set by other repos sharing the bucket under different prefixes —
// are preserved untouched.
async function ensureLifecycle(client: S3Client, cfg: S3Config): Promise<void> {
    const ruleId = `cirunlabs-cache-${cfg.prefix.replace(/\//g, "-") || "root"}`;
    const filterPrefix = cfg.prefix
        ? cfg.prefix.replace(/^\/+|\/+$/g, "") + "/"
        : "";
    const desired: LifecycleRule = {
        ID: ruleId,
        Status: "Enabled",
        Filter: { Prefix: filterPrefix },
        Expiration: { Days: cfg.ttlDays }
    };

    let existing: LifecycleRule[] = [];
    try {
        const out = await client.send(
            new GetBucketLifecycleConfigurationCommand({ Bucket: cfg.bucket })
        );
        existing = out.Rules || [];
    } catch (err: any) {
        // R2 / S3 return NoSuchLifecycleConfiguration when no rules are set.
        // Treat as empty rule set; any other error propagates.
        const code = err?.name || err?.Code || "";
        const status = err?.$metadata?.httpStatusCode;
        if (
            code === "NoSuchLifecycleConfiguration" ||
            code === "NoSuchLifecycleConfigurationError" ||
            status === 404
        ) {
            existing = [];
        } else {
            throw err;
        }
    }

    // Skip the PUT if the desired rule is already present and matches.
    const current = existing.find(r => r.ID === ruleId);
    if (
        current &&
        current.Status === "Enabled" &&
        current.Expiration?.Days === cfg.ttlDays &&
        (current.Filter as any)?.Prefix === filterPrefix
    ) {
        return;
    }

    const merged = [...existing.filter(r => r.ID !== ruleId), desired];
    await client.send(
        new PutBucketLifecycleConfigurationCommand({
            Bucket: cfg.bucket,
            LifecycleConfiguration: { Rules: merged }
        })
    );
    core.info(
        `S3 lifecycle: ensured ${cfg.ttlDays}-day expiry on ${cfg.prefix || "(root)"} prefix`
    );
}
