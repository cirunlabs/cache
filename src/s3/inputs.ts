import * as core from "@actions/core";

// S3 inputs are intentionally namespaced as `s3-*` and live in this fork's
// own action.yml files (s3/, s3-restore/, s3-save/). They never touch
// upstream's action.yml so future actions/cache rebases don't conflict.
export interface S3Config {
    bucket: string;
    endpoint?: string; // omit → AWS S3 default (regional)
    region: string;
    accessKeyId?: string;
    secretAccessKey?: string;
    prefix: string; // empty string when not set
}

// readS3Config returns null when s3-bucket is unset (caller falls back to
// upstream cache backend). When set, the other inputs are read with their
// documented defaults.
export function readS3Config(): S3Config | null {
    const bucket = core.getInput("s3-bucket");
    if (!bucket) {
        return null;
    }
    return {
        bucket,
        endpoint: core.getInput("s3-endpoint") || undefined,
        region: core.getInput("s3-region") || "auto",
        accessKeyId: core.getInput("s3-access-key-id") || undefined,
        secretAccessKey: core.getInput("s3-secret-access-key") || undefined,
        prefix: core.getInput("s3-prefix") || ""
    };
}
