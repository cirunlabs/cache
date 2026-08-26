// Post step for the composite cirunlabs/cache/s3 action: saves the cache
// when the main step recorded a primary key but no exact match (or no match
// at all). Skips when the primary key was an exact hit.

import * as core from "@actions/core";

import { Inputs, State } from "./constants";
import { saveCache } from "./s3/backend";
import { readS3Config } from "./s3/inputs";

async function run(): Promise<void> {
    try {
        const cfg = readS3Config();
        if (!cfg) {
            // Main didn't run S3 path either — nothing to do.
            return;
        }

        const primaryKey = core.getState(State.CachePrimaryKey);
        if (!primaryKey) {
            core.warning("No primary key state from main step — skipping save");
            return;
        }

        const matchedKey = core.getState(State.CacheMatchedKey);
        if (matchedKey === primaryKey) {
            core.info("Cache hit on primary key — skipping save");
            return;
        }

        const paths = (core.getInput(Inputs.Path, { required: true }) || "")
            .split("\n")
            .map(s => s.trim())
            .filter(s => s.length > 0);
        const crossOs = core.getBooleanInput(Inputs.EnableCrossOsArchive);

        await saveCache(cfg, paths, primaryKey, crossOs);
    } catch (err) {
        core.setFailed((err as Error).message);
    }
}

run();
