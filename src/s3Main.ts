// Main entry for the composite cirunlabs/cache/s3 action: restores cache and
// stores enough state for the post step (s3Post.ts) to know whether a save
// is required (skip when the primary key was an exact hit).

import * as core from "@actions/core";

import { restoreCache } from "./s3/backend";
import { readS3Config } from "./s3/inputs";
import { Inputs, Outputs, State } from "./constants";

async function run(): Promise<void> {
    try {
        const cfg = readS3Config();
        if (!cfg) {
            core.setFailed("s3-bucket is required for cirunlabs/cache/s3");
            return;
        }

        const primaryKey = core.getInput(Inputs.Key, { required: true });
        const restoreKeys = (core.getInput(Inputs.RestoreKeys) || "")
            .split("\n")
            .map(s => s.trim())
            .filter(s => s.length > 0);
        const paths = (core.getInput(Inputs.Path, { required: true }) || "")
            .split("\n")
            .map(s => s.trim())
            .filter(s => s.length > 0);
        const crossOs = core.getBooleanInput(Inputs.EnableCrossOsArchive);
        const failOnMiss = core.getBooleanInput(Inputs.FailOnCacheMiss);
        const lookupOnly = core.getBooleanInput(Inputs.LookupOnly);

        // Hand the primary key to the post step so it can decide whether to
        // save (skip when matched key === primary key — an exact hit means
        // nothing to write back).
        core.saveState(State.CachePrimaryKey, primaryKey);

        const result = await restoreCache(
            cfg,
            paths,
            primaryKey,
            restoreKeys,
            crossOs,
            lookupOnly
        );

        if (!result.matchedKey) {
            if (failOnMiss) {
                throw new Error(
                    `Failed to restore cache entry. Exiting as fail-on-cache-miss is set. Input key: ${primaryKey}`
                );
            }
            core.setOutput(Outputs.CacheHit, "false");
            return;
        }

        core.saveState(State.CacheMatchedKey, result.matchedKey);
        core.setOutput(Outputs.CacheHit, result.isExactKeyMatch.toString());
    } catch (err) {
        core.setFailed((err as Error).message);
    }
}

run();
