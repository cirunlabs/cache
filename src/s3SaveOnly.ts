import * as core from "@actions/core";

import { saveCache } from "./s3/backend";
import { readS3Config } from "./s3/inputs";
import { Inputs } from "./constants";

async function run(): Promise<void> {
    try {
        const cfg = readS3Config();
        if (!cfg) {
            core.setFailed("s3-bucket is required for cirunlabs/cache/s3-save");
            return;
        }

        const primaryKey = core.getInput(Inputs.Key, { required: true });
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
