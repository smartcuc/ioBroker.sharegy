/**
 * Independent Rollback Guard Process for ioBroker.sharegy
 * Runs completely detached in the background.
 * If the updated adapter does not confirm stability within 15 minutes,
 * this watchdog autonomously rolls back to the previous version and restarts the adapter.
 */

const fs = require("fs");
const path = require("path");
const { exec } = require("child_process");

const stateFilePath = process.argv[2] || path.join(__dirname, "..", ".update_guard_state.json");
const CHECK_INTERVAL_MS = 10000; // 10 seconds
const DEFAULT_TIMEOUT_MS = 15 * 60 * 1000; // 15 minutes

console.log(`[RollbackGuard] Started independent watchdog monitoring: ${stateFilePath} (PID: ${process.pid})`);

function readState() {
    try {
        if (fs.existsSync(stateFilePath)) {
            const raw = fs.readFileSync(stateFilePath, "utf8");
            return JSON.parse(raw);
        }
    } catch (e) {
        console.error(`[RollbackGuard] Error reading state file: ${e.message}`);
    }
    return null;
}

function writeState(state) {
    try {
        fs.writeFileSync(stateFilePath, JSON.stringify(state, null, 2), "utf8");
    } catch (e) {
        console.error(`[RollbackGuard] Error writing state file: ${e.message}`);
    }
}

function executeRollback(state) {
    const prevSpec = state.previous_pkg_spec || "smartcuc/ioBroker.sharegy#main";
    const cwd = state.install_dir || process.cwd();

    console.warn(`[RollbackGuard] ⚠️ 15-MINUTE TIMEOUT EXPIRED without confirmation! Triggering automatic rollback to: ${prevSpec}`);

    state.status = "rolling_back";
    state.rollback_triggered_at = Date.now();
    writeState(state);

    let prevUrl = prevSpec;
    if (!prevUrl.startsWith("http") && !prevUrl.startsWith("git@") && prevUrl.includes("/")) {
        prevUrl = `https://github.com/${prevUrl.replace(/^github:/, "")}`;
    }

    const rollbackCmd = `iobroker url "${prevUrl}" || (npm install --save "${prevUrl}" && (iobroker upload sharegy || true))`;
    console.log(`[RollbackGuard] Running rollback: ${rollbackCmd} in ${cwd}`);

    exec(rollbackCmd, { cwd: cwd, timeout: 180000, env: process.env }, (err, stdout, stderr) => {
        if (err) {
            console.error(`[RollbackGuard] Rollback install failed: ${err.message}`);
        } else {
            console.log(`[RollbackGuard] Rollback install output:\n${stdout}`);
        }

        // Restart adapter instance in ioBroker
        exec("iobroker restart sharegy.0 || pkill -f 'iobroker.sharegy'", { cwd: cwd }, (restartErr) => {
            if (restartErr) {
                console.warn(`[RollbackGuard] Direct iobroker restart notice: ${restartErr.message}`);
            }
            state.status = "rolled_back";
            state.rollback_completed_at = Date.now();
            writeState(state);
            console.log(`[RollbackGuard] ✅ Rollback completed successfully. Guard exiting.`);
            process.exit(0);
        });
    });
}

const timer = setInterval(() => {
    const state = readState();

    if (!state) {
        console.log(`[RollbackGuard] No active state file found. Guard exiting.`);
        clearInterval(timer);
        process.exit(0);
    }

    if (state.status === "confirmed") {
        console.log(`[RollbackGuard] ✅ Adapter update confirmed stable (${state.target_version}). Guard exiting cleanly.`);
        clearInterval(timer);
        process.exit(0);
    }

    if (state.status === "rolled_back") {
        console.log(`[RollbackGuard] Rollback already executed. Guard exiting.`);
        clearInterval(timer);
        process.exit(0);
    }

    const elapsed = Date.now() - (state.started_at || Date.now());
    const timeoutMs = (state.timeout_seconds || 900) * 1000;

    if (elapsed >= timeoutMs && state.status === "pending") {
        clearInterval(timer);
        executeRollback(state);
    }
}, CHECK_INTERVAL_MS);
