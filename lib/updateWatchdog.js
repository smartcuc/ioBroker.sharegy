/**
 * UpdateWatchdog: Manages Canary A/B OTA Remote Updates with 15-minute Rollback Protection
 */

const fs = require("fs");
const path = require("path");
const { spawn, exec } = require("child_process");

class UpdateWatchdog {
    constructor(adapter) {
        this.adapter = adapter;
        this.stateFilePath = path.join(__dirname, "..", ".update_guard_state.json");
        this.autoConfirmTimer = null;
        this.STABILITY_CHECK_MS = 3 * 60 * 1000; // 3 minutes of stable connection for auto-confirm
    }

    readState() {
        try {
            if (fs.existsSync(this.stateFilePath)) {
                return JSON.parse(fs.readFileSync(this.stateFilePath, "utf8"));
            }
        } catch (e) {
            this.adapter.log.warn(`[OTA Watchdog] Could not read state: ${e.message}`);
        }
        return null;
    }

    writeState(state) {
        try {
            fs.writeFileSync(this.stateFilePath, JSON.stringify(state, null, 2), "utf8");
        } catch (e) {
            this.adapter.log.error(`[OTA Watchdog] Could not write state: ${e.message}`);
        }
    }

    /**
     * Called on adapter boot: checks if an update was recently performed and starts self-test timer
     */
    checkPendingUpdateOnStartup() {
        const state = this.readState();
        if (!state) return;

        if (state.status === "pending") {
            const currentVer = this.adapter.version || "unknown";
            this.adapter.log.warn(`[OTA Watchdog] 🛡️ Adapter booted under pending update verification! Target: ${state.target_version} (Active: ${currentVer}). 15-min rollback timer active.`);

            // Emit warning telemetry event to carrier
            if (typeof this.adapter.emitCarrierErrorEvent === "function") {
                this.adapter.emitCarrierErrorEvent("OTA_UPDATE_PENDING_CONFIRMATION", `Adapter booted under verification window. Auto-confirm in 3 minutes if stable.`, {
                    target_version: state.target_version,
                    previous_version: state.previous_version,
                    timeout_seconds: state.timeout_seconds,
                }, "warn");
            }

            // Start 3-minute stability timer for automatic confirmation
            this.autoConfirmTimer = setTimeout(() => {
                // Verify connection is active
                const isPrimaryConnected = typeof this.adapter.isConnectionActive === "function" && this.adapter.isConnectionActive();
                const isCarrierConnected = this.adapter.carrierWs && this.adapter.carrierWs.readyState === 1;

                if (isPrimaryConnected || isCarrierConnected) {
                    this.confirmUpdate("auto_stability_test_passed");
                } else {
                    this.adapter.log.warn(`[OTA Watchdog] ⚠️ 3-min stability check: Connection not established yet. Waiting for manual confirm or 15-min timeout.`);
                }
            }, this.STABILITY_CHECK_MS);
        } else if (state.status === "confirmed") {
            this.adapter.log.info(`[OTA Watchdog] Current version ${this.adapter.version} is confirmed stable.`);
        }
    }

    /**
     * Confirms the update as permanent and terminates the rollback guard
     */
    confirmUpdate(reason = "manual_admin_rpc") {
        const state = this.readState();
        if (!state) {
            return { status: "no_active_update", message: "No pending update found to confirm." };
        }

        if (this.autoConfirmTimer) {
            clearTimeout(this.autoConfirmTimer);
            this.autoConfirmTimer = null;
        }

        state.status = "confirmed";
        state.confirmed_at = Date.now();
        state.confirmation_reason = reason;
        this.writeState(state);

        this.adapter.log.info(`[OTA Watchdog] ✅ Update confirmed permanently! Version ${this.adapter.version} marked as golden image (${reason}).`);

        if (typeof this.adapter.emitCarrierErrorEvent === "function") {
            this.adapter.emitCarrierErrorEvent("OTA_UPDATE_CONFIRMED", `Version ${this.adapter.version} confirmed permanently (${reason}).`, {
                active_version: this.adapter.version,
                confirmed_at: state.confirmed_at,
            }, "info");
        }

        return {
            status: "confirmed",
            active_version: this.adapter.version,
            reason: reason,
            confirmed_at: state.confirmed_at,
        };
    }

    /**
     * Initiates a remote update with 15-minute rollback safety net
     */
    async initiateUpdate(targetSpec, timeoutSeconds = 900) {
        const currentVersion = this.adapter.version || (require("../package.json").version) || "unknown";
        const target = targetSpec || "smartcuc/ioBroker.sharegy#main";
        const installDir = path.resolve(__dirname, "..", "..", ".."); // typically /opt/iobroker or node_modules parent

        if (String(target).toLowerCase().includes("homeassistant")) {
            this.adapter.log.error(`[OTA Watchdog] ❌ Invalid update target '${target}' for ioBroker adapter!`);
            return {
                status: "error",
                message: `Invalid update package '${target}'. ioBroker requires 'smartcuc/ioBroker.sharegy', not Home Assistant packages.`
            };
        }

        this.adapter.log.warn(`[OTA Watchdog] 🚀 Initiating guarded OTA update from ${currentVersion} to '${target}' (Rollback Timeout: ${timeoutSeconds}s)...`);

        // Record state
        const state = {
            previous_version: currentVersion,
            previous_pkg_spec: `smartcuc/ioBroker.sharegy#v${currentVersion}`,
            target_version: target,
            install_dir: installDir,
            started_at: Date.now(),
            timeout_seconds: timeoutSeconds,
            status: "pending",
        };
        this.writeState(state);

        // Spawn independent rollback guard script detached
        const guardScript = path.join(__dirname, "..", "scripts", "rollback_guard.js");
        try {
            const child = spawn(process.execPath, [guardScript, this.stateFilePath], {
                detached: true,
                stdio: "ignore",
                cwd: installDir,
            });
            child.unref();
            this.adapter.log.info(`[OTA Watchdog] Independent rollback guard spawned with PID: ${child.pid}`);
        } catch (err) {
            this.adapter.log.error(`[OTA Watchdog] Failed to spawn rollback guard: ${err.message}`);
        }

        // Normalize target URL for ioBroker
        let targetUrl = target;
        if (!targetUrl.startsWith("http") && !targetUrl.startsWith("git@") && targetUrl.includes("/")) {
            targetUrl = `https://github.com/${targetUrl.replace(/^github:/, "")}`;
        }

        // Execute ioBroker URL install or npm install + iobroker upload
        const installCmd = `iobroker url "${targetUrl}" || (npm install --save "${targetUrl}" && (iobroker upload sharegy || true))`;
        this.adapter.log.info(`[OTA Watchdog] Running install: ${installCmd} in ${installDir}`);

        exec(installCmd, { cwd: installDir, timeout: 180000, env: process.env }, (error, stdout, stderr) => {
            if (error) {
                this.adapter.log.error(`[OTA Watchdog] OTA install failed: ${error.message} (stderr: ${stderr}). Aborting update.`);
                state.status = "install_failed";
                state.error = error.message;
                this.writeState(state);
                return;
            }

            this.adapter.log.info(`[OTA Watchdog] OTA install succeeded! Output: ${stdout.slice(-300)}`);
            this.adapter.log.info(`[OTA Watchdog] Scheduling adapter restart in 1.5 seconds...`);
            setTimeout(() => {
                if (typeof this.adapter.restart === "function") {
                    this.adapter.restart();
                } else {
                    process.exit(0);
                }
            }, 1500);
        });

        return {
            status: "initiated",
            previous_version: currentVersion,
            target: target,
            timeout_seconds: timeoutSeconds,
            message: `Guarded OTA update initiated. Adapter will install and restart automatically. 15-minute rollback guard is active.`,
        };
    }

    /**
     * Triggers immediate manual rollback to previous version
     */
    triggerImmediateRollback() {
        const state = this.readState();
        const prevSpec = (state && state.previous_pkg_spec) ? state.previous_pkg_spec : "smartcuc/ioBroker.sharegy#main";
        const installDir = (state && state.install_dir) ? state.install_dir : path.resolve(__dirname, "..", "..", "..");

        this.adapter.log.warn(`[OTA Watchdog] 🚨 Manual rollback requested to '${prevSpec}'!`);

        if (state) {
            state.status = "manual_rollback_triggered";
            this.writeState(state);
        }

        exec(`npm install --save ${prevSpec}`, { cwd: installDir, timeout: 180000 }, (err) => {
            if (err) {
                this.adapter.log.error(`[OTA Watchdog] Manual rollback failed: ${err.message}`);
            } else {
                this.adapter.log.info(`[OTA Watchdog] Manual rollback package re-installed. Restarting...`);
            }
            setTimeout(() => {
                if (typeof this.adapter.restart === "function") {
                    this.adapter.restart();
                } else {
                    process.exit(0);
                }
            }, 1000);
        });

        return {
            status: "rolling_back",
            target: prevSpec,
            message: "Immediate manual rollback initiated.",
        };
    }

    getStatus() {
        const state = this.readState();
        return {
            current_version: this.adapter.version || (require("../package.json").version) || "unknown",
            guard_state: state || { status: "idle", message: "No update in progress." },
            uptime: Math.round(process.uptime()),
        };
    }
}

module.exports = UpdateWatchdog;
