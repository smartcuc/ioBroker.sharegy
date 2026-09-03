"use strict";

/*
 * ioBroker.sharegy Adapter
 * (C) 2026 Sharegy Team <info@sharegy.de>
 * License: MIT
 */

const utils = require("@iobroker/adapter-core");
const mqtt = require("mqtt");

class SharegyAdapter extends utils.Adapter {

    constructor(options = {}) {
        super({
            ...options,
            name: "sharegy",
        });

        this.mqttClient = null;
        this.subscribedStateIds = new Set();
        this.lastSentTimestamps = new Map();
        this.pendingUpdates = new Map();
        this.throttleTimer = null;

        this.on("ready", this.onReady.bind(this));
        this.on("stateChange", this.onStateChange.bind(this));
        this.on("unload", this.onUnload.bind(this));
    }

    /**
     * Is called when databases are connected and adapter received configuration.
     */
    async onReady() {
        this.log.info("Starting Sharegy Energy Management Adapter...");

        // Reset connection status
        await this.setStateAsync("info.connection", false, true);

        // Validate Token
        if (!this.config.mqttToken || this.config.mqttToken.trim() === "") {
            this.log.error("No Sharegy Home Token configured! Please enter your token in the adapter settings.");
            return;
        }

        // Initialize MQTT Connection
        this.connectMqtt();

        // Subscribe to configured EMS and Custom Device states
        this.initSubscriptions();
    }

    /**
     * Connect to the Sharegy Server via WSS (Secure WebSocket) or MQTTS
     */
    connectMqtt() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        const host = (this.config.host || "sharegy.de").trim();
        const port = Number(this.config.port) || (proto === "wss" ? 443 : 8883);
        const path = (this.config.path || "/mqtt").trim();

        let url = "";
        if (proto === "wss") {
            const cleanPath = path.startsWith("/") ? path : `/${path}`;
            url = `wss://${host}:${port}${cleanPath}`;
        } else {
            url = `mqtts://${host}:${port}`;
        }

        const token = this.config.mqttToken.trim();
        const clientId = `iobroker_sharegy_${this.instance}_${Math.random().toString(16).substring(2, 8)}`;

        const options = {
            clientId,
            clean: true,
            connectTimeout: 10000,
            reconnectPeriod: 5000,
            rejectUnauthorized: true,
        };

        if (this.config.mqttUsername) {
            options.username = this.config.mqttUsername.trim();
        }
        if (this.config.mqttPassword) {
            options.password = this.config.mqttPassword;
        }

        this.log.info(`Connecting to Sharegy via ${proto.toUpperCase()} at ${url} (ClientID: ${clientId})...`);

        try {
            this.mqttClient = mqtt.connect(url, options);

            this.mqttClient.on("connect", () => {
                this.log.info("Connected to Sharegy MQTT Broker successfully!");
                this.setState("info.connection", true, true);

                // Subscribe to Bidirectional Control Topics: h/<token>/+/set and h/<token>/control/+
                const controlTopicWildcard = `h/${token}/+/set`;
                const globalControlTopic = `h/${token}/control/#`;

                this.mqttClient.subscribe([controlTopicWildcard, globalControlTopic], (err) => {
                    if (err) {
                        this.log.error(`Failed to subscribe to control topics: ${err.message}`);
                    } else {
                        this.log.info(`Subscribed to Sharegy control channels: ${controlTopicWildcard}, ${globalControlTopic}`);
                    }
                });

                // Send initial snapshot of all monitored states
                this.publishAllStates();
            });

            this.mqttClient.on("message", (topic, payload) => {
                this.handleIncomingMqttMessage(topic, payload);
            });

            this.mqttClient.on("error", (err) => {
                this.log.warn(`MQTT Error: ${err.message}`);
                this.setState("info.connection", false, true);
            });

            this.mqttClient.on("close", () => {
                this.log.debug("MQTT connection closed.");
                this.setState("info.connection", false, true);
            });

            this.mqttClient.on("reconnect", () => {
                this.log.debug("Reconnecting to Sharegy MQTT Broker...");
            });

        } catch (e) {
            this.log.error(`Failed to create MQTT client: ${e.message}`);
        }
    }

    /**
     * Subscribe to ioBroker Foreign States based on configuration
     */
    initSubscriptions() {
        this.subscribedStateIds.clear();

        // 1. EMS Core States
        const emsKeys = [
            "pvPowerId",
            "gridPowerId",
            "gridImportId",
            "gridExportId",
            "batteryPowerId",
            "batterySocId",
            "houseConsumptionId",
        ];

        for (const key of emsKeys) {
            const stateId = this.config[key];
            if (stateId && typeof stateId === "string" && stateId.trim() !== "") {
                const cleanId = stateId.trim();
                this.subscribedStateIds.add(cleanId);
                this.subscribeForeignStates(cleanId);
                this.log.debug(`Subscribed to EMS state: ${cleanId} (${key})`);
            }
        }

        // 2. Custom Devices & Sensors Table
        if (Array.isArray(this.config.customDevices)) {
            for (const item of this.config.customDevices) {
                if (item && item.enabled !== false && item.id && item.id.trim() !== "") {
                    const cleanId = item.id.trim();
                    this.subscribedStateIds.add(cleanId);
                    this.subscribeForeignStates(cleanId);
                    this.log.debug(`Subscribed to Custom Device state: ${cleanId} -> ${item.identifier}`);
                }
            }
        }

        this.log.info(`Monitoring ${this.subscribedStateIds.size} ioBroker states for Sharegy.`);
    }

    /**
     * Is called when a subscribed state changes in ioBroker
     */
    async onStateChange(id, state) {
        if (!state || state.val === null || state.val === undefined) {
            return;
        }

        this.log.debug(`State changed: ${id} = ${state.val} (ack: ${state.ack})`);

        // Check if this state belongs to EMS or Custom Devices
        const payloadsToSend = [];
        const token = (this.config.mqttToken || "").trim();
        if (!token) return;

        // A) EMS Checks
        if (id === this.config.pvPowerId) {
            payloadsToSend.push({
                identifier: "pv",
                metric: "power",
                unit: "W",
                role: "producer",
                value: this.normalizePowerValue(state.val),
            });
        }

        if (id === this.config.gridPowerId) {
            let gridVal = this.normalizePowerValue(state.val);
            if (this.config.gridPowerSign === "pos_export") {
                gridVal = -gridVal; // Convert so + is import, - is export
            }
            payloadsToSend.push({
                identifier: "grid",
                metric: "power",
                unit: "W",
                role: "grid",
                value: gridVal,
            });
        }

        if (id === this.config.gridImportId) {
            payloadsToSend.push({
                identifier: "grid_meter",
                metric: "energy_import",
                unit: "kWh",
                value: Number(state.val),
            });
        }

        if (id === this.config.gridExportId) {
            payloadsToSend.push({
                identifier: "grid_meter",
                metric: "energy_export",
                unit: "kWh",
                value: Number(state.val),
            });
        }

        if (id === this.config.batteryPowerId) {
            payloadsToSend.push({
                identifier: "battery",
                metric: "power",
                unit: "W",
                role: "battery",
                value: this.normalizePowerValue(state.val),
            });
        }

        if (id === this.config.batterySocId) {
            payloadsToSend.push({
                identifier: "battery",
                metric: "soc",
                unit: "%",
                role: "battery",
                value: Math.max(0, Math.min(100, Number(state.val))),
            });
        }

        if (id === this.config.houseConsumptionId) {
            payloadsToSend.push({
                identifier: "house",
                metric: "power",
                unit: "W",
                role: "consumer",
                value: this.normalizePowerValue(state.val),
            });
        }

        // B) Custom Devices Table Check
        if (Array.isArray(this.config.customDevices)) {
            for (const item of this.config.customDevices) {
                if (item && item.enabled !== false && item.id === id) {
                    const identifier = (item.identifier || "device_1").trim();
                    const scale = Number(item.scale) || 1;
                    const valFloat = Number(state.val) * scale;

                    payloadsToSend.push({
                        identifier,
                        metric: item.metric || "value",
                        unit: item.unit || "W",
                        role: item.role || "sensor",
                        value: valFloat,
                    });
                }
            }
        }

        // Send all resolved payloads (with throttling)
        for (const p of payloadsToSend) {
            this.enqueueTelemetry(p);
        }
    }

    /**
     * Enqueue and throttle telemetry transmissions
     */
    enqueueTelemetry(telemetry) {
        const key = `${telemetry.identifier}_${telemetry.metric}`;
        this.pendingUpdates.set(key, telemetry);

        if (!this.throttleTimer) {
            const minIntervalMs = Math.max(1, Number(this.config.minSendIntervalSec) || 5) * 1000;
            this.throttleTimer = setTimeout(() => {
                this.flushPendingUpdates();
            }, 300); // 300ms collection window for simultaneous state changes
        }
    }

    /**
     * Flush all queued telemetry updates to MQTT
     */
    flushPendingUpdates() {
        this.throttleTimer = null;
        if (!this.mqttClient || !this.mqttClient.connected) {
            this.log.debug("MQTT not connected, skipping telemetry transmission.");
            return;
        }

        const token = (this.config.mqttToken || "").trim();
        const nowSec = Math.floor(Date.now() / 1000);

        for (const [key, t] of this.pendingUpdates.entries()) {
            const topic = `h/${token}/${t.identifier}`;
            const payload = {
                val: t.value,
                value: t.value,
                unit: t.unit,
                metric: t.metric,
                role: t.role,
                ts: nowSec,
                source: "iobroker.sharegy",
            };

            this.mqttClient.publish(topic, JSON.stringify(payload), { qos: 0, retain: false }, (err) => {
                if (err) {
                    this.log.warn(`Failed to publish to ${topic}: ${err.message}`);
                } else {
                    this.log.debug(`Published to ${topic}: ${JSON.stringify(payload)}`);
                }
            });
        }

        this.pendingUpdates.clear();
        this.setState("info.lastSync", new Date().toISOString(), true);
    }

    /**
     * Handle incoming control commands from Sharegy (Rückkanal)
     */
    async handleIncomingMqttMessage(topic, payloadBuffer) {
        const payloadStr = payloadBuffer.toString();
        this.log.info(`Received command from Sharegy on topic [${topic}]: ${payloadStr}`);

        await this.setStateAsync("control.lastCommand", payloadStr, true);

        // Parse Topic to extract identifier: h/<token>/<identifier>/set
        const parts = topic.split("/");
        if (parts.length < 3) return;

        const identifier = parts[2]; // e.g. bwwp_sg_ready, wallbox_power, relay1

        let data = {};
        try {
            data = JSON.parse(payloadStr);
        } catch (e) {
            // Plain string or number (e.g. "true", "1", "16")
            if (payloadStr === "true" || payloadStr === "1") data = { val: true };
            else if (payloadStr === "false" || payloadStr === "0") data = { val: false };
            else data = { val: payloadStr };
        }

        const rawVal = data.val !== undefined ? data.val : (data.value !== undefined ? data.value : (data.state !== undefined ? data.state : data));

        // Search in controlObjects table
        if (Array.isArray(this.config.controlObjects)) {
            for (const ctrl of this.config.controlObjects) {
                if (ctrl && ctrl.enabled !== false && ctrl.identifier === identifier && ctrl.targetId) {
                    const targetId = ctrl.targetId.trim();
                    let finalVal = rawVal;

                    if (ctrl.controlType === "switch_boolean") {
                        let boolVal = (rawVal === true || rawVal === "true" || rawVal === 1 || rawVal === "1" || rawVal === "ON" || rawVal === "on");
                        if (ctrl.invert) boolVal = !boolVal;
                        finalVal = boolVal;
                    } else if (ctrl.controlType === "power_limit_watt" || ctrl.controlType === "current_limit_amper" || ctrl.controlType === "soc_target_pct" || ctrl.controlType === "temperature_setpoint") {
                        finalVal = Number(rawVal);
                    }

                    this.log.info(`Executing Sharegy Control: Writing [${finalVal}] to ioBroker object [${targetId}]`);
                    await this.setForeignStateAsync(targetId, finalVal);
                }
            }
        }
    }

    /**
     * Send current snapshot of all subscribed objects
     */
    async publishAllStates() {
        for (const id of this.subscribedStateIds) {
            try {
                const state = await this.getForeignStateAsync(id);
                if (state) {
                    await this.onStateChange(id, state);
                }
            } catch (e) {
                this.log.debug(`Could not read state ${id}: ${e.message}`);
            }
        }
    }

    /**
     * Helper to normalize power values (auto-detect kW vs W)
     */
    normalizePowerValue(val) {
        let num = Number(val) || 0;
        // If absolute value is very small (< 40) but positive and not 0, it might be in kW (e.g. 5.4 kW -> 5400 W)
        // However, keep raw number if already in Watts
        return num;
    }

    /**
     * Is called when adapter shuts down
     */
    onUnload(callback) {
        try {
            if (this.throttleTimer) {
                clearTimeout(this.throttleTimer);
                this.throttleTimer = null;
            }
            if (this.mqttClient) {
                this.mqttClient.end(true);
                this.mqttClient = null;
            }
            this.setState("info.connection", false, true);
            this.log.info("Sharegy adapter stopped cleanly.");
            callback();
        } catch (e) {
            callback();
        }
    }
}

if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options) => new SharegyAdapter(options);
} else {
    // otherwise start the instance directly
    new SharegyAdapter();
}
