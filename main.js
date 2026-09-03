"use strict";

/*
 * ioBroker.sharegy Adapter
 * (C) 2026 Sharegy Team <info@sharegy.de>
 * License: MIT
 */

const utils = require("@iobroker/adapter-core");
const mqtt = require("mqtt");
const WebSocket = globalThis.WebSocket || require("ws");

const CANONICAL_METRIC_UNITS = {
    temperature: "°C",
    power: "W",
    soc: "%",
    voltage: "V",
    current: "A",
    energy: "kWh",
    humidity: "%",
    frequency: "Hz",
    pressure: "hPa",
    co2: "ppm",
    heat_power: "kW",
};

class SharegyAdapter extends utils.Adapter {

    constructor(options = {}) {
        super({
            ...options,
            name: "sharegy",
        });

        this.mqttClient = null;
        this.wsClient = null;
        this.reconnectTimer = null;
        this.subscribedStateIds = new Set();
        this.lastSentTimestamps = new Map();
        this.pendingUpdates = new Map();
        this.throttleTimer = null;
        this.offlineBuffer = [];
        this.isDrainingBuffer = false;

        this.on("ready", this.onReady.bind(this));
        this.on("stateChange", this.onStateChange.bind(this));
        this.on("unload", this.onUnload.bind(this));
    }

    /**
     * Extracts or resolves the effective Sharegy Home Token
     */
    getEffectiveToken() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        if (proto === "wss" && this.config.wsUrl) {
            const urlStr = this.config.wsUrl.trim();
            const match = urlStr.match(/\/ws\/energy\/([a-zA-Z0-9_-]+)/);
            if (match && match[1]) {
                return match[1].trim();
            }
        }
        return (this.config.mqttToken || "").trim();
    }

    /**
     * Is called when databases are connected and adapter received configuration.
     */
    async onReady() {
        this.log.info("Starting Sharegy Energy Management Adapter...");

        // Reset connection status and buffer counter
        await this.setStateAsync("info.connection", false, true);
        await this.setStateAsync("info.bufferedCount", 0, true);

        const token = this.getEffectiveToken();
        if (!token && (this.config.protocol || "wss").toLowerCase() !== "wss") {
            this.log.error("No Sharegy Home Token configured! Please enter your token in the adapter settings.");
            return;
        }

        // Initialize Connection
        this.connect();

        // Subscribe to configured EMS and Custom Device states
        this.initSubscriptions();
    }

    /**
     * Connect to Sharegy (WSS WebSocket or MQTTS)
     */
    connect() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        const token = this.getEffectiveToken();

        // ==========================================
        // 1. WSS (Native Secure WebSocket over 443)
        // ==========================================
        if (proto === "wss") {
            let wsUrl = (this.config.wsUrl || "").trim();
            if (!wsUrl || !wsUrl.startsWith("wss://")) {
                wsUrl = `wss://sharegy.de/ws/energy/${token}/`;
            } else if (!wsUrl.endsWith("/")) {
                wsUrl += "/";
            }

            this.log.info(`Connecting to Sharegy via Secure WebSocket (WSS) at ${wsUrl}...`);

            try {
                if (this.wsClient) {
                    try { this.wsClient.close(); } catch (e) {}
                    this.wsClient = null;
                }

                this.wsClient = new WebSocket(wsUrl);

                this.wsClient.onopen = () => {
                    this.log.info("Connected to Sharegy WebSocket (WSS) successfully!");
                    this.setState("info.connection", true, true);
                    this.drainOfflineBuffer();
                    this.publishAllStates();
                };

                this.wsClient.onmessage = (event) => {
                    this.handleIncomingWsMessage(event.data);
                };

                this.wsClient.onerror = (err) => {
                    this.log.warn(`WebSocket error: ${err.message || err}`);
                    this.setState("info.connection", false, true);
                };

                this.wsClient.onclose = () => {
                    this.log.debug("WebSocket connection closed. Reconnecting in 5 seconds...");
                    this.setState("info.connection", false, true);
                    if (!this.reconnectTimer) {
                        this.reconnectTimer = setTimeout(() => {
                            this.reconnectTimer = null;
                            this.connect();
                        }, 5000);
                    }
                };

            } catch (e) {
                this.log.error(`Failed to create WebSocket client: ${e.message}`);
            }
            return;
        }

        // ==========================================
        // 2. MQTTS (MQTT over TLS Port 8883)
        // ==========================================
        const host = (this.config.host || "sharegy.de").trim();
        const port = Number(this.config.port) || 8883;
        const url = `mqtts://${host}:${port}`;
        const clientId = `iobroker_sharegy_${this.instance}_${Math.random().toString(16).substring(2, 8)}`;

        const options = {
            clientId,
            clean: true,
            connectTimeout: 10000,
            reconnectPeriod: 5000,
            rejectUnauthorized: true,
        };

        if (this.config.mqttUsername) options.username = this.config.mqttUsername.trim();
        if (this.config.mqttPassword) options.password = this.config.mqttPassword;

        this.log.info(`Connecting to Sharegy via MQTTS at ${url} (ClientID: ${clientId})...`);

        try {
            this.mqttClient = mqtt.connect(url, options);

            this.mqttClient.on("connect", () => {
                this.log.info("Connected to Sharegy MQTT Broker successfully!");
                this.setState("info.connection", true, true);

                const controlTopicWildcard = `h/${token}/+/set`;
                const globalControlTopic = `h/${token}/control/#`;

                this.mqttClient.subscribe([controlTopicWildcard, globalControlTopic], (err) => {
                    if (err) {
                        this.log.error(`Failed to subscribe to control topics: ${err.message}`);
                    } else {
                        this.log.info(`Subscribed to Sharegy control channels: ${controlTopicWildcard}, ${globalControlTopic}`);
                    }
                });

                this.drainOfflineBuffer();
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
     * Check if connection is currently active
     */
    isConnectionActive() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        if (proto === "wss") {
            return this.wsClient && this.wsClient.readyState === 1; // 1 = OPEN
        }
        return this.mqttClient && this.mqttClient.connected;
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

        // 3. Bidirectional Control & Feedback Objects (Rückkanal Ist-Zustände)
        if (Array.isArray(this.config.controlObjects)) {
            for (const item of this.config.controlObjects) {
                if (item && item.enabled !== false && item.targetId && item.targetId.trim() !== "") {
                    const cleanId = item.targetId.trim();
                    this.subscribedStateIds.add(cleanId);
                    this.subscribeForeignStates(cleanId);
                    this.log.debug(`Subscribed to Control Object feedback state: ${cleanId} -> ${item.identifier}`);
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

        const payloadsToSend = [];
        const token = this.getEffectiveToken();

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
                gridVal = -gridVal;
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
                    const metric = item.metric || "value";
                    const unit = CANONICAL_METRIC_UNITS[metric] || item.unit || "W";

                    payloadsToSend.push({
                        identifier,
                        metric,
                        unit,
                        role: item.role || "sensor",
                        value: valFloat,
                    });
                }
            }
        }

        // C) Control Objects Feedback (Ist-Zustand Rückmeldung für Relais & Schalter)
        if (Array.isArray(this.config.controlObjects)) {
            for (const item of this.config.controlObjects) {
                if (item && item.enabled !== false && item.targetId && item.targetId.trim() === id) {
                    const identifier = (item.identifier || "switch_1").trim();
                    let boolVal = (state.val === true || state.val === "true" || state.val === 1 || state.val === "1" || state.val === "ON" || state.val === "on");
                    if (item.invert) boolVal = !boolVal;

                    payloadsToSend.push({
                        identifier,
                        state: boolVal,
                        relay_state: boolVal,
                        val: boolVal,
                        value: typeof state.val === "number" ? state.val : (boolVal ? 1 : 0),
                        role: "consumer",
                        metric: "relay_state",
                    });
                    this.log.debug(`Control feedback for [${identifier}]: state=${boolVal} (from ${id})`);
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
            }, 300);
        }
    }

    /**
     * Flush all queued telemetry updates (or buffer if offline)
     */
    flushPendingUpdates() {
        this.throttleTimer = null;
        const isConnected = this.isConnectionActive();
        const token = this.getEffectiveToken();
        const nowSec = Math.floor(Date.now() / 1000);
        const shouldBuffer = this.config.bufferOfflineData !== false;
        const maxBuffer = Math.max(100, Number(this.config.maxBufferSize) || 5000);

        // If offline: push into offline ring buffer
        if (!isConnected) {
            if (shouldBuffer) {
                for (const [key, t] of this.pendingUpdates.entries()) {
                    const topic = `h/${token}/${t.identifier}`;
                    const payload = {
                        val: t.value !== undefined ? t.value : t.val,
                        value: t.value !== undefined ? t.value : t.val,
                        state: t.state,
                        relay_state: t.relay_state,
                        unit: t.unit,
                        metric: t.metric,
                        role: t.role,
                        ts: nowSec,
                        source: "iobroker.sharegy",
                        device: t.identifier,
                        id: t.identifier,
                    };

                    this.offlineBuffer.push({ topic, payload });

                    while (this.offlineBuffer.length > maxBuffer) {
                        this.offlineBuffer.shift();
                    }
                }

                this.setState("info.bufferedCount", this.offlineBuffer.length, true);
                this.log.debug(`Connection offline: Queued ${this.pendingUpdates.size} packets in offline buffer (Total buffered: ${this.offlineBuffer.length})`);
            } else {
                this.log.debug("Connection offline and buffering disabled, dropping telemetry update.");
            }

            this.pendingUpdates.clear();
            return;
        }

        // When connected: send immediately
        for (const [key, t] of this.pendingUpdates.entries()) {
            const topic = `h/${token}/${t.identifier}`;
            const payload = {
                val: t.value !== undefined ? t.value : t.val,
                value: t.value !== undefined ? t.value : t.val,
                state: t.state,
                relay_state: t.relay_state,
                unit: t.unit,
                metric: t.metric,
                role: t.role,
                ts: nowSec,
                source: "iobroker.sharegy",
                device: t.identifier,
                id: t.identifier,
            };

            this.sendTelemetryPacket(topic, payload);
        }

        this.pendingUpdates.clear();
        this.setState("info.lastSync", new Date().toISOString(), true);
    }

    /**
     * Low-level send method supporting both WebSocket and MQTT
     */
    sendTelemetryPacket(topic, payload) {
        const proto = (this.config.protocol || "wss").toLowerCase();
        if (proto === "wss") {
            if (this.wsClient && this.wsClient.readyState === 1) {
                this.wsClient.send(JSON.stringify(payload));
                this.log.debug(`Sent via WSS: ${JSON.stringify(payload)}`);
                return true;
            }
            return false;
        } else {
            if (this.mqttClient && this.mqttClient.connected) {
                this.mqttClient.publish(topic, JSON.stringify(payload), { qos: 0, retain: false }, (err) => {
                    if (err) {
                        this.log.warn(`Failed to publish to ${topic}: ${err.message}`);
                    } else {
                        this.log.debug(`Published to ${topic}: ${JSON.stringify(payload)}`);
                    }
                });
                return true;
            }
            return false;
        }
    }

    /**
     * Drain queued offline packets in batched chunks after reconnection
     */
    async drainOfflineBuffer() {
        if (this.isDrainingBuffer || this.offlineBuffer.length === 0) return;
        if (!this.isConnectionActive()) return;

        this.isDrainingBuffer = true;
        const totalToDrain = this.offlineBuffer.length;
        this.log.info(`Reconnected! Draining ${totalToDrain} buffered offline telemetry packets to Sharegy...`);

        const batchSize = 25;
        while (this.offlineBuffer.length > 0 && this.isConnectionActive()) {
            const chunk = this.offlineBuffer.splice(0, batchSize);

            for (const item of chunk) {
                this.sendTelemetryPacket(item.topic, item.payload);
            }

            await this.setStateAsync("info.bufferedCount", this.offlineBuffer.length, true);
            await new Promise(resolve => setTimeout(resolve, 50));
        }

        this.isDrainingBuffer = false;
        await this.setStateAsync("info.bufferedCount", this.offlineBuffer.length, true);
        this.log.info(`Successfully drained offline buffer (${totalToDrain} packets sent).`);
    }

    /**
     * Handle incoming messages from Sharegy WebSocket
     */
    async handleIncomingWsMessage(msgData) {
        const payloadStr = msgData.toString();
        this.log.info(`Received message from Sharegy over WSS: ${payloadStr}`);
        await this.setStateAsync("control.lastCommand", payloadStr, true);

        let data = {};
        try {
            data = JSON.parse(payloadStr);
        } catch (e) {
            return;
        }

        // 1. Shelly RPC Relay Command: {"method": "Switch.Set", "params": {"id": 0, "on": true}}
        if (data.method && data.method.startsWith("Switch.")) {
            const onVal = data.params?.on;
            if (onVal !== undefined && Array.isArray(this.config.controlObjects)) {
                for (const ctrl of this.config.controlObjects) {
                    if (ctrl && ctrl.enabled !== false && ctrl.targetId) {
                        let finalVal = onVal;
                        if (ctrl.invert) finalVal = !finalVal;
                        this.log.info(`Executing Sharegy Switch command: Writing [${finalVal}] to [${ctrl.targetId}]`);
                        await this.setForeignStateAsync(ctrl.targetId.trim(), finalVal);
                    }
                }
            }
        }

        // 2. Standard Sharegy Command: {"identifier": "bwwp_sg_ready", "val": true}
        const identifier = data.identifier || data.device || data.src;
        if (identifier && Array.isArray(this.config.controlObjects)) {
            for (const ctrl of this.config.controlObjects) {
                if (ctrl && ctrl.enabled !== false && ctrl.identifier === identifier && ctrl.targetId) {
                    const rawVal = data.val !== undefined ? data.val : (data.value !== undefined ? data.value : data.state);
                    let finalVal = rawVal;
                    if (ctrl.controlType === "switch_boolean") {
                        let boolVal = (rawVal === true || rawVal === "true" || rawVal === 1 || rawVal === "1" || rawVal === "ON" || rawVal === "on");
                        if (ctrl.invert) boolVal = !boolVal;
                        finalVal = boolVal;
                    } else {
                        finalVal = Number(rawVal);
                    }
                    this.log.info(`Executing Sharegy Control: Writing [${finalVal}] to [${ctrl.targetId}]`);
                    await this.setForeignStateAsync(ctrl.targetId.trim(), finalVal);
                }
            }
        }
    }

    /**
     * Handle incoming control commands from Sharegy MQTT
     */
    async handleIncomingMqttMessage(topic, payloadBuffer) {
        const payloadStr = payloadBuffer.toString();
        this.log.info(`Received command from Sharegy on topic [${topic}]: ${payloadStr}`);
        await this.setStateAsync("control.lastCommand", payloadStr, true);

        const parts = topic.split("/");
        if (parts.length < 3) return;
        const identifier = parts[2];

        let data = {};
        try {
            data = JSON.parse(payloadStr);
        } catch (e) {
            if (payloadStr === "true" || payloadStr === "1") data = { val: true };
            else if (payloadStr === "false" || payloadStr === "0") data = { val: false };
            else data = { val: payloadStr };
        }

        const rawVal = data.val !== undefined ? data.val : (data.value !== undefined ? data.value : (data.state !== undefined ? data.state : data));

        if (Array.isArray(this.config.controlObjects)) {
            for (const ctrl of this.config.controlObjects) {
                if (ctrl && ctrl.enabled !== false && ctrl.identifier === identifier && ctrl.targetId) {
                    const targetId = ctrl.targetId.trim();
                    let finalVal = rawVal;

                    if (ctrl.controlType === "switch_boolean") {
                        let boolVal = (rawVal === true || rawVal === "true" || rawVal === 1 || rawVal === "1" || rawVal === "ON" || rawVal === "on");
                        if (ctrl.invert) boolVal = !boolVal;
                        finalVal = boolVal;
                    } else {
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
        return Number(val) || 0;
    }

    /**
     * Is called when adapter shuts down
     */
    onUnload(callback) {
        try {
            if (this.reconnectTimer) {
                clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }
            if (this.throttleTimer) {
                clearTimeout(this.throttleTimer);
                this.throttleTimer = null;
            }
            if (this.wsClient) {
                try { this.wsClient.close(); } catch (e) {}
                this.wsClient = null;
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
    module.exports = (options) => new SharegyAdapter(options);
} else {
    new SharegyAdapter();
}
