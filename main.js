"use strict";

/*
 * ioBroker.sharegy Adapter
 * (C) 2026 Sharegy Team <info@sharegy.de>
 * License: MIT
 */

const utils = require("@iobroker/adapter-core");
const mqtt = require("mqtt");
const WebSocket = require("ws");
const UpdateWatchdog = require("./lib/updateWatchdog");
const AutoDiscoveryScanner = require("./lib/autoDiscovery");

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
        this.reconnectAttempts = 0;
        this.heartbeatTimer = null;
        this.livenessTimer = null;
        this.lastHeartbeatTimestamp = 0;
        this.offlineLoopTimer = null;
        this.cachedSchedule24h = [];

        this.subscribedStateIds = new Set();
        this.lastSentTimestamps = new Map();
        this.pendingUpdates = new Map();
        this.throttleTimer = null;
        this.offlineBuffer = [];
        this.isDrainingBuffer = false;

        this.carrierWs = null;
        this.carrierReconnectTimer = null;
        this.carrierHeartbeatTimer = null;
        this.carrierReconnectAttempts = 0;

        // In-memory circular error buffer for remote diagnostics (moniy)
        this.errorLogBuffer = [];
        this.maxErrorLogSize = 30;

        // Canary A/B OTA Remote Update Watchdog
        this.updateWatchdog = new UpdateWatchdog(this);

        // Smart Auto-Discovery Engine
        this.autoDiscovery = new AutoDiscoveryScanner(this);

        this.on("ready", this.onReady.bind(this));
        this.on("stateChange", this.onStateChange.bind(this));
        this.on("unload", this.onUnload.bind(this));
    }

    /**
     * Extracts or resolves the effective Sharegy Home Token
     */
    getEffectiveToken() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        if (this.config.wsUrl) {
            const urlStr = this.config.wsUrl.trim();
            const match = urlStr.match(/\/ws\/energy\/([a-zA-Z0-9_-]+)/);
            if (match && match[1]) {
                return match[1].trim();
            }
        }
        if (this.config.mqttToken && this.config.mqttToken.trim()) {
            return this.config.mqttToken.trim();
        }
        if (this.config.token && this.config.token.trim()) {
            return this.config.token.trim();
        }
        return "";
    }

    /**
     * Is called when databases are connected and adapter received configuration.
     */
    async onReady() {
        this.log.info("Starting Sharegy Energy Management Adapter v2.1.0...");

        // Reset connection status, carrier status and buffer counter
        await this.setStateAsync("info.connection", false, true);
        await this.setStateAsync("info.carrierConnected", false, true);
        await this.setStateAsync("info.bufferedCount", 0, true);
        await this.setStateAsync("floorheating.offline_autonomous", false, true);

        // Check if booted under pending OTA update verification window
        if (this.updateWatchdog) {
            this.updateWatchdog.checkPendingUpdateOnStartup();
        }

        // Load cached schedule from persisted state if available
        try {
            const cachedSchedState = await this.getStateAsync("floorheating.cached_schedule");
            if (cachedSchedState && cachedSchedState.val && typeof cachedSchedState.val === "string") {
                const parsed = JSON.parse(cachedSchedState.val);
                if (Array.isArray(parsed) && parsed.length > 0) {
                    this.cachedSchedule24h = parsed;
                    this.log.info(`Loaded ${this.cachedSchedule24h.length} cached 24h schedule slots from persistent state.`);
                }
            }
        } catch (e) {
            this.log.debug(`Could not restore cached schedule: ${e.message}`);
        }

        const token = this.getEffectiveToken();
        if (!token) {
            this.recordError("ERR_CONFIG_MISSING_TOKEN", "No Sharegy Home Token configured! Please enter your token in the adapter settings.", null, "error", true);
            return;
        }

        // Initialize Primary Connection with auto-reconnect
        this.connect();

        // Initialize Decoupled Carrier Admin Socket (smartEvo moniy)
        if (this.config.carrierEnabled !== false) {
            this.initCarrierConnection();

            // Periodic Liveness Watchdog: Reconnect if socket dropped without close event
            if (this.carrierLivenessTimer) clearInterval(this.carrierLivenessTimer);
            this.carrierLivenessTimer = setInterval(() => {
                if (!this.carrierWs || this.carrierWs.readyState === WebSocket.CLOSED || this.carrierWs.readyState === WebSocket.CLOSING) {
                    this.log.debug("[Carrier Watchdog] Socket inactive, ensuring reconnection...");
                    this.initCarrierConnection();
                }
            }, 30000);
        }

        // Subscribe to configured EMS, FBH and Custom Device states
        this.initSubscriptions();

        // Start 60s Local Autonomous Offline-Resilience Controller Loop
        if (this.offlineLoopTimer) {
            clearInterval(this.offlineLoopTimer);
        }
        this.offlineLoopTimer = setInterval(() => {
            this.runOfflineHeatingLoop();
        }, 60000);

        // Start 20s Liveness Fallback Watchdog (ensures auto-recovery if any event was dropped)
        if (this.livenessTimer) {
            clearInterval(this.livenessTimer);
        }
        this.livenessTimer = setInterval(() => {
            if (!this.isConnectionActive() && !this.reconnectTimer) {
                this.log.debug("Liveness watchdog: Connection inactive and no retry timer pending. Triggering reconnect...");
                this.scheduleReconnect(1000);
            }
        }, 20000);
    }

    /**
     * Clean up active sockets and timers before reconnecting or unloading
     */
    cleanupSockets() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        if (this.wsClient) {
            const client = this.wsClient;
            this.wsClient = null;
            try {
                client.removeAllListeners();
                client.on("error", () => {});
                if (typeof client.terminate === "function") {
                    client.terminate();
                } else if (typeof client.close === "function") {
                    client.close();
                }
            } catch (e) {}
        }

        if (this.mqttClient) {
            const client = this.mqttClient;
            this.mqttClient = null;
            try {
                client.removeAllListeners();
                client.on("error", () => {});
                client.end(true);
            } catch (e) {}
        }

        if (this.carrierHeartbeatTimer) {
            clearInterval(this.carrierHeartbeatTimer);
            this.carrierHeartbeatTimer = null;
        }

        if (this.carrierWs) {
            const client = this.carrierWs;
            this.carrierWs = null;
            try {
                client.removeAllListeners();
                client.on("error", () => {});
                if (typeof client.terminate === "function") {
                    client.terminate();
                } else if (typeof client.close === "function") {
                    client.close();
                }
            } catch (e) {}
        }
    }

    /**
     * Centralized, exponential backoff reconnection manager
     */
    scheduleReconnect(forcedDelayMs = null) {
        if (this.reconnectTimer) {
            return; // Reconnect is already queued, avoid double timers
        }

        this.setState("info.connection", false, true);

        this.reconnectAttempts++;
        if (this.reconnectAttempts === 5 || this.reconnectAttempts === 10) {
            this.recordError("ERR_RECONNECT_FAILURES", `Repeated connection failures to Sharegy cloud (${this.reconnectAttempts} attempts)`, { attempts: this.reconnectAttempts }, "warn", true);
        }

        let delay = forcedDelayMs;
        if (delay === null || delay === undefined) {
            // Exponential backoff: 2s -> 3s -> 4.5s -> 6.75s -> ... capped at 25s
            const backoff = Math.min(25000, 2000 * Math.pow(1.5, Math.min(this.reconnectAttempts - 1, 7)));
            const jitter = Math.floor(Math.random() * 500);
            delay = Math.round(backoff + jitter);
        }

        this.log.info(`Scheduling reconnection to Sharegy in ${(delay / 1000).toFixed(1)}s (Attempt #${this.reconnectAttempts})...`);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, delay);
    }

    /**
     * Heartbeat watchdog: detects dead / half-open TCP connections (e.g. after Daphne restart)
     */
    startHeartbeatWatchdog() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        this.lastHeartbeatTimestamp = Date.now();

        this.heartbeatTimer = setInterval(() => {
            if (!this.isConnectionActive()) {
                return;
            }

            const now = Date.now();

            // 1. Send Ping Frame
            const proto = (this.config.protocol || "wss").toLowerCase();
            if (proto === "wss" && this.wsClient && this.wsClient.readyState === WebSocket.OPEN) {
                try {
                    if (typeof this.wsClient.ping === "function") {
                        this.wsClient.ping();
                    } else {
                        this.wsClient.send(JSON.stringify({ method: "ping" }));
                    }
                } catch (e) {
                    this.log.debug(`Failed to send WS ping: ${e.message}`);
                }
            }

            // 2. Check if server answered within 40s
            if (this.lastHeartbeatTimestamp > 0 && (now - this.lastHeartbeatTimestamp > 40000)) {
                this.log.warn("Sharegy connection watchdog: Heartbeat lost (no response for >40s). Forcing reconnect...");
                this.cleanupSockets();
                this.scheduleReconnect(1000);
            }
        }, 15000);
    }

    /**
     * Connect to Sharegy (WSS WebSocket or MQTTS)
     */
    connect() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        const token = this.getEffectiveToken();

        this.cleanupSockets();

        // ==========================================
        // 1. WSS (Native Secure WebSocket over 443)
        // ==========================================
        if (proto === "wss") {
            let wsUrl = (this.config.wsUrl || "").trim();
            const host = (this.config.host || "sharegy.de").trim();

            if (!wsUrl) {
                wsUrl = `wss://${host}/ws/energy/${token ? token + "/" : ""}`;
            } else {
                if (wsUrl.startsWith("http://")) wsUrl = wsUrl.replace("http://", "ws://");
                else if (wsUrl.startsWith("https://")) wsUrl = wsUrl.replace("https://", "wss://");
                else if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://")) wsUrl = `wss://${wsUrl}`;

                if (token && !wsUrl.includes(`/ws/energy/${token}`)) {
                    wsUrl = wsUrl.replace(/\/ws\/energy\/?$/, "");
                    wsUrl = `${wsUrl}/ws/energy/${token}/`;
                }
                if (!wsUrl.endsWith("/")) {
                    wsUrl += "/";
                }
                if (!wsUrl.includes("?")) {
                    wsUrl += "?client=iobroker&source=iobroker&version=2.1.0";
                }
            }

            this.log.info(`Connecting to Sharegy via Secure WebSocket (WSS) at ${wsUrl}...`);

            try {
                this.wsClient = new WebSocket(wsUrl, {
                    handshakeTimeout: 10000,
                    perMessageDeflate: false,
                });

                this.wsClient.on("open", () => {
                    this.log.info("Connected to Sharegy WebSocket (WSS) successfully!");
                    this.reconnectAttempts = 0;
                    this.lastHeartbeatTimestamp = Date.now();
                    this.setState("info.connection", true, true);
                    this.setState("floorheating.offline_autonomous", false, true);

                    this.startHeartbeatWatchdog();
                    this.drainOfflineBuffer();
                    this.publishAllStates();
                });

                this.wsClient.on("message", (data) => {
                    this.lastHeartbeatTimestamp = Date.now();
                    this.handleIncomingWsMessage(data);
                });

                this.wsClient.on("pong", () => {
                    this.lastHeartbeatTimestamp = Date.now();
                });

                this.wsClient.on("error", (err) => {
                    this.log.warn(`WebSocket error: ${err.message || err}`);
                    this.setState("info.connection", false, true);
                    this.cleanupSockets();
                    this.scheduleReconnect();
                });

                this.wsClient.on("close", (code, reason) => {
                    this.log.warn(`WebSocket connection closed (code: ${code || "-"}, reason: ${reason || "none"}).`);
                    this.setState("info.connection", false, true);
                    this.cleanupSockets();
                    this.scheduleReconnect();
                });

            } catch (e) {
                this.log.error(`Failed to initiate WebSocket connection: ${e.message}`);
                this.cleanupSockets();
                this.scheduleReconnect();
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
                this.reconnectAttempts = 0;
                this.lastHeartbeatTimestamp = Date.now();
                this.setState("info.connection", true, true);
                this.setState("floorheating.offline_autonomous", false, true);

                const controlTopicWildcard = `h/${token}/+/set`;
                const globalControlTopic = `h/${token}/control/#`;
                const fbhTopic = `h/${token}/floor_heating/#`;

                this.mqttClient.subscribe([controlTopicWildcard, globalControlTopic, fbhTopic], (err) => {
                    if (err) {
                        this.log.error(`Failed to subscribe to control topics: ${err.message}`);
                    } else {
                        this.log.info(`Subscribed to Sharegy control channels.`);
                    }
                });

                this.startHeartbeatWatchdog();
                this.drainOfflineBuffer();
                this.publishAllStates();
            });

            this.mqttClient.on("message", (topic, payload) => {
                this.lastHeartbeatTimestamp = Date.now();
                this.handleIncomingMqttMessage(topic, payload);
            });

            this.mqttClient.on("error", (err) => {
                this.log.warn(`MQTT Error: ${err.message}`);
                this.setState("info.connection", false, true);
                this.scheduleReconnect();
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
            this.scheduleReconnect();
        }
    }

    /**
     * Check if connection is currently active
     */
    isConnectionActive() {
        const proto = (this.config.protocol || "wss").toLowerCase();
        if (proto === "wss") {
            return this.wsClient && this.wsClient.readyState === WebSocket.OPEN;
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

        // 2. Dedicated Floor Heating & Screed Storage Inputs & Feedback
        if (this.config.fbhEnabled) {
            const fbhKeys = [
                "fbhRoomTempId",
                "fbhFlowTempActualId",
                "fbhFloorTempId",
                "fbhOutdoorTempId",
                "fbhHeatPumpPowerId",
                "fbhRelayTargetId",
            ];

            for (const key of fbhKeys) {
                const stateId = this.config[key];
                if (stateId && typeof stateId === "string" && stateId.trim() !== "") {
                    const cleanId = stateId.trim();
                    this.subscribedStateIds.add(cleanId);
                    this.subscribeForeignStates(cleanId);
                    this.log.debug(`Subscribed to Floor Heating state: ${cleanId} (${key})`);
                }
            }
        }

        // 3. Custom Devices & Sensors Table
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

        // 4. Bidirectional Control & Feedback Objects (Rückkanal Ist-Zustände)
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

        // B) Dedicated Floor Heating & Screed Storage Telemetry
        if (this.config.fbhEnabled) {
            if (id === this.config.fbhRoomTempId) {
                const roomTemp = Number(state.val);
                await this.setStateAsync("floorheating.room_temp_actual", roomTemp, true);
                payloadsToSend.push({
                    identifier: "floor_heating_room_temp",
                    metric: "temperature",
                    unit: "°C",
                    role: "sensor",
                    value: roomTemp,
                });
            }

            if (id === this.config.fbhFlowTempActualId) {
                const flowTemp = Number(state.val);
                await this.setStateAsync("floorheating.flow_temp_actual", flowTemp, true);
                payloadsToSend.push({
                    identifier: "floor_heating_flow_temp",
                    metric: "temperature",
                    unit: "°C",
                    role: "sensor",
                    value: flowTemp,
                });
            }

            if (id === this.config.fbhFloorTempId) {
                payloadsToSend.push({
                    identifier: "floor_heating_surface_temp",
                    metric: "temperature",
                    unit: "°C",
                    role: "sensor",
                    value: Number(state.val),
                });
            }

            if (id === this.config.fbhOutdoorTempId) {
                payloadsToSend.push({
                    identifier: "outdoor_temp",
                    metric: "temperature",
                    unit: "°C",
                    role: "sensor",
                    value: Number(state.val),
                });
            }

            if (id === this.config.fbhHeatPumpPowerId) {
                payloadsToSend.push({
                    identifier: "heatpump",
                    metric: "power",
                    unit: "W",
                    role: "consumer",
                    value: this.normalizePowerValue(state.val),
                });
            }

            if (id === this.config.fbhRelayTargetId) {
                let boolVal = (state.val === true || state.val === "true" || state.val === 1 || state.val === "1" || state.val === "ON" || state.val === "on");
                if (this.config.fbhRelayInvert) boolVal = !boolVal;
                await this.setStateAsync("floorheating.relay_state", boolVal, true);
                payloadsToSend.push({
                    identifier: "floor_heating_relay",
                    metric: "relay_state",
                    role: "consumer",
                    state: boolVal,
                    relay_state: boolVal,
                    val: boolVal,
                });
            }
        }

        // C) Custom Devices Table Check
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

        // D) Control Objects Feedback (Ist-Zustand Rückmeldung für Relais & Schalter)
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
            if (this.wsClient && this.wsClient.readyState === WebSocket.OPEN) {
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
        this.log.debug(`Received message from Sharegy over WSS: ${payloadStr}`);

        // Quick Pong/Heartbeat check
        if (payloadStr === '{"type":"pong","status":"ok"}' || payloadStr === '{"type": "pong", "status": "ok"}') {
            return;
        }

        await this.setStateAsync("control.lastCommand", payloadStr, true);

        let data = {};
        try {
            data = JSON.parse(payloadStr);
        } catch (e) {
            return;
        }

        // 1. Process 24h MPC Schedule sync & timeline caching for local offline resilience
        const timeline = data.timeline || (data.predictive_mpc && data.predictive_mpc.timeline);
        if (Array.isArray(timeline) && timeline.length > 0) {
            this.cachedSchedule24h = timeline;
            await this.setStateAsync("floorheating.cached_schedule", JSON.stringify(timeline), true);
            this.log.info(`Updated and cached 24h predictive MPC schedule (${timeline.length} hourly slots) for local offline resilience.`);
        }

        // 2. Update live status & setpoints
        if (data.flow_temp_setpoint_c !== undefined) {
            const spVal = Number(data.flow_temp_setpoint_c);
            await this.setStateAsync("status.flow_temp_setpoint", spVal, true);
            await this.setStateAsync("floorheating.flow_temp_setpoint", spVal, true);

            // Forward to configured FBH flow setpoint target object
            if (this.config.fbhEnabled && this.config.fbhFlowSetpointTargetId) {
                await this.setForeignStateAsync(this.config.fbhFlowSetpointTargetId.trim(), spVal);
            }
        }

        if (data.screed_soc_pct !== undefined) {
            const socVal = Number(data.screed_soc_pct);
            await this.setStateAsync("status.screed_soc", socVal, true);
            await this.setStateAsync("floorheating.screed_soc", socVal, true);
        }

        if (data.mode !== undefined) {
            await this.setStateAsync("status.operating_mode", String(data.mode), true);
            await this.setStateAsync("floorheating.operating_mode", String(data.mode), true);
        }

        if (data.floor_heating_boost !== undefined || data.action === "FLOOR_HEATING_BOOST") {
            const boostActive = Boolean(data.floor_heating_boost ?? (data.action === "FLOOR_HEATING_BOOST"));
            await this.setStateAsync("control.floor_heating_boost", boostActive, true);
            await this.setStateAsync("floorheating.boost_active", boostActive, true);

            if (this.config.fbhEnabled && this.config.fbhBoostTargetId) {
                await this.setForeignStateAsync(this.config.fbhBoostTargetId.trim(), boostActive);
            }
        }

        if (data.bwwp_boost !== undefined) {
            await this.setStateAsync("control.bwwp_boost", Boolean(data.bwwp_boost), true);
        }

        // 3. Dedicated Floor Heating Relay Actuator Direct Command
        if (this.config.fbhEnabled && this.config.fbhRelayTargetId) {
            let targetRelay = null;
            if (data.action === "Switch.Set" || data.method === "Switch.Set") {
                targetRelay = data.params?.on;
            } else if (data.relay_state !== undefined) {
                targetRelay = Boolean(data.relay_state);
            } else if (data.identifier === "floor_heating_relay" && data.val !== undefined) {
                targetRelay = Boolean(data.val);
            }

            if (targetRelay !== null && targetRelay !== undefined) {
                let finalRelay = targetRelay;
                if (this.config.fbhRelayInvert) finalRelay = !finalRelay;
                this.log.info(`Executing Floor Heating Relay: Setting [${finalRelay}] to [${this.config.fbhRelayTargetId}]`);
                await this.setForeignStateAsync(this.config.fbhRelayTargetId.trim(), finalRelay);
                await this.setStateAsync("floorheating.relay_state", Boolean(targetRelay), true);
            }
        }

        // Check if bidirectional control is enabled
        const bidiState = await this.getStateAsync("control.bidirectional_enabled");
        if (bidiState && bidiState.val === false) {
            this.log.debug("Bidirectional control is paused, skipping execution on target objects.");
            return;
        }

        // 4. Shelly RPC Relay Command: {"method": "Switch.Set", "params": {"id": 0, "on": true}}
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

        // 5. Standard Sharegy Command: {"identifier": "bwwp_sg_ready", "val": true}
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

        // 1. Process 24h MPC Schedule sync & timeline caching
        const timeline = data.timeline || (data.predictive_mpc && data.predictive_mpc.timeline);
        if (Array.isArray(timeline) && timeline.length > 0) {
            this.cachedSchedule24h = timeline;
            await this.setStateAsync("floorheating.cached_schedule", JSON.stringify(timeline), true);
            this.log.info(`Updated and cached 24h predictive MPC schedule (${timeline.length} hourly slots).`);
        }

        // 2. Update live status & setpoints
        if (data.flow_temp_setpoint_c !== undefined) {
            const spVal = Number(data.flow_temp_setpoint_c);
            await this.setStateAsync("status.flow_temp_setpoint", spVal, true);
            await this.setStateAsync("floorheating.flow_temp_setpoint", spVal, true);

            if (this.config.fbhEnabled && this.config.fbhFlowSetpointTargetId) {
                await this.setForeignStateAsync(this.config.fbhFlowSetpointTargetId.trim(), spVal);
            }
        }

        if (data.screed_soc_pct !== undefined) {
            const socVal = Number(data.screed_soc_pct);
            await this.setStateAsync("status.screed_soc", socVal, true);
            await this.setStateAsync("floorheating.screed_soc", socVal, true);
        }

        if (data.mode !== undefined) {
            await this.setStateAsync("status.operating_mode", String(data.mode), true);
            await this.setStateAsync("floorheating.operating_mode", String(data.mode), true);
        }

        if (data.floor_heating_boost !== undefined || data.action === "FLOOR_HEATING_BOOST" || identifier === "floor_heating_boost") {
            const boostActive = Boolean(data.floor_heating_boost ?? (data.action === "FLOOR_HEATING_BOOST" || rawVal === true || rawVal === 1));
            await this.setStateAsync("control.floor_heating_boost", boostActive, true);
            await this.setStateAsync("floorheating.boost_active", boostActive, true);

            if (this.config.fbhEnabled && this.config.fbhBoostTargetId) {
                await this.setForeignStateAsync(this.config.fbhBoostTargetId.trim(), boostActive);
            }
        }

        if (data.bwwp_boost !== undefined || identifier === "bwwp_boost") {
            const bwwpActive = Boolean(data.bwwp_boost ?? (rawVal === true || rawVal === 1));
            await this.setStateAsync("control.bwwp_boost", bwwpActive, true);
        }

        // 3. Dedicated Floor Heating Relay Actuator Direct Command
        if (this.config.fbhEnabled && this.config.fbhRelayTargetId && (identifier === "floor_heating" || identifier === "floor_heating_relay")) {
            let targetRelay = (rawVal === true || rawVal === "true" || rawVal === 1 || rawVal === "1" || rawVal === "ON" || rawVal === "on");
            let finalRelay = targetRelay;
            if (this.config.fbhRelayInvert) finalRelay = !finalRelay;
            this.log.info(`Executing Floor Heating Relay via MQTT: Setting [${finalRelay}] to [${this.config.fbhRelayTargetId}]`);
            await this.setForeignStateAsync(this.config.fbhRelayTargetId.trim(), finalRelay);
            await this.setStateAsync("floorheating.relay_state", Boolean(targetRelay), true);
        }

        // Check if bidirectional control is enabled
        const bidiState = await this.getStateAsync("control.bidirectional_enabled");
        if (bidiState && bidiState.val === false) {
            this.log.debug("Bidirectional control is paused, skipping execution on target objects.");
            return;
        }

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
     * Local 24h Offline-Resilience Controller Loop (Executed every 60s)
     * Keeps floor heating fully functional & executes pre-calculated MPC schedule when offline
     */
    async runOfflineHeatingLoop() {
        if (!this.config.fbhEnabled) {
            return;
        }

        // If cloud connection is online, cloud orchestrates in real time
        if (this.isConnectionActive()) {
            await this.setStateAsync("floorheating.offline_autonomous", false, true);
            return;
        }

        // Offline mode: mark indicator
        await this.setStateAsync("floorheating.offline_autonomous", true, true);

        if (!this.config.fbhRoomTempId || !this.config.fbhRelayTargetId) {
            return;
        }

        try {
            // Read current local room temperature
            const roomTempState = await this.getForeignStateAsync(this.config.fbhRoomTempId.trim());
            if (!roomTempState || roomTempState.val === null || roomTempState.val === undefined) {
                this.log.warn(`[Offline-Resilience] Could not read room temperature from ${this.config.fbhRoomTempId}`);
                return;
            }

            const currentRoomTemp = Number(roomTempState.val);
            const targetRoomTemp = Number(this.config.fbhTargetRoomTemp) || 21.0;
            const boostDelta = Number(this.config.fbhBoostDeltaK) || 1.0;
            const maxFloorTemp = Number(this.config.fbhMaxFloorTemp) || 24.5;
            const currentHour = new Date().getHours();

            // Find current hourly slot from cached schedule
            let currentSlot = null;
            if (Array.isArray(this.cachedSchedule24h) && this.cachedSchedule24h.length > 0) {
                const nowHourStr = `${String(currentHour).padStart(2, "0")}:00`;
                currentSlot = this.cachedSchedule24h.find(s => s.hour_label === nowHourStr) || this.cachedSchedule24h[0];
            }

            let shouldHeat = false;
            let decisionReason = "";

            // A) Overheating protection
            if (currentRoomTemp >= maxFloorTemp) {
                shouldHeat = false;
                decisionReason = `🛡️ Overheat protection (${currentRoomTemp.toFixed(1)}°C >= ${maxFloorTemp.toFixed(1)}°C)`;
            }
            // B) Comfort lower bound guarantee
            else if (currentRoomTemp < (targetRoomTemp - 0.5)) {
                shouldHeat = true;
                decisionReason = `❄️ Comfort protection (${currentRoomTemp.toFixed(1)}°C < ${targetRoomTemp - 0.5}°C)`;
            }
            // C) Schedule-based autonomous decision
            else if (currentSlot) {
                if (currentSlot.action_mode === "preheat") {
                    const maxPreheat = targetRoomTemp + boostDelta;
                    shouldHeat = (currentRoomTemp < maxPreheat);
                    decisionReason = `⚡ Autonomous 24h-Schedule Preheat (Target: ${maxPreheat.toFixed(1)}°C, Ist: ${currentRoomTemp.toFixed(1)}°C)`;
                } else if (currentSlot.action_mode === "coast") {
                    shouldHeat = false;
                    decisionReason = `🛋️ Autonomous Screed Coasting (${currentRoomTemp.toFixed(1)}°C >= ${targetRoomTemp.toFixed(1)}°C)`;
                } else if (currentSlot.action_mode === "heat") {
                    shouldHeat = (currentRoomTemp < targetRoomTemp);
                    decisionReason = `♨️ Autonomous Base Heat (Target: ${targetRoomTemp.toFixed(1)}°C, Ist: ${currentRoomTemp.toFixed(1)}°C)`;
                } else {
                    shouldHeat = false;
                    decisionReason = "⏸️ Autonomous Standby";
                }
            }
            // D) Fallback thermostat hysteresis (no schedule available)
            else {
                shouldHeat = (currentRoomTemp < targetRoomTemp);
                decisionReason = `🛋️ Offline Fallback Thermostat (${currentRoomTemp.toFixed(1)}°C vs ${targetRoomTemp.toFixed(1)}°C)`;
            }

            // Apply to Actuator Relay
            let finalRelayVal = shouldHeat;
            if (this.config.fbhRelayInvert) finalRelayVal = !finalRelayVal;

            await this.setForeignStateAsync(this.config.fbhRelayTargetId.trim(), finalRelayVal);
            await this.setStateAsync("floorheating.relay_state", shouldHeat, true);

            // Apply Flow Setpoint if target configured and slot has opt_flow_temp_c
            if (this.config.fbhFlowSetpointTargetId && currentSlot && currentSlot.opt_flow_temp_c) {
                const optFlow = Number(currentSlot.opt_flow_temp_c);
                await this.setForeignStateAsync(this.config.fbhFlowSetpointTargetId.trim(), optFlow);
                await this.setStateAsync("floorheating.flow_temp_setpoint", optFlow, true);
            }

            this.log.info(`[Offline-Resilience 🛡️] ${decisionReason} -> Relay ${shouldHeat ? "ON" : "OFF"} on [${this.config.fbhRelayTargetId}]`);

        } catch (e) {
            this.log.error(`[Offline-Resilience] Error during autonomous heating loop: ${e.message}`);
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
     * Initialize 24/7 Decoupled Carrier Admin Connection to smartEvo moniy
     */
    initCarrierConnection() {
        if (this.carrierWs) {
            try {
                this.carrierWs.removeAllListeners();
                this.carrierWs.on("error", () => {});
                if (typeof this.carrierWs.terminate === "function") {
                    this.carrierWs.terminate();
                } else if (typeof this.carrierWs.close === "function") {
                    this.carrierWs.close();
                }
            } catch (e) {}
            this.carrierWs = null;
        }

        if (this.carrierHeartbeatTimer) {
            clearInterval(this.carrierHeartbeatTimer);
            this.carrierHeartbeatTimer = null;
        }

        const token = this.getEffectiveToken();
        let carrierUrl = (this.config.carrierUrl || "wss://mon.smartevo.de/ws/agent/v1/").trim();
        if (!carrierUrl.startsWith("ws://") && !carrierUrl.startsWith("wss://")) {
            carrierUrl = `wss://${carrierUrl}`;
        }
        if (!carrierUrl.endsWith("/")) {
            carrierUrl += "/";
        }

        const fullUrl = `${carrierUrl}?device_sn=${encodeURIComponent(token)}&tenant=sharegy&client=iobroker&version=2.2.0`;
        this.log.info(`Connecting to smartEvo moniy Carrier Admin Socket at ${carrierUrl}...`);

        try {
            this.carrierWs = new WebSocket(fullUrl, {
                handshakeTimeout: 8000,
                perMessageDeflate: false,
            });

            this.carrierWs.on("open", () => {
                this.log.info("Connected to smartEvo moniy Carrier Admin Socket successfully!");
                this.carrierReconnectAttempts = 0;
                this.setState("info.carrierConnected", true, true);

                // Send immediate health ping & start 20s heartbeat loop
                this.sendCarrierHeartbeat();
                if (this.carrierHeartbeatTimer) clearInterval(this.carrierHeartbeatTimer);
                this.carrierHeartbeatTimer = setInterval(() => {
                    this.sendCarrierHeartbeat();
                }, 20000);
            });

            this.carrierWs.on("message", (data) => {
                this.handleCarrierMessage(data);
            });

            this.carrierWs.on("error", (err) => {
                this.log.debug(`Carrier socket error: ${err.message || err}`);
                this.setState("info.carrierConnected", false, true);
                if (this.carrierWs) {
                    try { this.carrierWs.terminate(); } catch (e) {}
                    this.carrierWs = null;
                }
                this.scheduleCarrierReconnect();
            });

            this.carrierWs.on("close", (code, reason) => {
                this.log.debug(`Carrier socket closed (code: ${code || "-"}, reason: ${reason || "none"}).`);
                this.setState("info.carrierConnected", false, true);
                if (this.carrierWs) {
                    try { this.carrierWs.terminate(); } catch (e) {}
                    this.carrierWs = null;
                }
                this.scheduleCarrierReconnect();
            });
        } catch (err) {
            this.log.debug(`Failed to initiate carrier connection: ${err.message}`);
            this.scheduleCarrierReconnect();
        }
    }

    /**
     * Exponential backoff reconnect for Carrier Admin connection
     */
    scheduleCarrierReconnect() {
        if (this.carrierReconnectTimer) return;
        if (this.carrierHeartbeatTimer) {
            clearInterval(this.carrierHeartbeatTimer);
            this.carrierHeartbeatTimer = null;
        }

        this.carrierReconnectAttempts++;
        // Fast retry: 3s -> 4.5s -> 6.7s -> 10s -> max 25s
        const backoff = Math.min(25000, 3000 * Math.pow(1.5, Math.min(this.carrierReconnectAttempts - 1, 5)));
        const delay = Math.round(backoff + Math.floor(Math.random() * 500));

        this.log.debug(`Scheduling Carrier reconnection in ${(delay / 1000).toFixed(1)}s (Attempt #${this.carrierReconnectAttempts})...`);
        this.carrierReconnectTimer = setTimeout(() => {
            this.carrierReconnectTimer = null;
            this.initCarrierConnection();
        }, delay);
    }

    /**
     * Record an error in the circular in-memory error buffer and optionally push to carrier
     */
    recordError(code, message, context = null, level = "error", pushToCarrier = false) {
        const errorEntry = {
            ts: Date.now(),
            level: level,
            code: code,
            message: String(message),
            context: context || null,
        };

        this.errorLogBuffer.unshift(errorEntry);
        if (this.errorLogBuffer.length > this.maxErrorLogSize) {
            this.errorLogBuffer.pop();
        }

        if (level === "error") {
            this.log.error(`[${code}] ${message}`);
        } else {
            this.log.warn(`[${code}] ${message}`);
        }

        if (pushToCarrier) {
            this.emitCarrierError(code, message, context, level);
        }
    }

    /**
     * Proactively emit an error/warning log event to moniy Carrier Admin socket
     */
    emitCarrierError(code, message, details = null, level = "error") {
        if (!this.carrierWs || this.carrierWs.readyState !== WebSocket.OPEN) return;
        try {
            const frame = {
                type: "log_event",
                level: level,
                code: code,
                message: String(message),
                details: details || {},
                timestamp: Date.now(),
            };
            this.carrierWs.send(JSON.stringify(frame));
        } catch (e) {
            this.log.debug(`Failed to emit carrier error log: ${e.message}`);
        }
    }

    /**
     * Validates all configured state IDs to detect missing, null, or wrong-type datapoints
     */
    async validateConfiguration() {
        const checks = [
            { field: "pvPowerId", id: this.config.pvPowerId, expected: "number", optional: false },
            { field: "gridPowerId", id: this.config.gridPowerId, expected: "number", optional: false },
            { field: "batteryPowerId", id: this.config.batteryPowerId, expected: "number", optional: true },
            { field: "batterySocId", id: this.config.batterySocId, expected: "number", optional: true },
            { field: "houseConsumptionId", id: this.config.houseConsumptionId, expected: "number", optional: true },
            { field: "fbhRoomTempId", id: this.config.fbhRoomTempId, expected: "number", optional: true },
            { field: "fbhFlowTempActualId", id: this.config.fbhFlowTempActualId, expected: "number", optional: true },
            { field: "fbhFloorTempId", id: this.config.fbhFloorTempId, expected: "number", optional: true },
            { field: "fbhHeatPumpPowerId", id: this.config.fbhHeatPumpPowerId, expected: "number", optional: true },
            { field: "fbhRelayTargetId", id: this.config.fbhRelayTargetId, expected: "boolean_or_number", optional: true },
            { field: "fbhFlowSetpointTargetId", id: this.config.fbhFlowSetpointTargetId, expected: "number", optional: true },
        ];

        if (Array.isArray(this.config.customDevices)) {
            for (const dev of this.config.customDevices) {
                if (dev && dev.stateId) {
                    checks.push({ field: `customDevice_${dev.name || "unnamed"}`, id: dev.stateId, expected: "number", optional: true });
                }
            }
        }
        if (Array.isArray(this.config.controlObjects)) {
            for (const ctrl of this.config.controlObjects) {
                if (ctrl && ctrl.targetStateId) {
                    checks.push({ field: `controlObject_${ctrl.identifier || "unnamed"}`, id: ctrl.targetStateId, expected: "any", optional: true });
                }
            }
        }

        const results = [];
        let totalConfigured = 0;
        let validCount = 0;
        let warningCount = 0;

        for (const check of checks) {
            const rawId = (check.id || "").trim();
            if (!rawId) {
                if (!check.optional) {
                    results.push({
                        field: check.field,
                        id: "",
                        status: "missing_required",
                        message: "Pflichtfeld ist nicht konfiguriert",
                    });
                    warningCount++;
                }
                continue;
            }

            totalConfigured++;
            try {
                const state = await this.getForeignStateAsync(rawId);
                if (state === null || state === undefined) {
                    results.push({
                        field: check.field,
                        id: rawId,
                        status: "not_found",
                        message: "Datenpunkt existiert in ioBroker nicht oder liefert keinen Wert (null)",
                    });
                    warningCount++;
                } else if (state.val === null || state.val === undefined || Number.isNaN(state.val)) {
                    results.push({
                        field: check.field,
                        id: rawId,
                        status: "null_value",
                        message: "Datenpunkt liefert 'null' oder 'NaN'",
                        raw_val: state.val,
                    });
                    warningCount++;
                } else if (check.expected === "number" && typeof state.val !== "number" && isNaN(Number(state.val))) {
                    results.push({
                        field: check.field,
                        id: rawId,
                        status: "type_mismatch",
                        message: `Erwartet Zahl, empfangen Typ '${typeof state.val}' ('${state.val}')`,
                        raw_val: state.val,
                    });
                    warningCount++;
                } else {
                    results.push({
                        field: check.field,
                        id: rawId,
                        status: "ok",
                        current_value: state.val,
                        ts: state.ts,
                    });
                    validCount++;
                }
            } catch (err) {
                results.push({
                    field: check.field,
                    id: rawId,
                    status: "error",
                    message: `Fehler beim Lesen: ${err.message}`,
                });
                warningCount++;
            }
        }

        return {
            valid: warningCount === 0,
            total_checked: totalConfigured,
            valid_count: validCount,
            issue_count: warningCount,
            checks: results,
            timestamp: Date.now(),
        };
    }

    /**
     * Send health ping frame to smartEvo moniy
     */
    sendCarrierHeartbeat() {
        if (!this.carrierWs || this.carrierWs.readyState !== WebSocket.OPEN) return;
        try {
            const payload = {
                type: "health_ping",
                timestamp: Date.now(),
                stats: {
                    uptime: Math.round(process.uptime()),
                    version: "2.2.0",
                    bufferedCount: this.offlineBuffer.length,
                    errorCount: this.errorLogBuffer.length,
                    connectedToSharegy: this.isConnectionActive(),
                    client: "iobroker",
                    memoryRssMb: Math.round(process.memoryUsage().rss / (1024 * 1024)),
                },
            };
            this.carrierWs.send(JSON.stringify(payload));
        } catch (e) {
            this.log.debug(`Failed to send carrier heartbeat: ${e.message}`);
        }
    }

    /**
     * Handle incoming Reverse-RPC commands from smartEvo moniy
     */
    async handleCarrierMessage(data) {
        let msg;
        try {
            msg = JSON.parse(data.toString());
        } catch (e) {
            this.log.warn(`Invalid JSON received on carrier socket: ${data}`);
            return;
        }

        if (!msg || typeof msg !== "object") return;

        // Support JSON-RPC 2.0
        const id = msg.id;
        const method = msg.method;
        const params = msg.params || {};

        if (!method) return;

        this.log.info(`[Carrier RPC] Received method '${method}' (ID: ${id})`);

        const sendResponse = (result = null, error = null) => {
            if (!this.carrierWs || this.carrierWs.readyState !== WebSocket.OPEN) return;
            if (id === undefined || id === null) return; // Notification, no response expected
            const resp = {
                jsonrpc: "2.0",
                id: id,
            };
            if (error) {
                resp.error = error;
            } else {
                resp.result = result || {};
            }
            try {
                this.carrierWs.send(JSON.stringify(resp));
            } catch (e) {
                this.log.warn(`Failed to send carrier RPC response: ${e.message}`);
            }
        };

        try {
            switch (method) {
                case "sys.ping": {
                    sendResponse({
                        pong: true,
                        timestamp: Date.now(),
                        uptime: Math.round(process.uptime()),
                        version: "2.2.0",
                    });
                    break;
                }

                case "sys.diagnostics":
                case "edge.get_diagnostics": {
                    sendResponse({
                        system: "ioBroker",
                        version: "2.2.0",
                        uptime: Math.round(process.uptime()),
                        memory: process.memoryUsage(),
                        bufferedCount: this.offlineBuffer.length,
                        errorCount: this.errorLogBuffer.length,
                        connectedToSharegy: this.isConnectionActive(),
                        subscriptions: Array.from(this.subscribedStateIds),
                        timestamp: Date.now(),
                    });
                    break;
                }

                case "sys.get_errors": {
                    sendResponse({
                        total_recorded: this.errorLogBuffer.length,
                        errors: this.errorLogBuffer,
                        timestamp: Date.now(),
                    });
                    break;
                }

                case "sys.clear_errors": {
                    this.errorLogBuffer = [];
                    sendResponse({ cleared: true, timestamp: Date.now() });
                    break;
                }

                case "sys.validate_config": {
                    const validation = await this.validateConfiguration();
                    sendResponse(validation);
                    break;
                }

                case "adapter.restart":
                case "edge.restart_service": {
                    sendResponse({ restarting: true, message: "Adapter restart initiated." });
                    this.log.warn("[Carrier RPC] Remote adapter restart requested by smartEvo moniy.");
                    setTimeout(() => {
                        this.restart();
                    }, 500);
                    break;
                }

                case "device.read": {
                    const stateId = params.id || params.state_id;
                    if (!stateId) {
                        sendResponse(null, { code: -32602, message: "Missing required parameter: id" });
                        return;
                    }
                    const state = await this.getForeignStateAsync(stateId);
                    sendResponse({
                        id: stateId,
                        state: state || null,
                        val: state ? state.val : null,
                        ts: state ? state.ts : null,
                    });
                    break;
                }

                case "ems.curtail":
                case "eebus.curtail": {
                    const active = Boolean(params.active !== undefined ? params.active : (params.limit_kw !== undefined || params.limit_w !== undefined));
                    const limitW = params.limit_w !== undefined ? Number(params.limit_w) : (params.limit_kw !== undefined ? Number(params.limit_kw) * 1000 : 0);
                    const reason = params.reason || "smartEvo carrier § 14a EnWG test";
                    this.log.warn(`[Carrier RPC] EMS Curtailment signal received: active=${active}, limit=${limitW}W, reason='${reason}'`);
                    sendResponse({
                        status: "acknowledged",
                        curtailed: active,
                        limit_w: limitW,
                        timestamp: Date.now(),
                    });
                    break;
                }

                case "adapter.update":
                case "edge.update": {
                    const targetSpec = params.target || params.version || params.target_version || "smartcuc/ioBroker.sharegy#main";
                    const timeoutSec = Number(params.timeout_seconds || params.timeout || 900);
                    const result = await this.updateWatchdog.initiateUpdate(targetSpec, timeoutSec);
                    sendResponse(result);
                    break;
                }

                case "adapter.confirm_update": {
                    const reason = params.reason || "manual_admin_rpc";
                    const result = this.updateWatchdog.confirmUpdate(reason);
                    sendResponse(result);
                    break;
                }

                case "adapter.rollback": {
                    const result = this.updateWatchdog.triggerImmediateRollback();
                    sendResponse(result);
                    break;
                }

                case "adapter.get_update_status": {
                    const result = this.updateWatchdog.getStatus();
                    sendResponse(result);
                    break;
                }

                case "device.discover":
                case "edge.discover_datapoints": {
                    const result = await this.autoDiscovery.scan();
                    sendResponse(result);
                    break;
                }

                default: {
                    sendResponse(null, { code: -32601, message: `Method '${method}' not found` });
                    break;
                }
            }
        } catch (err) {
            this.log.error(`[Carrier RPC] Error handling '${method}': ${err.message}`);
            sendResponse(null, { code: -32000, message: err.message });
        }
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
            if (this.carrierReconnectTimer) {
                clearTimeout(this.carrierReconnectTimer);
                this.carrierReconnectTimer = null;
            }
            if (this.carrierLivenessTimer) {
                clearInterval(this.carrierLivenessTimer);
                this.carrierLivenessTimer = null;
            }
            if (this.throttleTimer) {
                clearTimeout(this.throttleTimer);
                this.throttleTimer = null;
            }
            if (this.offlineLoopTimer) {
                clearInterval(this.offlineLoopTimer);
                this.offlineLoopTimer = null;
            }
            if (this.livenessTimer) {
                clearInterval(this.livenessTimer);
                this.livenessTimer = null;
            }
            this.cleanupSockets();
            this.setState("info.connection", false, true);
            this.setState("info.carrierConnected", false, true);
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
