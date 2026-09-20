"use strict";

/**
 * lib/autoDiscovery.js
 * 
 * Smart Auto-Discovery Engine for ioBroker.sharegy
 * Automatically scans ioBroker objects & states for PV inverters, batteries, smart meters,
 * wallboxes, and heat pumps across popular ecosystem adapters.
 */

const KNOWN_ADAPTERS = [
    { prefix: "sungrow", name: "Sungrow Hybrid Inverter", vendor: "Sungrow" },
    { prefix: "fronius", name: "Fronius Solar Inverter / Symo / Gen24", vendor: "Fronius" },
    { prefix: "sma-em", name: "SMA Energy Meter / Sunny Home Manager", vendor: "SMA" },
    { prefix: "sma", name: "SMA Solar Technology", vendor: "SMA" },
    { prefix: "solaredge", name: "SolarEdge Monitoring", vendor: "SolarEdge" },
    { prefix: "huawei", name: "Huawei FusionSolar", vendor: "Huawei" },
    { prefix: "shelly", name: "Shelly Pro / 3EM / EM / Plus", vendor: "Allterco Shelly" },
    { prefix: "smartmeter", name: "Smartmeter D0 / SML / OBIS", vendor: "Generic eHZ / Smart Meter" },
    { prefix: "tibber", name: "Tibber Pulse / Realtime Bridge", vendor: "Tibber" },
    { prefix: "senec", name: "SENEC Home Battery", vendor: "SENEC" },
    { prefix: "kostal", name: "Kostal Plenticore / Piko", vendor: "Kostal" },
    { prefix: "growatt", name: "Growatt Inverter", vendor: "Growatt" },
    { prefix: "sonnen", name: "sonnenBatterie", vendor: "Sonnen" },
    { prefix: "deye", name: "Deye / SunSynk Inverter", vendor: "Deye" },
    { prefix: "modbus", name: "Modbus TCP / RTU Energy Meter", vendor: "Modbus" },
    { prefix: "kecontact", name: "KEBA KeContact Wallbox", vendor: "KEBA" },
    { prefix: "go-e", name: "go-e Charger Wallbox", vendor: "go-e" },
    { prefix: "easee", name: "Easee Home / Charge Wallbox", vendor: "Easee" },
    { prefix: "ocpp", name: "OCPP 1.6-J Wallbox Bridge", vendor: "OCPP" },
    { prefix: "luxtronik", name: "Luxtronik 2 Heat Pump", vendor: "Alpha Innotec / Novelan" },
    { prefix: "nibe", name: "NIBE Uplink Heat Pump", vendor: "NIBE" },
    { prefix: "viessmann", name: "Viessmann ViCare / Heat Pump", vendor: "Viessmann" },
    { prefix: "vaillant", name: "Vaillant multiMATIC / sensocomFORT", vendor: "Vaillant" },
    { prefix: "daikin", name: "Daikin Altherma Heat Pump", vendor: "Daikin" },
];

const SIGNATURE_PATTERNS = {
    pv_power: [
        /pv[_-]?power/i,
        /solar[_-]?power/i,
        /total[_-]?pv/i,
        /yield[_-]?power/i,
        /p[_-]?ac/i,
        /ac[_-]?power/i,
        /ppv/i,
        /generation[_-]?power/i,
        /16_7_0.*pv/i,
    ],
    grid_power: [
        /grid[_-]?power/i,
        /meter[_-]?power/i,
        /active[_-]?power/i,
        /power[_-]?grid/i,
        /power[_-]?total/i,
        /16_7_0/i, // OBIS Wirkleistung Total
        /1_7_0/i,  // OBIS Wirkleistung Bezug
        /2_7_0/i,  // OBIS Wirkleistung Lieferung
        /power[_-]?l1.*l2.*l3/i,
        /em_power/i,
    ],
    battery_soc: [
        /bat[a-z0-9_-]*soc/i,
        /state[_-]?of[_-]?charge/i,
        /battery[_-]?level/i,
        /soc$/i,
        /capacity[_-]?percent/i,
    ],
    battery_power: [
        /bat[a-z0-9_-]*power/i,
        /battery[_-]?power/i,
        /bat[_-]?charge[_-]?power/i,
        /bat[_-]?discharge[_-]?power/i,
    ],
    house_load: [
        /house[_-]?power/i,
        /house[_-]?load/i,
        /consumption[_-]?power/i,
        /home[_-]?power/i,
        /load[_-]?power/i,
    ],
    wallbox_power: [
        /wallbox.*power/i,
        /ev.*power/i,
        /charge.*power/i,
        /charging.*power/i,
    ],
    heat_pump_temp: [
        /flow[_-]?temp/i,
        /vorlauf/i,
        /supply[_-]?temp/i,
        /water[_-]?temp/i,
        /actual[_-]?temp/i,
    ],
};

class AutoDiscoveryScanner {
    constructor(adapter) {
        this.adapter = adapter;
    }

    /**
     * Run full scan across ioBroker objects
     */
    async scan() {
        const result = {
            success: true,
            timestamp: Date.now(),
            detected_adapters: [],
            suggested_mappings: {},
            candidate_states: [],
        };

        try {
            // 1. Get all objects with type "state"
            const statesObj = await this.adapter.getForeignObjectsAsync("*", "state");
            if (!statesObj || Object.keys(statesObj).length === 0) {
                result.message = "No state objects found in ioBroker hierarchy.";
                return result;
            }

            const detectedAdapterPrefixes = new Set();
            const candidates = [];

            // 2. Iterate and match signatures
            for (const [id, obj] of Object.entries(statesObj)) {
                const common = obj.common || {};
                const name = typeof common.name === "object" ? (common.name.de || common.name.en || id) : (common.name || id);
                const role = common.role || "";
                const unit = common.unit || "";

                // Check adapter prefix
                for (const ad of KNOWN_ADAPTERS) {
                    if (id.startsWith(`${ad.prefix}.`)) {
                        detectedAdapterPrefixes.add(ad.prefix);
                    }
                }

                // Match metric type
                let matchedCategory = null;
                for (const [category, patterns] of Object.entries(SIGNATURE_PATTERNS)) {
                    for (const pattern of patterns) {
                        if (pattern.test(id) || pattern.test(role) || pattern.test(name)) {
                            matchedCategory = category;
                            break;
                        }
                    }
                    if (matchedCategory) break;
                }

                if (matchedCategory) {
                    candidates.push({
                        id,
                        name,
                        role,
                        unit,
                        category: matchedCategory,
                        type: common.type || "number",
                    });
                }
            }

            // Populate detected adapter info
            result.detected_adapters = KNOWN_ADAPTERS.filter(ad => detectedAdapterPrefixes.has(ad.prefix));

            // Fetch live values for the top candidates
            for (const item of candidates.slice(0, 100)) {
                try {
                    const state = await this.adapter.getForeignStateAsync(item.id);
                    item.current_value = state ? state.val : null;
                    item.last_updated = state ? state.ts : null;
                } catch {
                    item.current_value = null;
                }
            }

            result.candidate_states = candidates;

            // 3. Build suggested 1-click mappings (pick best candidate per category)
            const categories = ["pv_power", "grid_power", "battery_soc", "battery_power", "house_load", "wallbox_power", "heat_pump_temp"];
            for (const cat of categories) {
                const matches = candidates.filter(c => c.category === cat);
                if (matches.length > 0) {
                    // Prefer ones with non-null numeric values
                    const activeMatch = matches.find(m => m.current_value !== null && typeof m.current_value === "number") || matches[0];
                    result.suggested_mappings[cat] = {
                        id: activeMatch.id,
                        name: activeMatch.name,
                        unit: activeMatch.unit,
                        current_value: activeMatch.current_value,
                        role: activeMatch.role,
                    };
                }
            }

            this.adapter.log.info(`[AutoDiscovery] Scan completed: found ${result.detected_adapters.length} adapters and ${candidates.length} candidates.`);
            return result;
        } catch (e) {
            this.adapter.log.error(`[AutoDiscovery] Scan failed: ${e.message}`);
            return {
                success: false,
                error: e.message,
                timestamp: Date.now(),
            };
        }
    }
}

module.exports = AutoDiscoveryScanner;
