# ioBroker.sharegy Adapter

![Logo](admin/sharegy.png)

Official ioBroker adapter for the **Sharegy Energy Management Platform** ([sharegy.de](https://sharegy.de)).

Connects your ioBroker Smart Home (PV systems, battery storages, heat pumps, BWWP, wallboxes, smart meters, Shelly, Sonoff, KNX, Zigbee, Modbus) with Sharegy for:
- ☀️ **Realtime EMS Telemetry** (Live energy flow, PV generation, grid feed-in/import, battery SoC)
- 🌡️ **Custom Devices & Sensors** (Heatpumps, Brauchwasserwärmepumpen, temperature sensors, submeters)
- 🎛️ **Bidirectional Load Control & SG-Ready** (Control ioBroker relays & setpoints directly from Sharegy optimization rules)

---

## 🚀 Installation

### Option 1: Direct Install via ioBroker Admin (GitHub URL)
1. In your ioBroker Admin, switch to the **Adapters** tab.
2. Click on the **GitHub Octocat icon** (Install from custom URL) in the top toolbar.
3. Select **Custom URL** and enter:
   ```
   https://github.com/smartcuc/eswes/tree/main/integrations/iobroker.sharegy
   ```
   *(or copy the folder `integrations/iobroker.sharegy` into `/opt/iobroker/node_modules/iobroker.sharegy` and run `iobroker add sharegy`)*
4. Create an instance of the adapter (`sharegy.0`).

### Option 2: CLI Installation on your ioBroker Server
```bash
cd /opt/iobroker
npm install https://github.com/smartcuc/eswes/tarball/main#subdirectory=integrations/iobroker.sharegy
iobroker add sharegy
iobroker upload sharegy
```

---

## ⚙️ Configuration
The adapter settings provide 4 clear tabs:

### 1. 🔌 Verbindung & Zugangsdaten (Connection)
- **Verbindungsprotokoll**: `WSS` (WebSocket Secure via Port 443 - Standard) oder `MQTTS` (Port 8883)
  > [!NOTE]
  > Unverschlüsseltes MQTT (Port 1883) wird aus Sicherheitsgründen **nicht** unterstützt. Die Verbindung erfolgt standardmäßig vollverschlüsselt über den HTTPS-Port 443 (`wss://sharegy.de/mqtt`).
- **Server Host**: `sharegy.de` (Port: `443`, Pfad: `/mqtt`)
- **Home Token**: Enter your personal Home Token from Sharegy (*Settings -> MQTT / Integrations*).
- **Mindestsendeintervall**: Throttling interval in seconds (default: 5s) to avoid unnecessary network load.
- **Offline-Pufferung (Ringpuffer)**: Zwischenspeichern von Messdaten bei Internet-/Routerausfall (Standard: 5.000 Punkte). Nach Wiederverbindung werden alle Datenpunkte mit historisch exaktem Zeitstempel nachgeliefert (Status einsehbar unter `sharegy.0.info.bufferedCount`).

### 2. ☀️ EMS & Kern-Zähler (EMS Telemetry)
Select the central ioBroker states for your energy balance:
- **PV-Erzeugung Leistung**: e.g., `sungrow.0.total_pv_power` or `shelly.0.balkonkraftwerk.power` (W)
- **Netzleistung**: e.g., `smartmeter.0.1-0:16_7_0__255.value` (W)
- **Netz-Vorzeichen**: Choose whether positive means import or feed-in.
- **Netzbezug / Einspeisung Zählerstände**: e.g., `smartmeter.0.1-0:1_8_0__255.value` (kWh)
- **Batteriespeicher Leistung & SoC**: e.g., `sungrow.0.battery_power` (W) & `sungrow.0.battery_level` (%)
- **Hausverbrauch**: (optional, calculated automatically if omitted)

### 3. 🌡️ Sonstige Geräte & Sensoren (Custom Devices)
Add any individual sensors and sub-meters:
| ioBroker Object ID | Sharegy Identifier | Rolle | Messgröße | Einheit | Skalierung |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `sonoff.0.bwwp.temperature` | `bwwp_temp` | 🌡️ Sensor | Temperatur | `°C` | `1` |
| `shelly.0.bwwp.Relay0.Power` | `bwwp_power` | 🔌 Verbraucher | Leistung | `W` | `1` |
| `zigbee.0.living_room.temp` | `living_temp` | 🌡️ Sensor | Temperatur | `°C` | `1` |
| `modbus.0.heatpump.power` | `heatpump` | 🔌 Verbraucher | Leistung | `W` | `1` |

### 4. 🎛️ Rückkanal & Lastmanagement (Bidirectional Control)
Map Sharegy control commands to ioBroker states:
| Sharegy Steuer-Kanal | Ziel-Objekt im ioBroker | Steuer-Typ | Invertieren |
| :--- | :--- | :--- | :--- |
| `bwwp_sg_ready` | `shelly.0.shellyplus1#bwwp.Relay0.Switch` | Ein / Aus Schalter | Nein |
| `wallbox_charge_enable` | `go-e.0.allow_charging` | Ein / Aus Schalter | Nein |
| `wallbox_current_limit` | `go-e.0.ampere` | Ladestrom (Ampere) | Nein |
| `storage_target_soc` | `sungrow.0.target_soc` | Ziel-SoC (%) | Nein |

---

## 📡 MQTT Topic Architecture

### Telemetry (ioBroker ➔ Sharegy)
- `h/<token>/<identifier>`
- Payload: `{"value": 48.5, "unit": "°C", "metric": "temperature", "role": "sensor", "ts": 1756932000}`

### Control (Sharegy ➔ ioBroker)
- `h/<token>/<identifier>/set`
- Payload: `{"val": true}` or `{"val": 16}`

---

## 📄 License
MIT License - (C) 2026 Sharegy Team
