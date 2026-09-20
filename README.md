# ioBroker.sharegy Adapter (v2.2.0)

![Logo](admin/sharegy.png)

Official ioBroker adapter for the **Sharegy Energy Management Platform** ([sharegy.de](https://sharegy.de)).

Connects your ioBroker Smart Home (PV systems, battery storages, heat pumps, floor heating, BWWP, wallboxes, smart meters, Shelly, Homematic, KNX, Zigbee, Modbus) with Sharegy for:
- ☀️ **Realtime EMS Telemetry** (Live energy flow, PV generation, grid feed-in/import, battery SoC)
- 🔥 **Wärme, Heizung & Raumklima (DIN EN 12831 / MPC)**: Vorausschauende Fußbodenheizungs- & thermische Estrichspeicher-Steuerung
- 🛡️ **Lokale 24h-Offline-Resilienz**: Cacht den 24h-MPC-Fahrplan lokal und regelt die Heizung bei Internetausfall vollkommen autonom weiter
- 🛰️ **24/7 Entkoppelter Carrier Admin & Reverse-RPC**: Sichere Fernwartung, § 14a EnWG Notfalldrosselung und Echtzeit-Konfigurationsvalidierung ohne Port-Weiterleitung
- 🔄 **Canary A/B OTA Updates & 15-Minuten Rollback-Watchdog**: Selbstheilendes Flottenmanagement – schlägt ein Remote-Update fehl, stellt ein unabhängiger Hintergrundwächter nach 15 Minuten automatisch die funktionierende Vorversion wieder her
- 🌡️ **Custom Devices & Sensoren**: Wärmepumpen, Brauchwasserwärmepumpen, Raumtemperatursensoren, Submeter
- 🎛️ **Bidirektionales Lastmanagement & SG-Ready**: Direkte Ansteuerung von ioBroker-Aktoren und Vorgabe von Sollwerten aus der Sharegy KI-Merit-Order

---

## 🚀 Installation

### Option 1: Direkte Installation via ioBroker Admin (GitHub URL)
1. Im ioBroker Admin auf den Reiter **Adapter** wechseln.
2. Oben auf das **Katzen-Icon** (Benutzerdefiniert / GitHub) klicken.
3. **Aus GitHub** wählen und die URL eingeben:
   ```
   https://github.com/smartcuc/ioBroker.sharegy
   ```
4. Instanz des Adapters erzeugen (`sharegy.0`).

### Option 2: CLI Installation auf dem ioBroker-Server
```bash
iobroker url https://github.com/smartcuc/ioBroker.sharegy
```

---

## ⚙️ Konfiguration

Die Adapter-Einstellungen bieten 5 übersichtliche Tabs:

### 1. 🔌 Verbindung & Zugangsdaten (Connection)
- **Verbindungsprotokoll**: `WSS` (WebSocket Secure via Port 443 - Standard & Firewall-sicher)
  - **WebSocket URL**: Kopiere deine persönliche WebSocket-URL mit 1 Klick aus deinen Sharegy-Schnittstellen (`wss://sharegy.de/ws/energy/<TOKEN>/`).
- **Mindestsendeintervall**: Drosselung (Standard: 5s), um ioBroker und Netzwerk zu schonen.
- **Offline-Pufferung (Ringpuffer)**: Zwischenspeichern von Messdaten bei Internet-/Routerausfall. Nach Wiederverbindung werden alle Datenpunkte historisch exakt nachgeliefert (`sharegy.0.info.bufferedCount`).

### 2. ☀️ EMS & Kern-Zähler (EMS Telemetry)
Wähle die zentralen Datenpunkte deiner Energiebilanz:
- **PV-Erzeugung Leistung**: z. B. `sungrow.0.total_pv_power` oder `shelly.0.balkonkraftwerk.power` (W)
- **Netzleistung**: z. B. `smartmeter.0.1-0:16_7_0__255.value` (W)
- **Netz-Vorzeichen**: Wähle, ob positive Werte Bezug oder Einspeisung bedeuten.
- **Netzbezug / Einspeisung Zählerstände**: z. B. `smartmeter.0.1-0:1_8_0__255.value` (kWh)
- **Batteriespeicher Leistung & SoC**: z. B. `sungrow.0.battery_power` (W) & `sungrow.0.battery_level` (%)
- **Hausverbrauch**: (optional, wird sonst automatisch aus Erzeugung, Netz und Speicher berechnet)

### 3. 🔥 Fußbodenheizung & Estrich-Speicher (Wärme & Raumklima)
Vorausschauende Heizungs- & thermische Bauteilaktivierung (DIN EN 12831 / MPC):
- **Aktivierung & Betriebsmodus**: *Autopilot (KI-Wetter & 24h-MPC)*, *PV-Only*, *Price-Saver*, *Komfort* oder *Manuell*.
- **1. Messpunkte & Sensor-Eingänge**:
  - `Raum-Ist-Temperatur (°C) *`: Zentraler Raumfühler (z. B. `zigbee.0.living_room.temp` oder Homematic Thermostat)
  - `Vorlauf-Ist-Temperatur (°C, optional)`: Vorlauffühler des Heizkreises
  - `Estrich-/Bodentemperatur (°C, optional)`: Bodenfühler für Überhitzungsschutz
  - `Außentemperatur (°C, optional)`: Lokaler Außensensor (oder DWD/Open-Meteo Cloud-Fallback)
  - `Heizkreis-/WP-Leistung (W, optional)`: Wirkleistungsmessung
- **2. Aktorik & Steuer-Ausgänge**:
  - `Heizkreis-Relais / Ventil Zielobjekt *`: Schaltaktor (z. B. `shelly.0.shellyplus1#fbh.Relay0.Switch`)
  - `Vorlauf-Solltemperatur Zielobjekt (°C, optional)`: Vorgabe für modulierende Wärmepumpen/Mischer
  - `SG-Ready / Boost Schaltausgang`: Digitaler Kontakt für WP-Vorladung
- **3. Sollwerte & Speichergrenzen**:
  - Ziel-Raumtemperatur (z. B. 21.0 °C), Max. Vorladehub (z. B. +1.0 K), Max. Estrich-Sicherheitstemperatur (z. B. 24.5 °C).
- **4. Integrierte 24h-Offline-Resilienz**:
  - Der 24h-Fahrplan wird automatisch im ioBroker gesichert (`sharegy.0.floorheating.cached_schedule`). Bei Internetausfall schaltet der Adapter auf `sharegy.0.floorheating.offline_autonomous = true` und steuert die Heizung autonom weiter.

### 4. 🌡️ Sonstige Geräte & Sensoren (Custom Devices)
Füge beliebige individuelle Geräte hinzu (Brauchwasserwärmepumpen, Steckdosen, Einzelsensoren). Die physikalische Einheit wird automatisch zugewiesen:
| ioBroker Object ID | Sharegy Identifier | Rolle | Messgröße (Einheit auto) | Skalierung |
| :--- | :--- | :--- | :--- | :--- |
| `sonoff.0.bwwp.temperature` | `bwwp_temp` | 🌡️ Sensor | Temperatur (`°C`) | `1` |
| `shelly.0.bwwp.Relay0.Power` | `bwwp_power` | 🔌 Verbraucher | Leistung (`W`) | `1` |
| `modbus.0.heatpump.power` | `heatpump` | 🔌 Verbraucher | Leistung (`W`) | `1` |

### 5. 🎛️ Rückkanal & Lastmanagement (Bidirectional Control)
Verknüpfe beliebige Sharegy-Steuerkanäle mit ioBroker-Zielobjekten:
| Sharegy Steuer-Kanal | Ziel-Objekt im ioBroker | Steuer-Typ | Invertieren |
| :--- | :--- | :--- | :--- |
| `bwwp_sg_ready` | `shelly.0.shellyplus1#bwwp.Relay0.Switch` | Ein / Aus Schalter | Nein |
| `wallbox_charge_enable` | `go-e.0.allow_charging` | Ein / Aus Schalter | Nein |
| `wallbox_current_limit` | `go-e.0.ampere` | Ladestrom (Ampere) | Nein |
| `storage_target_soc` | `sungrow.0.target_soc` | Ziel-SoC (%) | Nein |

---

## 📄 Lizenz
MIT License - (C) 2026 Sharegy Team <info@sharegy.de>
