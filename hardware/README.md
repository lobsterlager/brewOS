# Hardware

BLE bridge to the Grainfather G30 and supporting equipment.

## Equipment

| Device | Connection | Role |
|--------|-----------|------|
| Grainfather G30 | BLE | Mash/boil vessel, pump, heater |
| Glycol chiller | Relay | Fermentation temperature control |
| Conical fermenter | Temp sensor | Fermentation vessel |
| USB camera | USB | Vision system input |

## BLE Protocol

The Grainfather G30 exposes BLE characteristics for:
- Temperature reading (current + target)
- Heater on/off
- Pump on/off
- Timer status
- Step progression

Connection managed from macOS via CoreBluetooth.
