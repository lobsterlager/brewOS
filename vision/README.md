# Vision

Computer vision layer for physical verification of brewing state.

## Purpose

BLE telemetry tells us what the hardware *reports*. Vision tells us what's *actually happening*.

## Capabilities

- **Display reading**: OCR on the G30 LCD to cross-check BLE data
- **Foam detection**: Monitor foam levels during mash/boil to prevent boilover
- **Kettle state**: Verify grain-in, sparge, boil visually
- **Anomaly alerts**: Flag unexpected states for human review

## Setup

USB camera mounted above the brewing vessel, angled to capture both the LCD display and the kettle opening.
