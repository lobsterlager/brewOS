# brewOS G30

Leichtgewichtige Steuerungs- und Dashboard-Software fuer den Grainfather G30.

## Inhalt

- `dashboard_server.py`: lokaler HTTP-Server fuer das Brau-Dashboard
- `gf_brew_flow.py`: Ablauf- und Steuerlogik fuer den Sudprozess
- `brew_controller.py`: Controller-Helfer fuer Brauschritte
- `dashboard/`: statische Dashboard-Oberflaeche
- `recipes/`: Beispiel- und Rezeptdateien
- `tests/`: Basis-Tests

## Hinweis zum aktuellen Stand

Dies ist ein aus dem produktiven Arbeitsverzeichnis herausgeloester Release-Stand.
Einige lokale Umgebungsannahmen sind noch enthalten, insbesondere:

- harte Python-Pfade wie `/Users/KriKri/gf-venv/bin/python`
- optionale Verweise auf lokale Hilfsdateien fuer Benachrichtigungen
- lokale BLE-Parameter ueber Umgebungsvariablen

Vor produktivem Einsatz sollten diese Stellen auf relative Pfade, `venv`-Aktivierung
oder konfigurierbare Environment-Variablen umgestellt werden.

## Empfohlene Vorbereitung vor Nutzung

1. Python-Virtualenv lokal anlegen
2. benoetigte Abhaengigkeiten installieren
3. lokale Pfade in `dashboard_server.py`, `dashboard/start_dashboard.sh` und
   `gf_brew_flow.py` anpassen
4. Umgebungsvariablen fuer BLE und optionale Benachrichtigungen setzen

## Tests

Beispiel:

```bash
pytest tests
```
