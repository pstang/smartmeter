
# SmartMeter tools

This respository holds various tools for retrieving and processing information from "SmartMeters" (domestic power/utility company electrical meters).
These tools have been developed and tested with PG&E California SmartMeters.

## Contents

- smartmeter_mqtt_esp32       - Arduino ESP32 application for processing Emporia Vue Smart Home Energy Monitor to MQTT
- smartmeter_service_influxdb - Python service that connects MQTT publications to an influxdb backend
