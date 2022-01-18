// SmartMeter->MQTT on ESP32
// Pascal Stang
//
// Converts the debug serial stream from the Emporia VUE HAN (Home Area Network) bridge
// to MQTT reports published via wifi.
// Expects the debug serial to be on Serial2 (GPIO16/GPIO17)
//
// Requires:
// - ESP32 development environment
// - PubSubClient

#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>

// Replace the next variables with your SSID/Password combination
const char* WifiHostname = "smartmeter";
const char* WifiSsid = "MySSID";
const char* WifiPassword = "MyPassword";

// MQTT Broker IP address:
const char* MqttServer = "192.168.1.28";

// LED Pin
const int LedPin = 2;

// Smartmeter data structure
typedef struct smartmeterData_s {
  uint32_t Timestamp;
  double Interval;
  double Current;
  double MeterSum;
  double LocalSum;
  double SolarSum;
  double UptimeHan;
} smartmeterData_t;

// Global objects
WiFiClient EspClient;
PubSubClient MqttClient(EspClient);
long TimeLastMsg = 0;
double Uptime = 0;
smartmeterData_t smartmeterData;

void setup() {
  Serial.begin(115200);
  Serial.println("");
  Serial.println("SmartMeter-MQTT server");

  Serial.println("Setting up WiFi");
  setupWifi();

  Serial.println("Setting up MQTT");
  MqttClient.setServer(MqttServer, 1883);
  MqttClient.setCallback(mqttCallback);

  Serial.println("Setting up SmartMeter");
  smartmeterSetup();

  pinMode(LedPin, OUTPUT);
}

void loop() {
  // Handle reconnection.
  if (!MqttClient.connected()) {
    mqttReconnect();
  }
  // Handle MQTT servicing.
  MqttClient.loop();

  // Handle smartmeter servicing.
  smartmeterService();

  // Every 10 seconds, report our internal uptime.
  long now = millis();
  if ((now - TimeLastMsg) > 10000) {
    TimeLastMsg += 10000;

    // Report internal uptime.
    char temp_str[40];
    Uptime = millis()/1000.0;
    dtostrf(Uptime, 1, 3, temp_str);
    Serial.print("Uptime: ");
    Serial.println(temp_str);
    MqttClient.publish("smbridge/uptime", temp_str);
  }

  // Sleep in microseconds.
  //esp_sleep_enable_timer_wakeup(1000);
  //Serial.println("Going to light-sleep now");
  //Serial.flush();
  //esp_light_sleep_start();
}

//-----------------------------------------------
void setupWifi() {
  delay(10);
  // We start by connecting to a WiFi network
  Serial.println();
  Serial.print("Wifi: Connecting to ");
  Serial.println(WifiSsid);

  WiFi.setHostname(WifiHostname)
  WiFi.begin(WifiSsid, WifiPassword);

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println("");
  Serial.println("WiFi: Connected");
  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());
}

void mqttCallback(char* topic, byte* message, unsigned int length) {
  Serial.print("MQTT Rx: Topic=");
  Serial.print(topic);
  Serial.print(" Msg=");

  String messageTemp;
  for(int i = 0; i < length; i++) {
    Serial.print((char)message[i]);
    messageTemp += (char)message[i];
  }
  Serial.println();

  // Example command handling, this is currently used only for demonstration.

  // If a message is received on the topic smbridge/cmd, then process the command.
  if(String(topic) == "smbridge/cmd") {
    Serial.print("Command: ");
    Serial.println(messageTemp);

    // Process command.
    if(messageTemp == "led on") {
      digitalWrite(LedPin, HIGH);
    } else if(messageTemp == "led off") {
      digitalWrite(LedPin, LOW);
    } else {
      Serial.println("Command: Not recognized");
    }
  }
}

void mqttConnectAction() {
  // Subscribe
  MqttClient.subscribe("smbridge/cmd");
}

void mqttReconnect() {
  // Loop until we're reconnected
  while (!MqttClient.connected()) {
    Serial.print("MQTT: Attempting connection...");
    // Attempt to connect
    if(MqttClient.connect("SmartMeter-ESP32")) {
      Serial.println("connected");
      mqttConnectAction();
    } else {
      Serial.print("failed, rc=");
      Serial.print(MqttClient.state());
      Serial.println(" try again in 5 seconds");
      // Wait 5 seconds before retrying
      delay(5000);
    }
  }
}

void smartmeterSetup() {
  //Serial2.begin(115200, SERIAL_8N1, RXD2, TXD2);
  Serial2.begin(115200);
}

void smartmeterService() {
  int rc = 0;
  char buffer[4096];

  smartmeterReportGet(buffer, sizeof(buffer));
  rc = smartmeterProcess(buffer);

  // Debug code
  //if(buffer) {
  //  Serial.print("SmartMeter: ");
  //  Serial.println(buffer);
  //  Serial.println("");
  //}

  digitalWrite(LedPin, HIGH);
  if(rc == 10) {
    char temp_str[400];
    Serial.print("SmartMeter:");

    // measurement data
    itoa(smartmeterData.Timestamp, temp_str, 10);
    Serial.print(" Time=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/timestamp", temp_str);

    dtostrf(smartmeterData.Interval, 1, 6, temp_str);
    Serial.print(" Intv=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/interval", temp_str);

    dtostrf(smartmeterData.Current, 1, 6, temp_str);
    Serial.print(" Amps=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/current", temp_str);

    dtostrf(smartmeterData.MeterSum, 1, 6, temp_str);
    Serial.print(" MS=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/metersum", temp_str);

    dtostrf(smartmeterData.LocalSum, 1, 6, temp_str);
    Serial.print(" LS=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/localsum", temp_str);

    dtostrf(smartmeterData.SolarSum, 1, 6, temp_str);
    Serial.print(" SS=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/solarsum", temp_str);

    // system monitoring
    dtostrf(smartmeterData.UptimeHan, 1, 6, temp_str);
    Serial.print(" UpHan=");
    Serial.print(temp_str);
    MqttClient.publish("smartmeter/uptimehan", temp_str);

    Serial.println("");

    // Publish report
    snprintf(temp_str, sizeof(temp_str),
      "Timestamp_s=%ld,Interval_s=%1.6f,Current_A=%1.6f,MeterSum=%1.6f,LocalSum=%1.6f,SolarSum=%1.6f,UptimeHan_s=%1.6f",
      smartmeterData.Timestamp,
      smartmeterData.Interval,
      smartmeterData.Current,
      smartmeterData.MeterSum,
      smartmeterData.LocalSum,
      smartmeterData.SolarSum,
      smartmeterData.UptimeHan);
    MqttClient.publish("smartmeter/report", temp_str);
  }
  digitalWrite(LedPin, LOW);
}

int smartmeterReportGet(char *buffer, int maxlen) {
  int len = 0;

  // Debug code
  //while (Serial2.available()) {
  //  str[len++] = char(Serial2.read());
  //}

  // Collect one line of data at a time.
  len = Serial2.readBytesUntil('\r', buffer, maxlen-1);
  buffer[len] = 0;
  // Remove unprintable chars (in place).
  char *src, *dst;
  src = buffer;
  dst = buffer;
  while(*src) {
    if((*src >= 0x20) && (*src <= 0x7F))
      *dst++ = *src;
    src++;
  }
  *dst = 0;

  return strlen(buffer);
}

int smartmeterProcess(const char *str) {
  int rc = 0;

  // Find strings
  char *p;
  if(p = strstrAfter(str, "Curr time:")) {
    smartmeterData.Timestamp = strtol(p, 0, 10);
    rc = 1;
  }
  if(p = strstrAfter(str, "uS since boot:")) {
    //Serial.print("UpTimeHan: ");
    //Serial.println(p);
    // Overflows 32-bit int
    //smartmeterData.UptimeHan = strtol(p, 0, 10) / 1e6;
    smartmeterData.UptimeHan = strtod(p, 0) / 1e6;
    rc = 2;
  }
  if(p = strstrAfter(str, "clock delta:")) {
    smartmeterData.Interval = strtol(p, 0, 10) / 1e6;
    rc = 3;
  }
  if(p = strstrAfter(str, "Instant Amps:")) {
    double current = strtod(p, 0);
    // We see a lot of intermittent zero-current readings that are apparently spurious.
    // If this reading is exactly zero, keep the previous reading.
    // This is a rare instance of intentionally testing a double against _exactly_ zero.
    if(current != 0)
      smartmeterData.Current = current;
    rc = 4;
  }
  if(p = strstrAfter(str, "Meter sum A:")) {
    smartmeterData.MeterSum = strtod(p, 0);
    rc = 5;
  }
  if(p = strstrAfter(str, "Local sum A:")) {
    smartmeterData.LocalSum = strtod(p, 0);
    rc = 6;
  }
  if(p = strstrAfter(str, "Solar sum A:")) {
    smartmeterData.SolarSum = strtod(p, 0);
    rc = 7;
  }
  if(p = strstrAfter(str, "Delta sum A:")) {
    rc = 10;
  }

  return rc;
}

char *strstrAfter(const char *str1, const char *str2) {
  char *p;
  p = strstr(str1, str2);
  if(p) p += strlen(str2);
  return p;
}

// Power reduction for ESP32 (not fully working)

void modemSleepSet() {
  WiFi.setSleep(true);
  if (!setCpuFrequencyMhz(40)){
    Serial.println("Not valid frequency!");
  }
  // Use this if 40Mhz is not supported
  // setCpuFrequencyMhz(80);
}

void modemSleepWake() {
  setCpuFrequencyMhz(240);
}
