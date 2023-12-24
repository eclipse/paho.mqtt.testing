# Eclipse Paho Testing Utilities

The Paho Testing Utilities are a collection of code and tools to help test MQTT clients and Brokers.

The components, or capabilities include:

- a Python MQTT broker which implements versions 3.1.1 and 5.0 (plus the start of MQTT-SN support)
- a simple Python MQTT client, also supporting versions 3.1.1 and 5.0, used for simple general test suites
- an MQTT network proxy, which can forward traffic to and from a broker, and display the MQTT packet info
- Python modules to de/serialize MQTT packets for MQTT 3.1.1 and 5.0
- an MQTT load/connection loss test, designed to investigate the reconnection logic for QoS 1 and 2 flows

## Links

- Project Website: [https://www.eclipse.org/paho](https://www.eclipse.org/paho)
- Eclipse Project Information: [https://projects.eclipse.org/projects/iot.paho](https://projects.eclipse.org/projects/iot.paho)
- Paho Testing Page: [https://www.eclipse.org/paho/clients/testing/](https://www.eclipse.org/paho/clients/testing/)
- GitHub: [https://github.com/eclipse/paho.mqtt.testing](https://github.com/eclipse/paho.mqtt.testing)
- Twitter: [@eclipsepaho](https://twitter.com/eclipsepaho)
- Issues: [https://github.com/eclipse/paho.mqtt.testing/issues](https://github.com/eclipse/paho.mqtt.testing/issues)
- Mailing-list: [https://dev.eclipse.org/mailman/listinfo/paho-dev](https://dev.eclipse.org/mailman/listinfo/paho-dev)

## Usage

### MQTT Version 5

Start a broker:

  python3 startbroker.py

Run client tests:

  python3 client_test5.py

various options are available, individual tests can be run with:

  python3 client_test5.py Test.test_name  

As yet unimplemented features:

  https://github.com/eclipse/paho.mqtt.testing/issues

Sub-packages:

  mqtt/formats/MQTTV5 - packet serialization and deserialization
  mqtt/clients/V5 - test client implementation
  mqtt/brokers/V5 - test broker implementation

### MQTT Version 3

Start a broker:

  python3 startbroker.py

Run client tests:

  python3 client_test.py

### TLS

A configuration file similar to that of Eclipse Mosquitto can be passed to startbroker:

  python3 startbroker.py -c client_testing.conf

so for example, a TCP listener with TLS support can be configured like this:

  listener 18884
  cafile tls_testing/keys/all-ca.crt
  certfile tls_testing/keys/server/server.crt
  keyfile tls_testing/keys/server/server.key
  require_certificate true

