# Eclipse Paho Testing Utilities

The Paho Testing Utilities are a collection of code and tools to help test MQTT clients and Brokers.

All the features are currently in the interoperability directory.  The components, or capabilities include:

- a Python MQTT broker which implements versions 3.1.1 and 5.0 (plus the start of MQTT-SN support)
- a simple Python MQTT client, also supporting versions 3.1.1 and 5.0, used for simple general test suites
- an MQTT network proxy, which can forward traffic to and from a broker, and display the MQTT packet info
- Python modules to de/serialize MQTT packets for MQTT 3.1.1 and 5.0
- an MQTT load/connection loss test, designed to investigate the reconnection logic for QoS 1 and 2 flows

Check the readme in the interoperability directory for details.

## Links

- Project Website: [https://www.eclipse.org/paho](https://www.eclipse.org/paho)
- Eclipse Project Information: [https://projects.eclipse.org/projects/iot.paho](https://projects.eclipse.org/projects/iot.paho)
- Paho Testing Page: [https://www.eclipse.org/paho/clients/testing/](https://www.eclipse.org/paho/clients/testing/)
- GitHub: [https://github.com/eclipse/paho.mqtt.testing](https://github.com/eclipse/paho.mqtt.testing)
- Twitter: [@eclipsepaho](https://twitter.com/eclipsepaho)
- Issues: [https://github.com/eclipse/paho.mqtt.testing/issues](https://github.com/eclipse/paho.mqtt.testing/issues)
- Mailing-list: [https://dev.eclipse.org/mailman/listinfo/paho-dev](https://dev.eclipse.org/mailman/listinfo/paho-dev)
