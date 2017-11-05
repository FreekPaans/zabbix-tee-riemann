# zabbix-tee-riemann

zabbix-tee-riemann is a proxy for zabbix that forwards the data to both zabbix and riemann. This allows you to use your existing zabbix configurations _and_ use the cool stuff from riemann.

## Usage

`lein run` should do the trick to run the server. Point your zabbix agent to the address of this service instead of the zabbix server.

## Disclaimer

Use at your own risk.
