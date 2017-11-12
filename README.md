# zabbix-tee-riemann

zabbix-tee-riemann intercepts zabbix agent <-> zabbix server traffic and forwards the metrics to riemann. This allows you to use your existing zabbix configurations _and_ use the cool stuff from riemann, such as having a dashboard with all the latest metrics and graphing the data using graphite.

## Usage

`lein run` should do the trick to run the server. Point your zabbix agent to the address of this service instead of the zabbix server.

## Disclaimer

Use at your own risk.
