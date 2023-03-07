package com.risingwave.metrics;

import static io.grpc.Status.INTERNAL;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetSocketAddress;

public class ConnectorNodeMetrics {
    private static final Counter activeConnections =
            Counter.build()
                    .name("active_connections")
                    .labelNames("sink_type", "ip")
                    .help("Number of active connections")
                    .register();

    private static final Counter totalConnections =
            Counter.build()
                    .name("total_connections")
                    .labelNames("sink_type", "ip")
                    .help("Number of total connections")
                    .register();
    private static final Gauge cpuUsage =
            Gauge.build()
                    .name("cpu_usage")
                    .labelNames("node_id")
                    .help("CPU usage in percentage")
                    .register();
    private static final Gauge ramUsage =
            Gauge.build()
                    .name("ram_usage")
                    .labelNames("node_id")
                    .help("RAM usage in bytes")
                    .register();

    private static final Counter sinkRowsReceived =
            Counter.build()
                    .name("sink_rows_received")
                    .help("Number of rows received by sink")
                    .register();

    private static final Counter errorCount =
            Counter.build()
                    .name("error_count")
                    .labelNames("sink_type", "ip")
                    .help("Number of errors")
                    .register();

    static class PeriodicMetricsCollector extends Thread {
        private final int interval;
        private final OperatingSystemMXBean osBean;
        private final String nodeId;

        public PeriodicMetricsCollector(int intervalMillis, String nodeId) {
            this.interval = intervalMillis;
            this.nodeId = nodeId;
            this.osBean = ManagementFactory.getOperatingSystemMXBean();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(interval);
                    collect();
                } catch (InterruptedException e) {
                    throw INTERNAL.withCause(e).asRuntimeException();
                }
            }
        }

        private void collect() {
            double cpuUsage = osBean.getSystemLoadAverage();
            ConnectorNodeMetrics.cpuUsage.labels(nodeId).set(cpuUsage);
            long ramUsageBytes =
                    Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            ConnectorNodeMetrics.ramUsage.labels(nodeId).set(ramUsageBytes);
        }
    }

    public static void startHTTPServer(int port) {
        CollectorRegistry registry = new CollectorRegistry();
        registry.register(activeConnections);
        registry.register(cpuUsage);
        registry.register(ramUsage);
        PeriodicMetricsCollector collector = new PeriodicMetricsCollector(1000, "node1");
        collector.start();

        try {
            HTTPServer server = new HTTPServer(new InetSocketAddress("localhost", 60071), registry);
        } catch (IOException e) {
            throw INTERNAL.withDescription("Failed to start HTTP server")
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    public static void incActiveConnections(String sinkType, String ip) {
        activeConnections.labels(sinkType, ip).inc();
    }

    public static void decActiveConnections(String sinkType, String ip) {
        activeConnections.remove(sinkType, ip);
    }

    public static void incSinkRowsReceived() {
        sinkRowsReceived.inc();
    }

    public static void incTotalConnections(String sinkType, String ip) {
        totalConnections.labels(sinkType, ip).inc();
    }

    public static void incErrorCount(String sinkType, String ip) {
        errorCount.labels(sinkType, ip).inc();
    }

    public static void setCpuUsage(String ip, double cpuUsagePercentage) {
        cpuUsage.labels(ip).set(cpuUsagePercentage);
    }

    public static void setRamUsage(String ip, long usedRamInBytes) {
        ramUsage.labels(ip).set(usedRamInBytes);
    }
}
