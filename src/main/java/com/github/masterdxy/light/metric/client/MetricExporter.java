package com.github.masterdxy.light.metric.client;

import com.github.masterdxy.light.metric.client.internal.CollectorRegistry;
import com.github.masterdxy.light.metric.client.internal.Counter;
import com.github.masterdxy.light.metric.client.internal.Gauge;
import com.github.masterdxy.light.metric.client.internal.Histogram;
import com.github.masterdxy.light.metric.client.internal.Summary;
import com.github.masterdxy.light.metric.client.internal.TextFormat;
import com.github.masterdxy.light.metric.client.internal.hotspot.DefaultExports;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * @author tomoyo
 */
public class MetricExporter {

    private static final Logger logger = LoggerFactory.getLogger(MetricExporter.class);

    private static final Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>();
    private static final Map<String, Summary> summarys = new ConcurrentHashMap<String, Summary>();
    private static final Map<String, Gauge> gauges = new ConcurrentHashMap<String, Gauge>();
    private static final Map<String, Histogram> histograms = new ConcurrentHashMap<String, Histogram>();

    private static MetricExporter instance = null;

    private static volatile boolean hasExported = false;
    private static volatile boolean hasException = false;


    public void doExport(final String ip, final int port) {
        if (hasExported) {
            return;
        }
        synchronized (Object.class) {
            if(hasExported || hasException){
                return;
            }
            if (port <= 0) {
                logger.error("metric export port cant be : " + port);
                throw new RuntimeException("metric client ip is null");
            }
            if (ip == null || "".equals(ip)) {
                logger.error("metric client ip is null! ");
                throw new RuntimeException("metric client ip is null");
            }
            try {
                InetSocketAddress addr = new InetSocketAddress(ip, port);
                HttpServer server = HttpServer.create(addr, 128);
                server.createContext("/service_metrics", exchange -> {
                    Headers responseHeaders = exchange.getResponseHeaders();
                    responseHeaders.set("Content-Type", TextFormat.CONTENT_TYPE_004);
                    exchange.sendResponseHeaders(200, 0);
                    OutputStream responseBody = exchange.getResponseBody();
                    OutputStreamWriter writer = new OutputStreamWriter(responseBody);
                    TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
                    writer.flush();
                    writer.close();
                });
                server.setExecutor(Executors.newFixedThreadPool(2));
                server.start();
                hasExported = true;
            } catch (Exception e) {
                logger.error("MetricExporter error : {}", e);
                hasException = true;
            }
            logger.info("MetricExporter on " + ip + " : " + port + " started.");
        }
    }

    private MetricExporter() {

    }

    public synchronized Counter getCounter(final String name, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Counter name cant be null or empty.");
        }
        Counter counter = counters.get(name);
        if (counter == null) {
            synchronized (counters) {
                counter = counters.get(name);
                if (counter == null) {
                    counter = Counter.build().name(name).help(help).register();
                    counters.putIfAbsent(name, counter);
                }
            }
        }
        return counter;
    }

    public Counter getCounter(final String name, final String lableNames, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Counter name cant be null or empty.");
        }
        Counter counter = counters.get(name);
        if (counter == null) {
            synchronized (counters) {
                counter = counters.get(name);
                if (counter == null) {
                    counter = Counter.build().name(name).help(help).labelNames(lableNames).register();
                    counters.put(name, counter);
                }
            }
        }
        return counter;
    }

    public Summary getSummary(final String name, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Summary name cant be null or empty.");
        }
        Summary summary = summarys.get(name);
        if (summary == null) {
            synchronized (summarys) {
                summary = summarys.get(name);
                if (summary == null) {
                    summary = Summary.build().name(name).help(help).register();
                    summarys.put(name, summary);
                }
            }
        }
        return summary;
    }

    public Summary getSummary(final String name, final String lableNames, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Summary name cant be null or empty.");
        }
        Summary summary = summarys.get(name);
        if (summary == null) {
            synchronized (summarys) {
                summary = summarys.get(name);
                if (summary == null) {
                    summary = Summary.build().name(name).help(help).labelNames(lableNames).register();
                    summarys.put(name, summary);
                }
            }
        }
        return summary;
    }

    public Gauge getGauge(final String name, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Gauge name cant be null or empty.");
        }
        Gauge gauge = gauges.get(name);
        if (gauge == null) {
            synchronized (gauges) {
                gauge = gauges.get(name);
                if (gauge == null) {
                    gauge = Gauge.build().name(name).help(help).register();
                    gauges.put(name, gauge);
                }
            }
        }
        return gauge;
    }

    public Gauge getGauge(final String name, final String lableNames, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Gauge name cant be null or empty.");
        }
        Gauge gauge = gauges.get(name);
        if (gauge == null) {
            synchronized (gauges) {
                gauge = gauges.get(name);
                if (gauge == null) {
                    gauge = Gauge.build().name(name).help(help).labelNames(lableNames).register();
                    gauges.put(name, gauge);
                }
            }
        }
        return gauge;
    }

    public Histogram getHistogram(final String name, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Gauge name cant be null or empty.");
        }
        Histogram histogram = histograms.get(name);
        if (histogram == null) {
            synchronized (histograms) {
                histogram = histograms.get(name);
                if (histogram == null) {
                    histogram = Histogram.build().name(name).help(help).register();
                    histograms.put(name, histogram);
                }
            }
        }
        return histogram;
    }

    public Histogram getHistogram(final String name, final String lableNames, final String help) {
        if (name == null || "".equals(name)) {
            throw new NullPointerException("Gauge name cant be null or empty.");
        }
        Histogram histogram = histograms.get(name);
        if (histogram == null) {
            synchronized (histograms) {
                histogram = histograms.get(name);
                if (histogram == null) {
                    histogram = Histogram.build().name(name).help(help).labelNames(lableNames).register();
                    histograms.put(name, histogram);
                }
            }
        }
        return histogram;
    }

    /**
     * 获取MetricExporter，并export
     *
     * @param ip
     * @param port
     * @return
     */
    public static MetricExporter getInstance(String ip, int port) {
        instance = getInstance();
        if (!hasExported && !hasException) {
            instance.doExport(ip, port);
            DefaultExports.initialize();
        }
        return instance;
    }

    /**
     * 获取MetricExporter
     *
     * @return
     */
    public static MetricExporter getInstance() {

        if (instance == null) {
            synchronized (Object.class) {
                if (instance == null) {
                    instance = new MetricExporter();
                    return instance;
                } else {
                    return instance;
                }
            }
        }
        return instance;
    }
}
