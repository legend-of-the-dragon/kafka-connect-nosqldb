package org.datacenter.kafka.sink.ignite;

import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.UrlResource;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ignite 辅助工具类
 *
 * @author sky @Date 2022-05-10
 * @discription
 */
public enum DataGrid implements AutoCloseable {
    SOURCE,
    SINK;

    private static final Logger log = LoggerFactory.getLogger(DataGrid.class);
    private final Semaphore gridLock = new Semaphore(1);
    private final AtomicInteger igniteClientCnt = new AtomicInteger(0);
    private volatile Ignite ignite;

    DataGrid() {}

    private static IgniteConfiguration createConfiguration(String cfgPath, String gridName) {
        IgniteConfiguration cfg;
        if (cfgPath != null && !cfgPath.isEmpty()) {
            URL url = null;

            try {
                url = new URL(cfgPath);
            } catch (MalformedURLException var8) {
                File file = new File(cfgPath);
                if (file.exists()) {
                    try {
                        url = file.toURI().toURL();
                    } catch (MalformedURLException var7) {
                        throw new ConnectException(var7);
                    }
                } else {
                    ClassLoader clsLdr = Thread.currentThread().getContextClassLoader();
                    if (clsLdr != null) {
                        url = clsLdr.getResource(cfgPath.replaceAll("\\\\", "/"));
                    }

                    if (url == null) {
                        throw new ConnectException("Configuration file is not found.");
                    }
                }
            }

            cfg =
                    (new GenericXmlApplicationContext(new UrlResource(url)))
                            .getBean(IgniteConfiguration.class);
        } else {
            cfg = new IgniteConfiguration();
        }

        if (cfg.isClientMode() == null || !cfg.isClientMode()) {
            log.info("set client mode");
            cfg.setClientMode(true);
        }

        cfg.setIgniteInstanceName(
                cfg.getIgniteInstanceName() == null
                        ? gridName
                        : String.format("%s-%s", cfg.getIgniteInstanceName(), gridName));
        log.info("set Ignite instance name to " + cfg.getIgniteInstanceName());
        return cfg;
    }

    public String getIgniteName() {
        return this.ignite.name();
    }

    public synchronized void init(String connectorName, String cfgPath) {
        try {
            this.gridLock.acquire();
            if (this.ignite == null) {
                log.info("ignite is null.");
                initIgnite(connectorName, cfgPath);
            } else {

                IgniteState state = Ignition.state(ignite.name());
                log.info(
                        "ignite sink is ready to initialize,The ignite client current state is:{}",
                        state);

                if (state != IgniteState.STARTED) {
                    log.info(
                            "ignite sink state not is started,try init connector:{} ...",
                            connectorName);
                    initIgnite(connectorName, cfgPath);
                } else {
                    boolean disconnect = false;
                    try {
                        IgniteCache<Object, Object> cache = this.ignite.getOrCreateCache("CACHES");
                        if (!cache.isClosed()) {
                            log.info("ignite sink client is connected.");
                            cache.close();
                        } else {
                            disconnect = true;
                            log.warn("init try connect test cache error.");
                        }
                    } catch (Throwable e) {
                        disconnect = true;
                        log.warn("init try connect error.", e);
                    }
                    if (disconnect) {
                        log.info("ignite sink client is disconnected,now restart.");
                        initIgnite(connectorName, cfgPath);
                    }
                }
            }

            this.igniteClientCnt.incrementAndGet();
            log.info("ignite init " + this.name());
        } catch (InterruptedException e) {
            log.error("ignite init exception.", e);
            throw new ConnectException(e);
        } finally {
            this.gridLock.release();
        }
    }

    private void initIgnite(String connectorName, String cfgPath) {

        log.info("ignite sink client init connector:{},.....", connectorName);
        this.close();

        IgniteConfiguration cfg =
                createConfiguration(cfgPath, String.format("KAFKA-%s-CONNECTOR", this.name()));
        cfg.setIncludeEventTypes(
                EventType.EVT_NODE_SEGMENTED, EventType.EVT_CLIENT_NODE_DISCONNECTED);
        this.ignite = Ignition.start(cfg);
        IgnitePredicate<Event> localListener =
                event -> {
                    log.warn("ignite 集群发生故障.");
                    this.close();
                    log.info("ignite sink now state:{}", Ignition.state(ignite.name()));
                    this.ignite = null;
                    log.info("ignite sink is null?:{}", this.ignite == null);
                    return true;
                };
        ignite.events()
                .localListen(
                        localListener,
                        EventType.EVT_NODE_SEGMENTED,
                        EventType.EVT_CLIENT_NODE_DISCONNECTED);
    }

    public void close() {
        try {
            this.gridLock.acquire();
            int cnt = this.igniteClientCnt.decrementAndGet();
            if (cnt < 0) {
                throw new IllegalStateException("DataGrid is not initialized.");
            }

            if (cnt == 0) {
                if (this.ignite == null) {
                    Ignition.stopAll(true);
                } else {
                    this.ignite.close();
                }
                this.ignite = null;
                log.info("Disconnected from " + this.name());
            }
        } catch (InterruptedException var5) {

        } finally {
            this.gridLock.release();
        }
    }

    public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) {
        this.ensureInitialized();
        return this.ignite.dataStreamer(cacheName);
    }

    public IgniteBinary binary() {
        this.ensureInitialized();
        return this.ignite.binary();
    }

    public void ensureCache(String name) {
        this.ensureInitialized();
        this.ignite.getOrCreateCache(name);
    }

    private void ensureInitialized() {
        if (this.ignite == null) {
            throw new IllegalStateException(String.format("%s is not initialized.", this.name()));
        }
    }
}
