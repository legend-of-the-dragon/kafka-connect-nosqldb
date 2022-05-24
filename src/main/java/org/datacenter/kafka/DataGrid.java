package org.datacenter.kafka;

import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.Service;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.UrlResource;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
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

    public void init(String cfgPath) {
        try {
            this.gridLock.acquire();
            if (this.ignite == null) {
                IgniteConfiguration cfg =
                        createConfiguration(
                                cfgPath, String.format("KAFKA-%s-CONNECTOR", this.name()));
                this.ignite = Ignition.start(cfg);
            }

            this.igniteClientCnt.incrementAndGet();
            log.info("Connected to " + this.name());
        } catch (InterruptedException e) {
            log.error("ignite init exception.", e);
            throw new ConnectException(e);
        } finally {
            this.gridLock.release();
        }
    }

    public void close() {
        try {
            this.gridLock.acquire();
            int cnt = this.igniteClientCnt.decrementAndGet();
            if (cnt < 0) {
                throw new IllegalStateException("DataGrid is not initialized.");
            }

            if (cnt == 0) {
                this.ignite.close();
                this.ignite = null;
                log.info("Disconnected from " + this.name());
            }
        } catch (InterruptedException var5) {
        } finally {
            this.gridLock.release();
        }
    }

    public Collection<String> cacheNames() {
        this.ensureInitialized();
        return this.ignite.cacheNames();
    }

    public IgniteCache<BinaryObject, BinaryObject> cache(String name) {
        this.ensureInitialized();
        IgniteCache<BinaryObject, BinaryObject> cache = this.ignite.cache(name);
        return cache == null ? null : cache.withKeepBinary();
    }

    public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) {
        this.ensureInitialized();
        return this.ignite.dataStreamer(cacheName);
    }

    public IgniteBinary binary() {
        this.ensureInitialized();
        return this.ignite.binary();
    }

    public void deployService(String name, Service svc) {
        this.ensureInitialized();
        this.ignite.services().deployClusterSingleton(name, svc);
    }

    public void removeService(String name) {
        this.ensureInitialized();
        this.ignite.services().cancel(name);
    }

    public <T> boolean isServiceDeployed(String name, Class<? super T> cls, Consumer<T> test) {
        this.ensureInitialized();

        try {
            T svc = this.ignite.services().serviceProxy(name, cls, false);
            test.accept(svc);
            return true;
        } catch (IgniteException var5) {
            return false;
        }
    }

    public void ensureCache(String name) {
        this.ensureInitialized();
        this.ignite.getOrCreateCache(name);
    }

    public IgniteConfiguration configuration() {
        this.ensureInitialized();
        return this.ignite.configuration();
    }

    private void ensureInitialized() {
        if (this.ignite == null) {
            throw new IllegalStateException(String.format("%s is not initialized.", this.name()));
        }
    }
}
