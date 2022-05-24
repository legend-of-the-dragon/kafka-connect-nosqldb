package org.datacenter.kafka;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import javax.cache.Cache;
import java.util.Objects;

/**
 * @author sky
 * @date 2022-05-
 * @discription
 */
public final class CacheEntry implements Cache.Entry<Object, Object> {

    @QuerySqlField private final Object key;
    @QuerySqlField private final Object val;

    @QuerySqlField(index = true)
    private final String cache;

    public CacheEntry(String cache, Cache.Entry<?, ?> entry) {
        this.cache = cache;
        if (entry != null) {
            this.key = entry.getKey();
            this.val = entry.getValue();
        } else {
            this.key = this.val = null;
        }
    }

    public CacheEntry(String cache, Object key, Object val) {
        this.cache = cache;
        this.key = key;
        this.val = val;
    }

    public Object getKey() {
        return this.key;
    }

    public Object getValue() {
        return this.val;
    }

    public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(this.getClass())) {
            return cls.cast(this);
        } else {
            throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
        }
    }

    public String getCache() {
        return this.cache;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof CacheEntry)) {
            return false;
        } else {
            CacheEntry other = (CacheEntry) obj;
            return Objects.equals(this.key, other.key)
                    && Objects.equals(this.val, other.val)
                    && Objects.equals(this.cache, other.cache);
        }
    }

    public int hashCode() {
        int res = 11;
        if (this.key != null) {
            res += 31 * res + this.key.hashCode();
        }

        if (this.val != null) {
            res += 31 * res + this.val.hashCode();
        }

        if (this.cache != null) {
            res += 31 * res + this.cache.hashCode();
        }

        return res;
    }

    public String toString() {
        return String.format("[%s -> %s]@%s", this.key, this.val, this.cache);
    }
}
