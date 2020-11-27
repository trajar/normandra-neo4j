package org.normandra.neo4j.impl;

import org.neo4j.graphdb.Label;
import org.normandra.NormandraException;
import org.normandra.PropertyQuery;
import org.normandra.data.DataHolder;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.neo4j.Neo4jGraph;
import org.normandra.neo4j.Neo4jUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a basic lazy-loaded data field
 *
 * @date 11/27/20.
 */
public class Neo4jLazyDataHolder<T> implements DataHolder {
    private final Neo4jGraph session;

    private final EntityMeta meta;

    private final Object key;

    private final ColumnMeta column;

    private T value;

    private final AtomicBoolean loaded = new AtomicBoolean(false);

    public Neo4jLazyDataHolder(final Neo4jGraph s, EntityMeta meta, Object key, ColumnMeta column) {
        this.session = s;
        this.meta = meta;
        this.key = key;
        this.column = column;
    }

    @Override
    public T get() throws NormandraException {
        return this.ensureResults();
    }

    private T ensureResults() throws NormandraException {
        if (this.loaded.get()) {
            return this.value;
        }

        try {
            final Label label = Neo4jUtils.getLabel(meta);
            final String q = "MATCH (n:" + label.name() + ") WHERE n." + this.meta.getPrimaryKey().getName() + " = {key} RETURN n." + this.column.getName() + " AS \"f\" LIMIT 1";
            final Map<String, Object> params = new HashMap<>(1);
            params.put("key", Neo4jUtils.packValue(this.meta.getPrimaryKey(), this.key));
            try (final PropertyQuery query = this.session.query(q, params)) {
                final Map<String, Object> row = query.first();
                if (row != null) {
                    this.value = (T) Neo4jUtils.unpackValue(this.column, row.get("f"));
                }
            }
            this.loaded.getAndSet(true);
        } catch (final Exception e) {
            throw new NormandraException("Unable to get orientdb document by key [" + this.key + "].", e);
        }
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Neo4jLazyDataHolder that = (Neo4jLazyDataHolder) o;
        return Objects.equals(meta, that.meta) &&
                Objects.equals(key, that.key) &&
                Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meta, key, column);
    }
}
