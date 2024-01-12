package org.normandra.neo4j;

import org.neo4j.graphdb.Result;
import org.normandra.PropertyQuery;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class Neo4jPropertyQuery implements PropertyQuery {
    private final Neo4jGraph graph;

    private final String query;

    private final Map<String, Object> parameters;

    private Result result = null;

    private Map<String, Object> firstItem = null;

    public Neo4jPropertyQuery(final Neo4jGraph graph, final String query) {
        this(graph, query, Collections.emptyMap());
    }

    public Neo4jPropertyQuery(final Neo4jGraph graph, final String query, final Map<String, Object> params) {
        this.graph = graph;
        this.query = query;
        this.parameters = params;
    }

    @Override
    public void close() throws Exception {
        this.closeResults();
    }

    @Override
    public Map<String, Object> first() {
        if (this.firstItem != null) {
            return Collections.unmodifiableMap(this.firstItem);
        }

        for (final Map<String, Object> item : this) {
            if (item != null) {
                this.firstItem = item;
                return Collections.unmodifiableMap(item);
            }
        }

        return null;
    }

    private void closeResults() throws Exception {
        if (this.result != null) {
            this.result.close();
            this.result = null;
        }
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        try {
            this.closeResults();
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to close existing results.", e);
        }

        if (!this.parameters.isEmpty()) {
            this.result = this.graph.tx().execute(this.query, this.parameters);
        } else {
            this.result = this.graph.tx().execute(this.query);
        }
        return new PropertySetIterator(this.result);
    }

    private static class PropertySetIterator implements Iterator<Map<String, Object>> {
        private final Result result;

        private PropertySetIterator(final Result result) {
            this.result = result;
        }

        @Override
        public boolean hasNext() {
            if (this.result.hasNext()) {
                return true;
            } else {
                try {
                    this.result.close();
                    return false;
                } catch (final Exception e) {
                    throw new IllegalStateException("Unable to close query transaction or results.");
                }
            }
        }

        @Override
        public Map<String, Object> next() {
            return this.result.next();
        }
    }
}
