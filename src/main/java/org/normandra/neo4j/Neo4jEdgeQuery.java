package org.normandra.neo4j;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.normandra.Transaction;
import org.normandra.graph.Edge;
import org.normandra.graph.EdgeQuery;
import org.normandra.meta.EntityMeta;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class Neo4jEdgeQuery<T> implements EdgeQuery<T> {
    private final Neo4jGraph graph;

    private final EntityMeta meta;

    private final String query;

    private final Map<String, Object> parameters;

    private Result result = null;

    public Neo4jEdgeQuery(final Neo4jGraph graph, final EntityMeta meta, final String query) {
        this(graph, meta, query, Collections.emptyMap());
    }

    public Neo4jEdgeQuery(final Neo4jGraph graph, final EntityMeta meta, final String query, final Map<String, Object> params) {
        this.graph = graph;
        this.meta = meta;
        this.query = query;
        this.parameters = params;
    }

    @Override
    public void close() throws Exception {
        this.closeResults();
    }

    private void closeResults() throws Exception {
        if (this.result != null) {
            this.result.close();
            this.result = null;
        }
    }

    @Override
    public Iterator<Edge<T>> iterator() {
        try {
            this.closeResults();
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to close existing results.", e);
        }

        try {
            final Transaction tx = this.graph.beginTransaction();
            if (!this.parameters.isEmpty()) {
                this.result = this.graph.tx().execute(this.query, this.parameters);
            } else {
                this.result = this.graph.tx().execute(this.query);
            }
            return new EdgeIterator<>(this.graph, tx, this.meta, this.result);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to execute edge query.", e);
        }
    }

    private static class EdgeIterator<T> implements Iterator<Edge<T>> {
        private final Neo4jGraph graph;

        private final Transaction tx;

        private final EntityMeta meta;

        private final Result result;

        private String edgeColumn = "e";

        private final ResourceIterator iterator;

        private EdgeIterator(final Neo4jGraph g, final Transaction tx, final EntityMeta m, final Result result) {
            this.graph = g;
            this.meta = m;
            this.tx = tx;
            this.result = result;
            if (result.columns().size() == 1) {
                this.edgeColumn = result.columns().get(0);
            } else {
                for (final String possibleName : Arrays.asList("e", "edge")) {
                    for (final String column : result.columns()) {
                        if (possibleName.equalsIgnoreCase(column)) {
                            this.edgeColumn = column;
                            break;
                        }
                    }
                }
            }
            if (!this.result.columns().contains(this.edgeColumn)) {
                throw new IllegalStateException("Unable find edge column from results.");
            }
            this.iterator = this.result.columnAs(this.edgeColumn);
        }

        @Override
        public boolean hasNext() {
            if (this.iterator.hasNext()) {
                return true;
            } else {
                try {
                    this.tx.close();
                    this.iterator.close();
                    this.result.close();
                    return false;
                } catch (final Exception e) {
                    throw new IllegalStateException("Unable to close query transaction or results.");
                }
            }
        }

        @Override
        public Edge<T> next() {
            final Object e = this.iterator.next();
            if (e instanceof org.neo4j.graphdb.Relationship) {
                try {
                    final org.neo4j.graphdb.Relationship relationship = (org.neo4j.graphdb.Relationship) e;
                    return this.graph.buildEdge(relationship, this.meta);
                } catch (final Exception ex) {
                    throw new IllegalStateException("Unable to build edge.", ex);
                }
            } else {
                throw new IllegalStateException("Unexpected element type [" + e.getClass() + "] - expected edge.");
            }
        }
    }
}
