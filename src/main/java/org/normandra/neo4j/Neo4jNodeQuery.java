package org.normandra.neo4j;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.normandra.Transaction;
import org.normandra.graph.Node;
import org.normandra.graph.NodeQuery;
import org.normandra.meta.EntityMeta;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class Neo4jNodeQuery<T> implements NodeQuery<T> {
    private final Neo4jGraph graph;

    private final EntityMeta meta;

    private final String query;

    private final Map<String, Object> parameters;

    private Result result = null;

    public Neo4jNodeQuery(final Neo4jGraph graph, final EntityMeta meta, final String query) {
        this(graph, meta, query, Collections.emptyMap());
    }

    public Neo4jNodeQuery(final Neo4jGraph graph, final EntityMeta meta, final String query, final Map<String, Object> params) {
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
    public Iterator<Node<T>> iterator() {
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
            return new NodeIterator<>(this.graph, tx, this.meta, this.result);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to execute node query.", e);
        }
    }

    private static class NodeIterator<T> implements Iterator<Node<T>> {
        private final Neo4jGraph graph;

        private final Transaction tx;

        private final EntityMeta meta;

        private final Result result;

        private String nodeColumn = "n";

        private final ResourceIterator iterator;

        private NodeIterator(final Neo4jGraph g, final Transaction tx, final EntityMeta m, final Result result) {
            this.graph = g;
            this.meta = m;
            this.tx = tx;
            this.result = result;
            if (result.columns().size() == 1) {
                this.nodeColumn = result.columns().get(0);
            } else {
                for (final String possibleName : Arrays.asList("n", "node")) {
                    for (final String column : result.columns()) {
                        if (possibleName.equalsIgnoreCase(column)) {
                            this.nodeColumn = column;
                            break;
                        }
                    }
                }
            }
            if (!this.result.columns().contains(this.nodeColumn)) {
                throw new IllegalStateException("Unable find node column from results.");
            }
            this.iterator = this.result.columnAs(this.nodeColumn);
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
        public Node<T> next() {
            final Object n = this.iterator.next();
            if (n instanceof org.neo4j.graphdb.Node) {
                try {
                    final org.neo4j.graphdb.Node node = (org.neo4j.graphdb.Node) n;
                    return this.graph.buildNode(node, this.meta);
                } catch (final Exception e) {
                    throw new IllegalStateException("Unable to build node.", e);
                }
            } else {
                throw new IllegalStateException("Unexpected element type [" + n.getClass() + "] - expected node.");
            }
        }
    }
}
