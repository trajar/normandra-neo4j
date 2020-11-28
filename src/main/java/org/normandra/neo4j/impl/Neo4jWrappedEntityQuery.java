package org.normandra.neo4j.impl;

import org.normandra.DatabaseQuery;
import org.normandra.NormandraException;
import org.normandra.graph.Node;
import org.normandra.graph.NodeQuery;

import java.util.Iterator;

public class Neo4jWrappedEntityQuery<T> implements DatabaseQuery<T> {
    private final NodeQuery<T> delegate;

    private T firstItem = null;

    public Neo4jWrappedEntityQuery(NodeQuery<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T first() {
        if (this.firstItem != null) {
            return this.firstItem;
        }

        for (final T item : this) {
            if (item != null) {
                this.firstItem = item;
                return item;
            }
        }

        return null;
    }

    @Override
    public void close() throws Exception {
        this.delegate.close();
    }

    @Override
    public Iterator<T> iterator() {
        return new WrappedIterator<>(this.delegate.iterator());
    }

    private static class WrappedIterator<T> implements Iterator<T> {
        private final Iterator<Node<T>> delegate;

        private WrappedIterator(final Iterator<Node<T>> d) {
            this.delegate = d;
        }

        @Override
        public boolean hasNext() {
            return this.delegate.hasNext();
        }

        @Override
        public T next() {
            final Node<T> node = this.delegate.next();
            if (null == node) {
                return null;
            }
            try {
                return node.getEntity();
            } catch (NormandraException e) {
                throw new IllegalStateException("Unable to get next node entity from query.", e);
            }
        }
    }
}
