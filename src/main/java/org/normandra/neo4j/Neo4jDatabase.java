package org.normandra.neo4j;

import org.normandra.DatabaseConstruction;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.graph.GraphDatabase;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.GraphMeta;
import org.normandra.meta.GraphMetaBuilder;
import org.normandra.neo4j.impl.EmbeddedGraphFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class Neo4jDatabase implements GraphDatabase, Closeable {
    private final URI url;

    private final GraphMeta meta;

    private final Neo4jGraphFactory databaseFactory;

    public static Neo4jDatabase createLocalEmbedded(
            final File path,
            final String database,
            final EntityCacheFactory cacheFactory,
            final GraphMetaBuilder builder) {
        final GraphMeta meta = builder.create();
        final EmbeddedGraphFactory embeddedFactory = new EmbeddedGraphFactory(path, database, meta, cacheFactory);
        return new Neo4jDatabase(path, meta, embeddedFactory);
    }

    protected Neo4jDatabase(final File databaseDir, final GraphMeta meta, final Neo4jGraphFactory factory) {
        this.url = databaseDir.toURI();
        this.meta = meta;
        this.databaseFactory = factory;
    }

    public GraphMeta getMeta() {
        return this.meta;
    }

    @Override
    public Neo4jGraph createGraph() {
        return this.databaseFactory.create();
    }

    @Override
    public DatabaseSession createSession() {
        return this.createGraph();
    }

    @Override
    public void refreshWith(final DatabaseMeta databaseMeta, final DatabaseConstruction constructionMode) throws NormandraException {
        final Set<EntityMeta> metas = new HashSet<>();
        metas.addAll(databaseMeta.getEntities());
        this.databaseFactory.refresh(metas, constructionMode);
    }

    @Override
    public void refreshWith(final GraphMeta graphMeta, final DatabaseConstruction constructionMode) throws NormandraException {
        final Set<EntityMeta> metas = new HashSet<>();
        metas.addAll(graphMeta.getNodeEntities());
        metas.addAll(graphMeta.getEdgeEntities());
        metas.addAll(graphMeta.getEntities());
        this.databaseFactory.refresh(metas, constructionMode);
    }

    @Override
    public void close() throws IOException {
        this.databaseFactory.close();
    }
}
