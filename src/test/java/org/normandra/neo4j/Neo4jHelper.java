package org.normandra.neo4j;

import org.apache.commons.io.FileUtils;
import org.normandra.*;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.cache.MapFactory;
import org.normandra.cache.MemoryCache;
import org.normandra.graph.GraphManager;
import org.normandra.graph.GraphManagerFactory;
import org.normandra.meta.DatabaseMetaBuilder;
import org.normandra.meta.GraphMetaBuilder;

import java.io.File;

public class Neo4jHelper implements TestHelper {

    private File databaseDir = new File("neo4j.db");

    private Neo4jDatabase database;

    private GraphManager graphManager;

    private EntityManager entityManager;

    @Override
    public EntityManager getManager() {
        if (entityManager != null) {
            return entityManager;
        }
        throw new IllegalStateException();
    }

    @Override
    public Database getDatabase() {
        if (database != null) {
            return database;
        }
        throw new IllegalStateException();
    }

    @Override
    public GraphManager getGraph() {
        if (graphManager != null) {
            return graphManager;
        }
        throw new IllegalStateException();
    }

    @Override
    public void create(DatabaseMetaBuilder builder) throws Exception {
        if (this.databaseDir.exists()) {
            FileUtils.deleteDirectory(this.databaseDir);
        }

        EntityCacheFactory cacheFactory = new MemoryCache.Factory(MapFactory.withConcurrency());
        this.database = Neo4jDatabase.createLocalEmbedded(databaseDir, cacheFactory, DatabaseConstruction.RECREATE, builder.asGraph());
        this.entityManager = new EntityManagerFactory(this.database, this.database.getMeta()).create();
    }

    @Override
    public void create(GraphMetaBuilder builder) throws Exception {
        if (this.databaseDir.exists()) {
            FileUtils.deleteDirectory(this.databaseDir);
        }

        EntityCacheFactory cacheFactory = new MemoryCache.Factory(MapFactory.withConcurrency());
        this.database = Neo4jDatabase.createLocalEmbedded(databaseDir, cacheFactory, DatabaseConstruction.RECREATE, builder);
        this.graphManager = new GraphManagerFactory(this.database, this.database.getMeta()).create();
    }

    @Override
    public void cleanup() throws Exception {
        if (this.entityManager != null) {
            this.entityManager.close();
            this.entityManager = null;
        }
        if (this.graphManager != null) {
            this.graphManager.close();
            this.graphManager = null;
        }
        if (this.database != null) {
            this.database.close();
            this.database = null;
        }
        if (this.databaseDir.exists()) {
            FileUtils.deleteDirectory(this.databaseDir);
        }
    }
}
