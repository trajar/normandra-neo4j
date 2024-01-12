package org.normandra.neo4j;

import org.apache.commons.io.FileUtils;
import org.normandra.*;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.cache.MapFactory;
import org.normandra.cache.StrongMemoryCache;
import org.normandra.graph.GraphManager;
import org.normandra.graph.GraphManagerFactory;
import org.normandra.meta.DatabaseMetaBuilder;
import org.normandra.meta.GraphMetaBuilder;

import java.io.File;
import java.nio.file.Path;

public class Neo4jHelper implements TestHelper {

    private Path databaseDir = new File("neo4j.db").toPath();

    private static String databaseName = "neo4j";

    private Neo4jDatabase database;

    private GraphManager graphManager;

    private EntityManager entityManager;

    private DatabaseConstruction constructionMode = DatabaseConstruction.CREATE_SCHEMA;

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
        cleanupDirs();
        EntityCacheFactory cacheFactory = new StrongMemoryCache.Factory(MapFactory.withConcurrency());
        this.database = Neo4jDatabase.createLocalEmbedded(databaseDir, databaseName, cacheFactory, builder.asGraph());
        this.entityManager = new EntityManagerFactory(this.database, this.database.getMeta(), this.constructionMode).create();
    }

    @Override
    public void create(GraphMetaBuilder builder) throws Exception {
        cleanupDirs();
        EntityCacheFactory cacheFactory = new StrongMemoryCache.Factory(MapFactory.withConcurrency());
        this.database = Neo4jDatabase.createLocalEmbedded(databaseDir, databaseName, cacheFactory, builder);
        this.graphManager = new GraphManagerFactory(this.database, this.database.getMeta(), this.constructionMode).create();
    }

    private void cleanupDirs() throws Exception {
        final File logdir = new File("logs");
        final File lockfile = new File("store_lock");
        if (databaseDir.toFile().exists()) {
            FileUtils.forceDelete(databaseDir.toFile());
        }
        if (logdir.exists()) {
            FileUtils.forceDelete(logdir);
        }
        if (lockfile.exists()) {
            FileUtils.forceDelete(lockfile);
        }
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
        cleanupDirs();
    }
}
