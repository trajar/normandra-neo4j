package org.normandra.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.normandra.DatabaseConstruction;
import org.normandra.DatabaseSession;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.graph.GraphDatabase;
import org.normandra.meta.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Neo4jDatabase implements GraphDatabase {
    private static final Logger logger = LoggerFactory.getLogger(Neo4jDatabase.class);

    private final URL url;

    private final GraphMeta meta;

    private final EntityCacheFactory cacheFactory;

    private final GraphDatabaseFactory databaseFactory;

    public static Neo4jDatabase createLocalEmbedded(
            final File path,
            final EntityCacheFactory factory,
            final GraphMetaBuilder builder) {
        final GraphMeta meta = builder.create();
        try {
            return new Neo4jDatabase(path.toURI().toURL(), factory, meta);
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    protected Neo4jDatabase(final URL url, final EntityCacheFactory cache, final GraphMeta meta) {
        this.url = url;
        this.meta = meta;
        this.cacheFactory = cache;
        this.databaseFactory = new GraphDatabaseFactory();
    }

    public GraphMeta getMeta() {
        return this.meta;
    }

    @Override
    public Neo4jGraph createGraph() {
        final GraphDatabaseService database = this.createNeo4j(false);
        return new Neo4jGraph(this.meta, this.cacheFactory.create(), database);
    }

    @Override
    public DatabaseSession createSession() {
        return this.createGraph();
    }

    @Override
    public void refreshWith(final DatabaseMeta databaseMeta, final DatabaseConstruction constructionMode) throws NormandraException {
        final Set<EntityMeta> metas = new HashSet<>();
        metas.addAll(databaseMeta.getEntities());
        this.refreshWithEntities(metas, constructionMode);
    }

    @Override
    public void refreshWith(final GraphMeta graphMeta, final DatabaseConstruction constructionMode) throws NormandraException {
        final Set<EntityMeta> metas = new HashSet<>();
        metas.addAll(graphMeta.getNodeEntities());
        metas.addAll(graphMeta.getEdgeEntities());
        metas.addAll(graphMeta.getEntities());
        this.refreshWithEntities(metas, constructionMode);
    }

    private void refreshWithEntities(final Set<EntityMeta> metas, final DatabaseConstruction constructionMode) throws NormandraException {
        if (DatabaseConstruction.NONE.equals(constructionMode)) {
            return;
        }

        final GraphDatabaseService database = this.createNeo4j(false);
        try (final Transaction tx = database.beginTx()) {
            for (final EntityMeta entity : metas) {
                // build label
                final Label label = Label.label(entity.getTable());
                if (!database.schema().getConstraints(label).iterator().hasNext()) {
                    if (entity.getPrimaryKeys().size() > 1) {
                        logger.warn("Unable to setup unique constraints for composite primary keys for [" + label + "], index only.");
                    } else if (!entity.getPrimaryKeys().isEmpty()) {
                        ConstraintCreator creator = database.schema().constraintFor(label);
                        for (final ColumnMeta column : entity.getPrimaryKeys()) {
                            creator = creator.assertPropertyIsUnique(column.getName());
                        }
                        creator.create();
                    }
                }
                // neo4j only supports index-per-column in 4.0+, so find all the columns we want
                final List<ColumnMeta> columns = new ArrayList<>();
                for (final IndexMeta index : entity.getIndexed()) {
                    columns.addAll(index.getColumns());
                }
                for (final ColumnMeta column : columns) {
                    if (!hasIndex(database.schema(), label, column.getName())) {
                        database.schema().indexFor(label).on(column.getName()).create();
                    }
                }
            }
            tx.success();
        } catch (final Exception e) {
            throw new NormandraException("Unable to initialize graph schema.", e);
        } finally {
            database.shutdown();
        }
    }

    @Override
    public void close() {

    }

    private boolean hasIndex(final Schema schema, final Label label, final String property) {
        if (null == property || null == schema || null == label) {
            return false;
        }
        for (final IndexDefinition index : schema.getIndexes(label)) {
            for (final String key : index.getPropertyKeys()) {
                if (property.equalsIgnoreCase(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    private GraphDatabaseService createNeo4j(final boolean readOnly) {
        if ("file".equalsIgnoreCase(this.url.getProtocol())) {
            try {
                final File path = new File(this.url.toURI());
                if (readOnly) {
                    return this.databaseFactory.newEmbeddedDatabaseBuilder(path).setConfig(GraphDatabaseSettings.read_only, "true").newGraphDatabase();
                } else {
                    return this.databaseFactory.newEmbeddedDatabase(path);
                }
            } catch (final URISyntaxException e) {
                throw new IllegalStateException("Unable to convert URL to file [" + this.url + "].");
            }
        }

        throw new IllegalStateException("Unable to create remote neo4j graph.");
    }
}
