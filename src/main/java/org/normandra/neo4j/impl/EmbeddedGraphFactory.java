package org.normandra.neo4j.impl;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.normandra.DatabaseConstruction;
import org.normandra.NormandraException;
import org.normandra.cache.EntityCacheFactory;
import org.normandra.meta.ColumnMeta;
import org.normandra.meta.EntityMeta;
import org.normandra.meta.GraphMeta;
import org.normandra.meta.IndexMeta;
import org.normandra.neo4j.Neo4jGraph;
import org.normandra.neo4j.Neo4jGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EmbeddedGraphFactory implements Neo4jGraphFactory {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedGraphFactory.class);

    private final File graphDir;

    private final GraphMeta meta;

    private final EntityCacheFactory cache;

    private final String databaseName;

    private final DatabaseManagementService managementService;

    public EmbeddedGraphFactory(final File graphDir, final String database, final GraphMeta meta, final EntityCacheFactory cache) {
        this.graphDir = graphDir;
        this.meta = meta;
        this.cache = cache;
        this.databaseName = database;
        this.managementService = new DatabaseManagementServiceBuilder(graphDir).build();
    }

    @Override
    public Neo4jGraph create() {
        return new Neo4jGraph(this.meta, this.cache.create(), this.managementService.database(this.databaseName));
    }

    @Override
    public void refresh(final Set<EntityMeta> metas, final DatabaseConstruction constructionMode) throws NormandraException {
        if (DatabaseConstruction.NONE.equals(constructionMode)) {
            return;
        }

        final GraphDatabaseService databaseService = this.managementService.database(this.databaseName);
        try (final Transaction tx = databaseService.beginTx()) {
            for (final EntityMeta entity : metas) {
                // build label
                final Label label = Label.label(entity.getTable());
                if (!tx.schema().getConstraints(label).iterator().hasNext()) {
                    if (entity.getPrimaryKeys().size() > 1) {
                        logger.warn("Unable to setup unique constraints for composite primary keys for [" + label + "], index only.");
                    } else if (!entity.getPrimaryKeys().isEmpty()) {
                        ConstraintCreator creator = tx.schema().constraintFor(label);
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
                    if (!hasIndex(tx.schema(), label, column.getName())) {
                        tx.schema().indexFor(label).on(column.getName()).create();
                    }
                }
            }
            tx.commit();
        } catch (final Exception e) {
            throw new NormandraException("Unable to initialize graph schema.", e);
        }
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

    @Override
    public void close() {
        this.managementService.shutdown();
    }
}
