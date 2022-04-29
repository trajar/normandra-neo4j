package org.normandra.neo4j;

import org.normandra.DatabaseConstruction;
import org.normandra.NormandraException;
import org.normandra.meta.EntityMeta;

import java.io.Closeable;
import java.util.Set;

/**
 * a factory for creating graph instances
 *
 * @date 11/19/21.
 */
public interface Neo4jGraphFactory extends Closeable {
    Neo4jGraph create();
    void refresh(Set<EntityMeta> metas, DatabaseConstruction constructionMode) throws NormandraException;
}
