package org.normandra.neo4j;


import org.normandra.GraphTest;

public class Neo4jGraphTest extends GraphTest {
    public Neo4jGraphTest() {
        super(new Neo4jHelper());
    }
}
