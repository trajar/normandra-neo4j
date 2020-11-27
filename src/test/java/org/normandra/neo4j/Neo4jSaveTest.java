package org.normandra.neo4j;

import org.normandra.SaveTest;

public class Neo4jSaveTest extends SaveTest {
    public Neo4jSaveTest() {
        super(new Neo4jHelper());
    }
}
