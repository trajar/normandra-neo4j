package org.normandra.neo4j;


import org.junit.Assert;
import org.junit.Test;
import org.normandra.GraphTest;
import org.normandra.graph.*;

import java.util.Collections;

public class Neo4jGraphTest extends GraphTest {
    public Neo4jGraphTest() {
        super(new Neo4jHelper());
    }

    @Test
    public void testQueryEdges() throws Exception {
        final GraphManager manager = helper.getGraph();

        Node john = manager.addNode(new SimpleNode("john"));
        Node bob = manager.addNode(new SimpleNode("bob"));
        Node mike = manager.addNode(new SimpleNode("mike"));
        Node fred = manager.addNode(new SimpleNode("fred"));
        john.createEdge(bob, new SimpleEdge("likes"));
        bob.createEdge(john, new SimpleEdge("hates"));
        bob.createEdge(mike, new SimpleEdge("likes"));
        mike.createEdge(fred, new SimpleEdge("likes"));
        mike.createEdge(john, new SimpleEdge2("tolerates"));

        manager.clear();

        try (EdgeQuery edgeQuery = manager.queryEdges(SimpleEdge2.class, "MATCH ()-[r:simple_edge_2]-() RETURN DISTINCT r", Collections.emptyMap())) {
            Assert.assertEquals(1, edgeQuery.list().size());
        }
    }
}
