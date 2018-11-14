package io.opensaber.entity.dao;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = { EntityDao.class, Environment.class})
public class EntityDaoTest {
	
	@Autowired
	private EntityDao aEntityDao;
	private TinkerGraph graph;
	
	@Before
	public void initializeGraph() {
		graph = TinkerGraph.open();
	}
	/**
	 * Creates a Tinkerpop graph and passes to the dao layer for persistence.
	 * @throws Exception
	 */
	@Test
	public void testToCreateGraph() throws Exception{		
		
		//Structure graph 		
		Vertex v1 = graph.addVertex(T.label, "persons", "name", "Persons");
		
        final Vertex v2 = graph.addVertex(T.label, "person", "name", "marko");
        final Vertex v3 = graph.addVertex(T.label, "person", "name", "vadas");
        
        v2.addEdge("type", v1, "name", "type");
        v3.addEdge("type", v1, "name", "type");
        v2.addEdge("friend", v3, "name", "friend");
		
		boolean isCommited = aEntityDao.addGraph(graph);
		System.out.println("is tinker pop graph persist? "+isCommited);
			
	}

}
