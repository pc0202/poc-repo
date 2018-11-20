package io.opensaber.entity.dao;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {GraphFeature.class})
public class GraphFeatureTest {
	
	private TinkerGraph graph;
	
	@Autowired
	private GraphFeature graphFeature;
	
	@Before
	public void initializeGraph() {
		graph = TinkerGraph.open();
	}
	
	@Test @Ignore
	public void testToCreateTypeVertexOfGraphFeature() throws IllegalAccessException{
		Vertex v1 = graph.addVertex(T.label, "teacher", "type", "teacher"); 
		graphFeature.createTypeVertex(v1, "type");
		graphFeature.persist();
		graph.clear();

	}
	
	@Test
	public void testToCreateDuplicateTypeVertexOfGraphFeature(){
		Vertex v1 = graph.addVertex(T.label, "teacher", "type", "teacher"); 
		try {
			graphFeature.createTypeVertex(v1, "type");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		graphFeature.persist();
		graph.clear();

	}
	
	@Test @Ignore
	public void testToCreateNodeVertexOfGraphFeature() throws IllegalAccessException{
		Vertex v1 = graph.addVertex(T.label, "teacher1"); 
		graphFeature.createNodeVertex(v1);
		graphFeature.persist();
		graph.clear();

	}

}
