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
	
	@Test  
	public void testToCreateTypeVertexOfGraphFeature(){
		Vertex v1 = graph.addVertex(T.label, "teacher", "type", "teacher"); 
		try {
			graphFeature.createTypeVertex(v1, "type");
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println("Exception "+e.getMessage());
		}
		graphFeature.persist();
		graph.clear();

	}
	
	@Test @Ignore
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
	@Test @Ignore
	public void testAddToVertex(){
		Vertex nodeVertex = graph.addVertex(T.label, "teacher1"); 
		Vertex node = graphFeature.createNodeVertex(nodeVertex);
		graphFeature.addTypeToNode(node, "teacher");
		graphFeature.persist();
		graph.clear();		
	}
	
/*	@Test @Ignore
	public void testaddInEdgesForNode(){
		Vertex nodeVertex = graph.addVertex(T.label, "teacherA"); 
		Vertex nodeChild = graph.addVertex(T.label, "AttibuteA"); 
		nodeChild.addEdge("type", nodeVertex, "name", "attibute");
		
		Vertex node = graphFeature.createNodeVertex(nodeVertex);
		graphFeature.addTypeToNode(node, "teacher");

		Vertex child = graphFeature.createNodeVertex(nodeChild);
		graphFeature.persist();


		GraphTraversal<Vertex, Vertex> childVertexTr = graphFeature.getGraph().traversal().V(child.id());
		while (childVertexTr.hasNext())
			System.out.println("childVertexTr "+childVertexTr.next());

		graphFeature.addInEdgesForNode(node, childVertexTr);
		
		graphFeature.persist();
		graph.clear();	
	}*/
	
	@Test 
	public void timePerformanceTestAPIs(){
	
		//create node vertex
		Vertex nodeVertex = graph.addVertex(T.label, "teacherA"); 		
	    long startTime = System.currentTimeMillis();

		for(int i=0; i<1000; i++){
			Vertex node = graphFeature.createNodeVertex(nodeVertex);
			graphFeature.addTypeToNode(node, "teacher");
			graphFeature.persist();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("time taken for AddToVertices 1000 time "+(endTime - startTime));

		
	}

}
