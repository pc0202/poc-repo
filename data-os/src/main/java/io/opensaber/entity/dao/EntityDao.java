package io.opensaber.entity.dao;

import javax.annotation.PostConstruct;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.opensaber.datasource.Neo4jGraphDataSource;

@Component
public class EntityDao implements IEntityDao {

	@Autowired
	private Environment environment;

	@PostConstruct
	public void init() {
		System.out.println("================== " + environment.getProperty("database.neo4j.database_directory")
				+ "================== ");
		System.out.println(
				"================== " + environment.getProperty("database.neo4j.embedded") + "================== ");

	}

	/**
	 * Adding a graph to a graph source(store).
	 */
	
	public boolean addGraph(Graph entity) throws Exception {

		boolean isCommited = false;
		Graph destinationgraph = new Neo4jGraphDataSource(environment).getGraphStore();
		System.out.println("graphstroe " + destinationgraph);

		Graph sourceGraph = (TinkerGraph) entity;		
		
		try {
			GraphTraversalSource sourceTraversal = sourceGraph.traversal();
			if (destinationgraph.features().graph().supportsTransactions()) {
				org.apache.tinkerpop.gremlin.structure.Transaction tx = destinationgraph.tx();
				tx.onReadWrite(org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR.AUTO);

				//Adding all the vertices from source graph to destination graph.
				addVerticesToGraphSource(destinationgraph, sourceTraversal);
				destinationgraph.tx().commit();

				//adding edges(relations) form sourceTraversal in destination graph
				GraphTraversalSource traversalForGraphStore = destinationgraph.traversal();			
				addEdgesToGraphSource(traversalForGraphStore, sourceTraversal);
				
				destinationgraph.tx().commit();
				isCommited = true;				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isCommited;
	}
	/**
	 * Process: Traversing vertices of given graph and adding vertices to graphSource.
	 * @param graphStore
	 * @param traversalSourceForEntity
	 */
	private void addVerticesToGraphSource(Graph graphStore, GraphTraversalSource traversalSourceForEntity){

		GraphTraversal<Vertex, Vertex> gtV = traversalSourceForEntity.V();
		GraphTraversalSource traversalForGraphStore = graphStore.traversal();
		Vertex cloneVertex = null;
		while (gtV.hasNext()) {
			Vertex v = gtV.next();			
			cloneVertex = copyVertexToGraphSource(graphStore, v);			
		}
		traversalForGraphStore.tx().commit();

	}
	
	//TODO: Move out GraphFeatures for granularity
	private Vertex copyVertexToGraphSource(Graph graphStore, Vertex vertexofEntitySource){
		String key = vertexofEntitySource.keys().iterator().next();
		VertexProperty vp = vertexofEntitySource.property(key);			
		Vertex vertex = graphStore.addVertex(T.label, vertexofEntitySource.label(),
				"label", vertexofEntitySource.label(), vp.key(), vp.value());		
		return vertex;
	}
	

	/**
	 * Traversing the edges of given graph and add the edges to graphSource traversal.
	 * @param traversalForGraphStore
	 * @param traversalSourceForEntity
	 */
	private void addEdgesToGraphSource(GraphTraversalSource traversalForGraphStore, GraphTraversalSource traversalSourceForEntity){
		System.out.println("=====================Traversal Edges==================================");
		GraphTraversal<Edge, Edge> gtE = traversalSourceForEntity.E();
		while (gtE.hasNext()) {
			Edge e = gtE.next();
			String propertyKey = e.keys().iterator().next();

			GraphTraversal<Vertex, Vertex> vc = traversalForGraphStore.V().has(propertyKey,e.inVertex().property(propertyKey).value());
			GraphTraversal<Vertex, Vertex> vd = traversalForGraphStore.V().has(propertyKey,e.outVertex().property(propertyKey).value());

			while (vc.hasNext()) {
				Vertex vi = vc.next();
				while(vd.hasNext()){
					Vertex vo = vd.next();
					vo.addEdge(e.label(), vi, propertyKey, e.label());
				}
			}
		}
	}
	//TOOD: move out to GraphFeatures for granularity
	private void addEdges(Vertex vOut, GraphTraversalSource traversalForGraphStore, GraphTraversalSource traversalSourceForEntity){
		GraphTraversal<Edge, Edge> gtE = traversalSourceForEntity.E();
		while (gtE.hasNext()) {
			Edge e = gtE.next();
			String propertyKey = e.keys().iterator().next();

			GraphTraversal<Vertex, Vertex> vins = traversalForGraphStore.V().has(propertyKey,e.inVertex().property(propertyKey).value());
			while(vins.hasNext()){
				Vertex vi = vins.next();
				vOut.addEdge(e.label(), vi, propertyKey, e.label());

			}
		}
	}
	

	public boolean updateGraph(String id, Graph entityToUpdate) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	public Graph getGraphById(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean deleteGraphById(String id) {
		// TODO Auto-generated method stub
		return false;
	}

}
