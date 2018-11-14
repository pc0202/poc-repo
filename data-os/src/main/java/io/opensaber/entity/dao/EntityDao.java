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
		Graph graphStore = new Neo4jGraphDataSource(environment).getGraphStore();
		System.out.println("graphstroe " + graphStore);

		Graph graph = (TinkerGraph) entity;
		try {
			GraphTraversalSource traversalSourceForEntity = graph.traversal();
			if (graphStore.features().graph().supportsTransactions()) {
				org.apache.tinkerpop.gremlin.structure.Transaction tx = graphStore.tx();
				tx.onReadWrite(org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR.AUTO);

				addVerticesToGraphSource(graphStore, traversalSourceForEntity);
				GraphTraversalSource traversalForGraphStore = graphStore.traversal();
				addEdgesToGraphSource(traversalForGraphStore, traversalSourceForEntity);			
				graphStore.tx().commit();
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
		System.out.println("=====================Traversal Vertices==================================");
		GraphTraversal<Vertex, Vertex> gtV = traversalSourceForEntity.clone().V();
		while (gtV.hasNext()) {
			Vertex v = gtV.next();
			System.out.println("vertex " + v + " " + v.label());
			// TODO: access multiple properties.
			Vertex v1 = null;
			for (String key : v.keys()) {
				VertexProperty vp = v.property(key);
				System.out.println(vp.key() + ": " + vp.value());
				v1 = graphStore.addVertex(T.label, v.label(), vp.key(), vp.value());

			}
		}
	}
	/**
	 * Traversing the edges of given graph and add the edges to graphSource traversal.
	 * @param traversalForGraphStore
	 * @param traversalSourceForEntity
	 */
	private void addEdgesToGraphSource(GraphTraversalSource traversalForGraphStore, GraphTraversalSource traversalSourceForEntity){
		System.out.println("=====================Traversal Edges==================================");
		GraphTraversal<Edge, Edge> gtE = traversalSourceForEntity.clone().E();
		while (gtE.hasNext()) {
			Edge e = gtE.next();
			System.out.println("Edge traversed label " + e.label());
			System.out.println("Edge traversed keys " + e.keys().size());
			System.out.println(e+" invertex - "+e.inVertex()+" outvertex - "+e.outVertex());
			String propertyKey = null;
			if(e.keys().iterator().hasNext()){
				propertyKey = e.keys().iterator().next();
			}
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
	
/*	public String addGraph(Graph entity) throws Exception {

		Graph graphStore = new Neo4jGraphDataSource(environment).getGraphStore();
		System.out.println("graphstroe " + graphStore);

		Graph graph = (TinkerGraph) entity;
		// graph.
		GraphTraversalSource entityTraversalSource = graph.traversal();
		if (graphStore.features().graph().supportsTransactions()) {
			org.apache.tinkerpop.gremlin.structure.Transaction tx = graphStore.tx();
			tx.onReadWrite(org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR.AUTO);

			//Process: Traversing vertices of entity
			GraphTraversal<Vertex, Vertex> gtV = entityTraversalSource.clone().V();
			while (gtV.hasNext()) {
				Vertex v = gtV.next();
				System.out.println("vertex " + v + " " + v.label());

				// TODO: access multiple properties.
				Vertex v1 = null;
				for (String key : v.keys()) {
					// vproperties.put(key, v.value(key));
					VertexProperty vp = v.property(key);
					System.out.println(vp.key() + ": " + vp.value());
					v1 = graphStore.addVertex(T.label, v.label(), vp.key(), vp.value());

				}
				Iterator<Edge> outEdgeIter = v.edges(Direction.OUT);
				while (outEdgeIter.hasNext()) {
					Edge e = outEdgeIter.next();
					System.out.println("edged out " + e.label() + " - " + e.inVertex().property("name").value());

					GraphTraversal<Vertex, Vertex> vc = graphStore.traversal().V().has("name",
							e.inVertex().property("name").value());// graphStore.vertices(e.inVertex().id()).next();
					System.out.println("vc " + vc);

					while (vc.hasNext()) {
						Vertex vx = vc.next();
						v1.addEdge(e.label(), vx, "name", e.label());
					}

				}

			}
			System.out.println("================================================================================");
			GraphTraversal<Edge, Edge> gtE = entityTraversalSource.clone().E();
			while (gtE.hasNext()) {
				Edge e = gtE.next();
				System.out.println("Edge traversed label " + e.label());
				System.out.println("Edge traversed keys " + e.keys().size());
				System.out.println(e+" invertex - "+e.inVertex()+" outvertex - "+e.outVertex());
				GraphTraversal<Vertex, Vertex> vc = graphStore.traversal().V().has("name",e.inVertex().property("name").value());
				GraphTraversal<Vertex, Vertex> vd = graphStore.traversal().V().has("name",e.outVertex().property("name").value());

				while (vc.hasNext()) {
					Vertex vi = vc.next();
					while(vd.hasNext()){
						Vertex vo = vd.next();

						vi.addEdge(e.label(), vo, "name", e.label());
					}
				}
			}
			//graphStore.tx().commit();

			
			 * Vertex v1 = graphStore.addVertex(T.label, "persons", "type",
			 * "Persons");
			 * 
			 * final Vertex v2 = graphStore.addVertex(T.label, "person", "name",
			 * "marko"); final Vertex v3 = graphStore.addVertex(T.label,
			 * "person", "name", "vadas");
			 * 
			 * final Edge e1 = v2.addEdge("type", v1, "name", "type"); final
			 * Edge e2 = v3.addEdge("type", v1, "name", "type"); final Edge e3 =
			 * v2.addEdge("friend", v3, "name", "friend");
			 

			

		}
		return null;
	}*/

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
