package io.opensaber.entity.dao;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.opensaber.datasource.Neo4jGraphDataSource;

@Component
public class GraphFeature {

	@Autowired
	private Environment environment;

	private Graph graph;
	// TO store locally all the type vertices
	private Map<String, Vertex> typeMapVertices;

	private static final String TYPE_VERTEX = "type_vertex";
	private static final String VERTEX_NAME_KEY = "name";

	private static final String VERTEX_TYPE_EXIST_EXCEPTION = "Vertex type already exist";
	
	@PostConstruct
	public void init() {
		try {
			graph = new Neo4jGraphDataSource(environment).getGraphStore();
			enableRwTransaction();
			System.out.println("graph instance created "+graph);
			GraphTraversal<Vertex, Vertex> vTypes = graph.traversal().V().hasLabel(TYPE_VERTEX);
			typeMapVertices = new HashMap<>();
			while (vTypes.hasNext()) {
				Vertex v = vTypes.next();
				String key = v.property(VERTEX_NAME_KEY).value().toString();
				typeMapVertices.put(key, v);
			}
			System.out.println("typeMapVertices created with size "+typeMapVertices.size());

		} catch (Exception e) {
			System.out.println("exception occured post constructor create graph instance "+e);
			e.printStackTrace();
		}

	}


	/**
	 * Create a vertex whose label is type
	 * 
	 * @param vertex
	 * @param typeKey
	 * @return
	 * @throws IllegalAccessException
	 */
	public Vertex createTypeVertex(Vertex vertex, String typeKey) throws IllegalAccessException {

		Vertex typeVertex = null;
		if (vertex.property(typeKey).isPresent()) {
			VertexProperty vertexProperty = vertex.property(typeKey);
			System.out.println("typeMapVertices.containsKey(typeKey) "+typeMapVertices.containsKey(vertexProperty.value()));
			if (typeMapVertices != null && typeMapVertices.containsKey(vertexProperty.value())) {
				throw new IllegalAccessException(VERTEX_TYPE_EXIST_EXCEPTION);
			}
			typeMapVertices.put(typeKey, typeVertex);
			typeVertex = graph.addVertex(T.label, TYPE_VERTEX, VERTEX_NAME_KEY, vertex.label(), vertexProperty.key(),
					vertexProperty.value());


		}
		return typeVertex;
	}

	/**
	 * create node vertex
	 * 
	 * @param sourceVertex
	 * @return
	 */
	public Vertex createNodeVertex(Vertex sourceVertex) {
		Vertex nodeVertex = graph.addVertex(T.label, sourceVertex.label(), VERTEX_NAME_KEY, sourceVertex.label());
		return nodeVertex;
	}

	/**
	 * connect node vertex to type vertex
	 * 
	 * @param vertex
	 * @param type
	 */
	public void addTypeToNode(Vertex nodeVertex, String type) {
		Vertex typeVertex = typeMapVertices.get(type);
		nodeVertex.addEdge(type, typeVertex);
	}

	/**
	 * Create in edges to node vertex.
	 * 
	 * @param node
	 * @param sourceEdges
	 */
	public void addInEdgesForNode(Vertex node, GraphTraversal<Vertex, Vertex> inVertices) {

		while (inVertices.hasNext()) {
			Vertex inV = inVertices.next();
			node.addEdge(inV.label(), inV);
		}
	}
	
	public void persist(){
		graph.tx().commit();
	}
	
	public Graph getGraph(){
		return graph;
	}
	
	private void enableRwTransaction(){
		if (graph.features().graph().supportsTransactions()) {
			org.apache.tinkerpop.gremlin.structure.Transaction tx = graph.tx();
			tx.onReadWrite(org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR.AUTO);
		}
	}


}
