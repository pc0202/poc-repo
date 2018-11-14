package io.opensaber.entity.dao;

import org.apache.tinkerpop.gremlin.structure.Graph;

public interface IEntityDao {
	
	public boolean addGraph(Graph graph) throws Exception;
	public boolean updateGraph(String id, Graph entityToUpdate) throws Exception;
	public Graph getGraphById(String id);
	public boolean deleteGraphById(String id);


}
