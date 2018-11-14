package io.opensaber.datasource;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;


public abstract class DatabaseSource {

    public abstract Graph getGraphStore();
    public abstract  void shutdown() throws Exception;

    /**
     * This method is used for checking database service. It fires a dummy query to check for a non-existent label
     * and checks for the count of the vertices
     * @return
     */
    public boolean isDatabaseServiceUp() {
        boolean databaseStautsUp = false;
        try {
            long count = IteratorUtils.count(getGraphStore().traversal().clone().V().has(T.label, "HealthCheckLabel"));
            if (count >= 0) {
                databaseStautsUp = true;
            }
        } catch (Exception ex) {
            databaseStautsUp = false;
        }
        return databaseStautsUp;
    }

    /**
     * This method is used to initialize some global graph level configuration
     */
    public void initializeGlobalGraphConfiguration() {
        if (IteratorUtils.count(getGraphStore().traversal().V().has(T.label, "graph_global_config")) == 0) {
            if (getGraphStore().features().graph().supportsTransactions()) {
                org.apache.tinkerpop.gremlin.structure.Transaction tx;
                tx = getGraphStore().tx();
                tx.onReadWrite(org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR.AUTO);
                Vertex globalConfig = getGraphStore().traversal().clone().addV( "graph_global_config").next();
                globalConfig.property("persisten_graph", true);
                tx.commit();
            } else {
                Vertex globalConfig = getGraphStore().traversal().clone().addV( "graph_global_config").next();
                globalConfig.property("persisten_graph", true);
            }
        }
    }

}

