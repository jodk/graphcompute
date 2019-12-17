package sugon;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

public class OlapWithSparkComputerRemote {
    public static void main(String[] args) {
        //build connect
        String contactPoint = "10.6.6.32";
        int port = 8182;
        GryoMapper.Builder builder = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
        MessageSerializer serializer = new GryoMessageSerializerV3d0(builder);
        Cluster cluster = Cluster.build()
                .addContactPoint(contactPoint)
                .port(port)
                .serializer(serializer)
                .create();
        //remote driver connection
        DriverRemoteConnection connection = DriverRemoteConnection.using(cluster, "g");
        // remote graph traversal
        Graph instance = EmptyGraph.instance();
        GraphTraversalSource g = instance.traversal().withRemote(connection);

    }
}
