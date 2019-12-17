package sugon;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.List;

public class OlapWithSparkComputer {
    public static void main(String[] args) throws Exception {
        String path = "D:\\gitproject\\graphcompute\\target\\classes\\conf\\read-hbase-olap.properties";
        Graph graph = GraphFactory.open(path);
        GraphTraversalSource g = graph.traversal().withComputer(SparkGraphComputer.class);
        List<Vertex> it = g.V().fold().next();
        for (Vertex i : it) {
            System.out.println(i);
        }

        Object r = g.V().count().next();
        System.out.println(r);
        graph.close();

    }


}
