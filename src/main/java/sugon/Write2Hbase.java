package sugon;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.List;

public class Write2Hbase {
    public static void main(String[] args) throws Exception {
        String path = "D:\\gitproject\\graphcompute\\target\\classes\\conf\\janusgraph-hbase.properties";
        Graph graph = GraphFactory.open(path);
        GraphTraversalSource g = graph.traversal();
        gen(graph);
        g.tx().commit();
        List<Vertex> it = g.V().fold().next();
        for (Vertex i : it) {
            System.out.println(i);
        }

        Object r = g.V().count().next();
        System.out.println(r);
        graph.close();

    }

    public static void gen(Graph graph) {
        final Vertex marko = graph.addVertex(T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = graph.addVertex(T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = graph.addVertex(T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = graph.addVertex(T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = graph.addVertex(T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = graph.addVertex(T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, "weight", 0.5d);
        marko.addEdge("knows", josh, "weight", 1.0d);
        marko.addEdge("created", lop, "weight", 0.4d);
        josh.addEdge("created", ripple, "weight", 1.0d);
        josh.addEdge("created", lop, "weight", 0.4d);
        peter.addEdge("created", lop, "weight", 0.2d);
    }
}
