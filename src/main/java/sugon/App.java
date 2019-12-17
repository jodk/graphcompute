package sugon;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.noggit.JSONUtil;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws Exception {
        String contactPoint = "10.6.6.32";
        int port = 8182;
        GryoMapper.Builder builder = GryoMapper.build().addRegistry(JanusGraphIoRegistry.getInstance());
        MessageSerializer serializer = new GryoMessageSerializerV3d0(builder);
        //返回结果转换为字符串
        Map<String, Object> config = new HashMap<>();
       //config.put(GryoMessageSerializerV3d0.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
       //((GryoMessageSerializerV3d0) serializer).configure(config, new HashMap<>());

        Cluster cluster = Cluster.build().
                addContactPoint(contactPoint).
                port(port).
                serializer(serializer)
                .create();

        Client connect = cluster.connect(Client.SessionSettings.build().create().getSessionId(), true);
        connect.init();
        try {
            List<String> graphNames = allGraphNames(connect);
            //createTemplateConfiguration(connect);
            //existTemplateConfiguration(connect);
            //createDynamicGraphByTemplateConfiguration(connect, "sugon_person4");
            // createDynamicGraphByTemplateConfiguration(connect,"graph5");
            // openDynamicGraphWithAlias(connect,"graph5","g");
            dropDynamicGraph(connect,"sugon_person2");

            // openDynamicGraphWithAlias(connect,"graph6","g");
            // dropDynamicGraph(connect,"graph6");

        } finally {
            connect.closeAsync();
            cluster.closeAsync();
        }

    }

    /**
     * 动态创建图
     * 使用配置模板创建
     *
     * @param connect
     * @throws Exception
     */
    public static void createDynamicGraphByTemplateConfiguration(Client connect, String graphName) throws Exception {
        String createGraphGremlin = String.format("ConfiguredGraphFactory.create(%s);", decorate(graphName));
        CompletableFuture<ResultSet> resultSetCompletableFuture = connect.submitAsync(createGraphGremlin);
        Iterator<Result> iterator = resultSetCompletableFuture.get().iterator();
        while (iterator.hasNext()) {
            //todo
            Result next = iterator.next();
            System.out.println(next);
        }
    }

    /**
     * 删除动态创建的图
     *
     * @param connect
     * @param graphName
     * @throws Exception
     */
    public static void dropDynamicGraph(Client connect, String graphName) throws Exception {
        String dropGraphGremlin = String.format("ConfiguredGraphFactory.drop(%s);", decorate(graphName));
        CompletableFuture<ResultSet> resultSetCompletableFuture = connect.submitAsync(dropGraphGremlin);
        Iterator<Result> iterator = resultSetCompletableFuture.get().iterator();
        GraphDatabaseConfiguration c = new GraphDatabaseConfiguration(null,null,null,null);
        while (iterator.hasNext()) {
            Result next = iterator.next();
            System.out.println(next);
        }
    }

    /**
     * 使用图名称打开图，并在session下建立别名
     * 别名可以在同一个session下的gremlin语句中使用
     *
     * @param connect
     * @param graphName
     * @return
     * @throws Exception
     */
    public static String openDynamicGraphWithAlias(Client connect, String graphName, String alias) throws Exception {
        Preconditions.checkArgument(null != alias, "alias cannot be empty");
        String openGraphGremlin = alias + " = " + String.format("ConfiguredGraphFactory.open(%s);", decorate(graphName));
        CompletableFuture<ResultSet> resultSetCompletableFuture = connect.submitAsync(openGraphGremlin);
        resultSetCompletableFuture.get().iterator();

        return alias;
    }

    /**
     * 发现图服务下的配置模板
     *
     * @param connect
     * @return
     */
    public static Map<String, Object> existTemplateConfiguration(Client connect) {
        Map<String, Object> map = new HashMap<>();
        try {
            String getTemplateConfigurationGremlin = "ConfiguredGraphFactory.getTemplateConfiguration();";
            CompletableFuture<ResultSet> resultSetCompletableFuture = connect.submitAsync(getTemplateConfigurationGremlin);
            Iterator<Result> iterator = resultSetCompletableFuture.get().iterator();
            while (iterator.hasNext()) {
                Result next = iterator.next();
                Map.Entry<String, Object> entry = (Map.Entry) next.getObject();
                map.put(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            System.out.println("warn:" + e.getMessage());
        }
        System.out.println(JSONUtil.toJSON(map));
        return map;
    }

    /**
     * 创建图的配置模板
     *
     * @param connect
     * @throws Exception
     */
    public static void createTemplateConfiguration(Client connect) throws Exception {
        Map<String, Object> existTemplateConfiguration = existTemplateConfiguration(connect);
        if (!existTemplateConfiguration.isEmpty()) {
            System.out.println("You may only have one template configuration and one exists already.");
            return;
        }
        String createTemplateConfigurationGremlin =
                "Map<String,Object> map = new HashMap<>();" +
                        String.format("map.put(%s,%s);",
                                decorate("storage.backend"), decorate("hbase")) +
                        String.format("map.put(%s,%s);",
                                decorate("storage.hostname"), decorate("10.6.6.32")) +
                        String.format("map.put(%s,%s);",
                                decorate("storage.hbase.ext.zookeeper.znode.parent"), decorate("/hbase-unsecure")) +
                        "ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));";
        System.out.println(createTemplateConfigurationGremlin);

        CompletableFuture<ResultSet> resultSetCompletableFuture = connect.submitAsync(createTemplateConfigurationGremlin);
        System.out.println("创建配置模板返回结果如下：");
        System.out.println("completed :" + resultSetCompletableFuture.isCompletedExceptionally());
        if (!resultSetCompletableFuture.isCompletedExceptionally()) {
            return;
        }
        Iterator<Result> iterator = resultSetCompletableFuture.get().iterator();
        while (iterator.hasNext()) { //action start
            System.out.println(iterator.next().getString());
        }
    }

    /**
     * 将参数字符加双引号返回
     *
     * @param value
     * @return
     */
    private static String decorate(String value) {
        return "\"" + value + "\"";
    }

    /**
     * 动态创建的所有图名称
     *
     * @param connect
     * @throws Exception
     */
    public static List<String> allGraphNames(Client connect) throws Exception {
        List<String> graphNames = new ArrayList<>();
        //获取图数据库下的所有图名称
        CompletableFuture<ResultSet> resultSetCompletableFuture =
                connect.submitAsync("ConfiguredGraphFactory.getGraphNames()");
        Iterator<Result> iterator = resultSetCompletableFuture.get().iterator();
        while (iterator.hasNext()) {
            String graphName = iterator.next().getString();
            System.out.println(graphName);
            graphNames.add(graphName);
        }
        return graphNames;
    }

/**
 public void cluster(){
 Cluster cluster = Cluster.build()
 .addContactPoint("10.6.6.32")
 .port(8182)
 .create();

 DriverRemoteConnection connection = DriverRemoteConnection.using(cluster, "ge");
 GraphTraversalSource g = new GraphTraversalSource(connection);

 System.out.println("v count : " + g.V().count().next());

 g.addV("person").property("name", "zhangdekun").next();
 System.out.println("v count after add : " + g.V().count().next());

 cluster.close();
 }
 **/
}
