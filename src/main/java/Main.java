import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Created by s133781 on 15-Sep-16.
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        final ExecutionEnvironment env;
//        env = ExecutionEnvironment.getExecutionEnvironment();
//
//        DataSet<Tuple3<Integer,Integer,Integer>> data = env.readCsvFile("./datasets/vertices.csv").types(Integer.class,Integer.class,Integer.class);
//
//        DataSet<Tuple3<Integer,Integer,Integer>> sum = data.sum(2);
////        sum.collect();
//        sum.print();
//        DataSet<Tuple3<String, String, Integer>> data =

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Long>> vertexTuples = env.readCsvFile("./datasets/vertices.csv").types(String.class,Long.class);
        DataSet<Tuple3<String, String, Double>> edgeTuples = env.readCsvFile("./datasets/edges.csv").types(String.class, String.class, Double.class);
        edgeTuples.print();
        Graph<String, Long, Double> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);

//        DataSet<Edge<String, Double>> edges =

        Edge<Long, Double> e = new Edge<>(1l,2l,5d);

        Vertex<Long, String> v = new Vertex<Long, String>(1L, "foo");

        Vertex<Long, String> g = new Vertex<Long, String>(1L, "foo");


//        Graph<String, Long, Double> graph = Graph.fromDataSet(vertices, edges, env);
    }
}
