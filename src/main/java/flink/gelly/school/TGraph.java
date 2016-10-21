package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import javax.xml.crypto.Data;

/**
 * Created by s133781 on 17-Oct-16.
 */
public class TGraph<K,VV,EV,N> {
    /**
     * Creates a graph from two DataSets: vertices and edges
     *
     * @param vertices a DataSet of vertices.
     * @param edges    a DataSet of edges.
     * @param context  the flink execution environment.
     */
    private final ExecutionEnvironment context;
    private final DataSet<Vertex<K, VV>> vertices;
    private final DataSet<Edge<K,Tuple3<EV,N,N>>> edges;

    public TGraph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K,Tuple3<EV,N,N>>> edges, ExecutionEnvironment context) {
        this.vertices = vertices;
        this.edges = edges;
        this.context = context;
    }

//    public static <K,VV,EV,N> TGraph<K,VV,EV,N> from5Tuple(DataSet<Tuple5<K,K,EV,N,N>> edgeSet, ExecutionEnvironment context) throws Exception {
//
//        DataSet<Edge<K,Tuple3<EV,N,N>>> edges = edgeSet.map(new MapFunction<Tuple5<K, K, EV, N, N>, Edge<K,Tuple3<EV,N,N>>>() {
//            @Override
//            public Edge<K,Tuple3<EV,N,N>> map(Tuple5<K, K, EV, N, N> value) throws Exception {
//                return new Edge<K,Tuple3<EV,N,N>>(value.f0, value.f1, new Tuple3<EV,N,N>(value.f2, value.f3, value.f4));
//            }
//        });
////        Graph<K,VV,Tuple3<EV,N,N>> temporalgraph = Graph.fromDataSet(edges,context);
//
//        Graph.fromDataSet(edges,context).getEdges().print();
//        return new TGraph<K, VV, EV, N>(edges,edges,context);

//        DataSet<Vertex<K, NullValue>> vertices = Graph.fromDataSet(edges,context).getVertices();
//// a temporal set created with Flink, now we need to make it into a temporal set into gelly
//
//        DataSet<Tuple5<Long,Long, Double,Integer, Integer>> temporalset = context.readCsvFile("./datasets/testdata")
//                .fieldDelimiter(",")  // node IDs are separated by spaces
//                .ignoreComments("%")  // comments start with "%"
//                .types(Long.class,Long.class,Double.class,Integer.class,Integer.class); // read the node IDs as Longs
//        DataSet<Edge<Long,Tuple3<Double,Integer,Integer>>> edges8 = temporalset.map(new MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, Edge<Long, Tuple3<Double, Integer, Integer>>>() {
//            @Override
//            public Edge<Long, Tuple3<Double, Integer, Integer>> map(Tuple5<Long, Long, Double, Integer, Integer> value) throws Exception {
//                return new Edge<>(value.f0, value.f1, new Tuple3<>(value.f2, value.f3, value.f4));
//            }
//        });
//        Graph<Long, NullValue, Tuple3<Double,Integer,Integer>> temporalgraph4 = Graph.fromDataSet(edges8,context);


//        return new TGraph<K, VV, EV, N>(vertices,edges,context);
//        return null;
//    }
    public TGraph<K,VV,EV,N> from5Tuple(DataSet<Tuple5<K,K,EV,N,N>> TupleSet, ExecutionEnvironment context) throws Exception {
        DataSet<Edge<K,Tuple3<EV,N,N>>> edges = TupleSet.map(new MapFunction<Tuple5<K, K, EV, N, N>, Edge<K, Tuple3<EV, N, N>>>() {
            @Override
            public Edge<K, Tuple3<EV, N, N>> map(Tuple5<K, K, EV, N, N> value) throws Exception {
                return new Edge<K,Tuple3<EV,N,N>>(value.f0, value.f1, new Tuple3<EV,N,N>(value.f2, value.f3, value.f4));
            }
        });
        Graph.fromDataSet(edges,context).getEdges().print();

        return null;
    }

    public int test(int b) {
        return b + 1;
    }
//    public class from5Tuple(DataSet<Tuple5<VV,VV,EV,N,N>> TupleSet, ExecutionEnvironment context) {
////        public from5Tuple(DataSet<Tuple5<Long, Long, Double, Integer, Integer>> temporalset, ExecutionEnvironment env) {
//
//        DataSet<Edge<K,Tuple3<EV,N,N>>> edges = edgeSet.map(new MapFunction<Tuple5<K, K, EV, N, N>, Edge<K,Tuple3<EV,N,N>>>() {
//            @Override
//            public Edge<K,Tuple3<EV,N,N>> map(Tuple5<K, K, EV, N, N> value) throws Exception {
//                return new Edge<K,Tuple3<EV,N,N>>(value.f0, value.f1, new Tuple3<EV,N,N>(value.f2, value.f3, value.f4));
//            }
//        });
////        Graph<K,VV,Tuple3<EV,N,N>> temporalgraph = Graph.fromDataSet(edges,context);
////        }
//    }
}











