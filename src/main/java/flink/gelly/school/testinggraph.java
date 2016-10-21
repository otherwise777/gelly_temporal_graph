package flink.gelly.school;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.LabelPropagation;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by s133781 on 10-Oct-16.
 */
public class testinggraph {
    public static void main(String[] args) throws Exception {
        System.out.println("and so the testing begins");


//        test1();
        test8();
    }
    private static void test8() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Step #1:  Load the data in a DataSet
        DataSet<Tuple2<Long, Long>> twitterEdges = env.readCsvFile("./datasets/out.munmun_twitter_social")
                .fieldDelimiter(" ")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class); // read the node IDs as Longs

// Step #2: Create a Graph and initialize vertex values
        Graph<Long, Long, NullValue> graph = Graph.fromTuple2DataSet(twitterEdges, new InitVertices(), env);

//        graph.getEdges().print();
        DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(new LabelPropagation<>(1));
//

        System.out.println(verticesWithCommunity.count());
//        verticesWithCommunity.print();

    }
    private static void test6() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // a temporal set created with Flink, now we need to make it into a temporal set into gelly
        DataSet<Tuple5<Long,Long, Double,Integer, Integer>> temporalset = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,Double.class,Integer.class,Integer.class); // read the node IDs as Longs
        Tgraphv3<Long, NullValue, Double, Integer> testgraph = Tgraphv3.From5TupleNoVertexes(temporalset,env);

        Graph<Long, NullValue, Tuple3<Double, Integer, Integer>> gellygraph = testgraph.getGellyGraph();



//        System.out.println(testgraph.numberOfEdges());

    }
    private static void test7() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // a temporal set created with Flink, now we need to make it into a temporal set into gelly
        DataSet<Tuple5<Long,Long, Double,Integer, Integer>> temporalset = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,Double.class,Integer.class,Integer.class); // read the node IDs as Longs
//        Tgraphv3<Long, Double, Double, Integer> testgraph = Tgraphv3.From5Tuple(temporalset,);




    }
    private static void test5() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // a temporal set created with Flink, now we need to make it into a temporal set into gelly
        DataSet<Tuple5<Long,Long, Double,Integer, Integer>> temporalset = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,Double.class,Integer.class,Integer.class); // read the node IDs as Longs

//        TGraph<Long, NullValue, Long, Integer> temporalNetwork = new TGraph
//        TGraph<Long, NullValue, Long, Integer> test123 = new TGraph<Long, NullValue, Long, Integer>().test(5);
        Tgraphv3<Long, NullValue, Double, Integer> testgraph = Tgraphv3.From5TupleNoVertexes(temporalset,env);

        Graph<Long, NullValue, Tuple3<Double, Integer, Integer>> gellygraph = testgraph.getGellyGraph();
        gellygraph.getEdges().print();
    }
    private static void test4() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // a temporal set created with Flink, now we need to make it into a temporal set into gelly
        DataSet<Tuple4<Long,Long,Integer, Integer>> temporalset = env.readCsvFile("./datasets/testdatatuple4.txt")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,Integer.class,Integer.class); // read the node IDs as Longs


        Tgraphv3<Long, NullValue, NullValue, Integer> testgraph = Tgraphv3.From4TupleNoEdgesNoVertexes(temporalset,env);
        testgraph.getEdges().print();
    }

    private static void test3() throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // a temporal set created with Flink, now we need to make it into a temporal set into gelly
        DataSet<Tuple5<Long,Long, Double,Integer, Integer>> temporalset = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,Double.class,Integer.class,Integer.class); // read the node IDs as Longs

//        TGraph<Long, NullValue, Long, Integer> temporalNetwork = new TGraph
//        TGraph<Long, NullValue, Long, Integer> test123 = new TGraph<Long, NullValue, Long, Integer>().test(5);
        Tgraphv2 test = new Tgraphv2(temporalset,env);

        test.getEdges().print();

        Tgraphv3<Long, NullValue, Double, Integer> testgraph = Tgraphv3.From5TupleNoVertexes(temporalset,env);
        testgraph.getEdges().print();

    }
    private static void test1() {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


//        Create a tuple 3 dataset of edges without temporal data
        DataSet<Tuple3<Long, Long, Double>> testeedges = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")	// node IDs are separated by spaces
                .ignoreComments("%")	// comments start with "%"
                .types(Long.class,Long.class, Double.class);

//        printing the test edges
        System.out.println("the dataset has the following edges:");
//        testeedges.print();

        DataSet<Tuple2<Long,Long>> testvertices = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")	// node IDs are separated by spaces
                .ignoreComments("%")	// comments start with "%"
                .types(Long.class,Long.class);

//        printing the test vertices
        System.out.println("the dataset has the following vertices:");
//        testvertices.print();

//        creating the graph from the edge and vertice sets:
        Graph<Long, NullValue, Double> testgraph = Graph.fromTupleDataSet(testeedges, env);

//        testgraph.getVertices().print();
    }

    private static void test2() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Step #1:  Load the data in a DataSet
        DataSet<Tuple2<Long, Long>> twitterEdges = env.readCsvFile("./datasets/out.munmun_twitter_social")
                .fieldDelimiter(" ")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class); // read the node IDs as Longs

// Step #2: Create a Graph and initialize vertex values
        Graph<Long, Long, NullValue> graph = Graph.fromTuple2DataSet(twitterEdges, new InitVertices(), env);

        Graph<Long, NullValue, NullValue> graph2 = Graph.fromTuple2DataSet(twitterEdges,env);


        List<Edge<Long, String>> edgeList = new ArrayList<Edge<Long,String>>();

        edgeList.add(new Edge<Long, String>(1L,2L, "test"));
        edgeList.add(new Edge<Long, String>(2L,3L, "test2"));
        edgeList.add(new Edge<Long, String>(3L,4L, "test3"));

        // initialize the vertex value to be equal to the vertex ID
        Graph<Long, Long, String> graph3 = Graph.fromCollection(edgeList,
                new MapFunction<Long, Long>() {
                    public Long map(Long value) {
                        return value;
                    }
                }, env);

        List<Edge<Long, Tuple3<Double,Integer,Integer>>> tempedgeList = new ArrayList<>();

        tempedgeList.add(new Edge<>(1L, 2L, new Tuple3<>(1D, 1, 2)));
        tempedgeList.add(new Edge<>(2L, 3L, new Tuple3<>(1D, 2, 3)));
        tempedgeList.add(new Edge<>(1L, 3L, new Tuple3<>(1D, 3, 4)));

        // initialize the vertex value to be equal to the vertex ID
        Graph<Long, Long, Tuple3<Double,Integer,Integer>> graph4 = Graph.fromCollection(tempedgeList,
                new MapFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        return value;
                    }
                }, env);

        List<TemporalEdgev2<Long,Double,Long>> tempedge2 = new ArrayList<>();

        tempedge2.add(new TemporalEdgev2<>(1L,2L,0.5,1L,2L));
        tempedge2.add(new TemporalEdgev2<>(2L,3L,0.5,3L,4L));
        tempedge2.add(new TemporalEdgev2<>(1L,3L,0.5,1L,5L));


        // a temporal set created with Flink, now we need to make it into a temporal set into gelly
        DataSet<Tuple5<Long,Long, Double,Integer, Integer>> temporalset = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,Double.class,Integer.class,Integer.class); // read the node IDs as Longs

        DataSet<TemporalEdgev3<Long,Double>> edgeset3 = temporalset.map(new MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, TemporalEdgev3<Long, Double>>() {
            @Override
            public TemporalEdgev3<Long, Double> map(Tuple5<Long, Long, Double, Integer, Integer> value) throws Exception {

                return new TemporalEdgev3<Long, Double>(value.f0,value.f1,value.f2,value.f3,value.f4);
            }
        });

        DataSet<TemporalEdgev3<Long,Double>> edgeset5 = temporalset.map(new MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, TemporalEdgev3<Long, Double>>() {
            @Override
            public TemporalEdgev3<Long, Double> map(Tuple5<Long, Long, Double, Integer, Integer> value) throws Exception {

                return new TemporalEdgev3<Long, Double>(value.f0,value.f1,value.f2,value.f3,value.f4);
            }
        });

        DataSet<Edge<Long,Tuple3<Double,Integer,Integer>>> edges8 = temporalset.map(new MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, Edge<Long, Tuple3<Double, Integer, Integer>>>() {
            @Override
            public Edge<Long, Tuple3<Double, Integer, Integer>> map(Tuple5<Long, Long, Double, Integer, Integer> value) throws Exception {
                return new Edge<>(value.f0, value.f1, new Tuple3<>(value.f2, value.f3, value.f4));
            }
        });
        Graph<Long, NullValue, Tuple3<Double,Integer,Integer>> temporalgraph4 = Graph.fromDataSet(edges8,env);

        temporalgraph4.getEdges().print();


//        creates a dataset of edges where the edges have 3 values
//        somehow doesnt work, dont get the error code either
        DataSet<Edge<Long,Tuple3<Double,Integer,Integer>>> edgeset4;
        edgeset4 = temporalset.map(new MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, Edge<Long, Tuple3<Double, Integer, Integer>>>() {
            @Override
            public Edge<Long, Tuple3<Double, Integer, Integer>> map(Tuple5<Long, Long, Double, Integer, Integer> value) throws Exception {
                return new Edge<Long, Tuple3<Double, Integer, Integer>>(value.f0, value.f1, new Tuple3<Double, Integer, Integer>(value.f2, value.f3, value.f4));
            }
        });
////        Creates a graph with 3 values in the value field of the edge
//




//        Dataset edgeset 7 created from 5tuple into edge set with edge values as EdgeTime
        DataSet<Edge<Long, EdgeTime<Double,Integer>>> edgeset7 = temporalset.map(new MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, Edge<Long, EdgeTime<Double,Integer>>>() {
            @Override
            public Edge<Long, EdgeTime<Double,Integer>> map(Tuple5<Long, Long, Double, Integer, Integer> value) throws Exception {

                return new Edge<Long, EdgeTime<Double,Integer>>(value.f0, value.f1,new EdgeTime<Double, Integer>(value.f2,value.f3,value.f4));
            }
        });
//        edgeset7 = temporalset.map((MapFunction<Tuple5<Long, Long, Double, Integer, Integer>, Edge<Long, EdgeTime<Double, Integer>>>) value -> new Edge<>(value.f0, value.f1, new EdgeTime<>(value.f2, value.f3, value.f4)));

        Graph<Long, NullValue, EdgeTime<Double,Integer>> temporalgraph = Graph.fromDataSet(edgeset7,env);






//        Early method:
//        Graph<Long, Long, Tuple3<Double,Integer,Integer>> graph4 = Graph.fromCollection(tempedgeList,
//                new MapFunction<Long, Long>() {
//                    public Long map(Long value) {
//                        return value;
//                    }
//                }, env);
//        graph4.getEdges().print();

        Graph<Long,Long,Tuple3<Double,Long,Long>> graph5;


//        graph.getEdges().print();
//        twitterEdges.print();
//        twitterEdges.minBy(0).print();

        DataSet<Vertex<Long,Long>> testevertices = graph.getVertices();


        // Create dataset with edges with values
        DataSet<Tuple3<Long, Long, String>> twitterEdgesWithValues = env.readCsvFile("./datasets/out.munmun_twitter_socialwithvalues")
                .fieldDelimiter(" ")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .types(Long.class,Long.class,String.class); // read the node IDs as Longs





//        temporalset.print();

        Graph<Long, NullValue, String> graphwithvalues = Graph.fromTupleDataSet(twitterEdgesWithValues, env);

        DataSet<Edge<Long,String>> edgeset = graphwithvalues.getEdges();
//        DataSet<TemporalEdgev2<Long,String,Tuple3<String,Long,Long>>> tedgeset = graphwithvalues.getEdges();
//        edgeset.print();
        DataSet<Edge<Long,Tuple3<String,Integer,Integer>>> testset;

        TemporalEdgev2<Long,String,Integer> testedge = new TemporalEdgev2<Long,String,Integer>(1L,2L,"test",3,4);



        Graph<Long, Long, String> testgraph = Graph.fromDataSet(testevertices, edgeset, env);
//        testgraph.getEdges().print();



        /** find the vertex with the maximum degree **/
//        DataSet<Tuple1<Long>> maxInDegreeVertex = graph.getDegrees().maxBy(1).project(0);
//        maxInDegreeVertex.print();

//        DataSet<TemporalEdgev2<Long, NullValue>> edges = graph.getEdges();
//        edges.print();
    }

    /**
     * Initializes the vertex values with the vertex ID
     */
    public static final class InitVertices implements MapFunction<Long, Long> {

        @Override
        public Long map(Long vertexId) {
            return vertexId;
        }
    }
}
