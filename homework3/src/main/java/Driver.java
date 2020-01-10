import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Driver {
    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String args[]) {
        SparkConf conf = new SparkConf()
                .setAppName("Page Rank")
                //setMaster("spark://jefferson-city:30380");
        .setMaster("local");
        //.setSparkHome("src/main/resources");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> namesFile = sc.textFile(args[1]);
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] arr = s.split(": ");
            String[] outLinks = new String[0];
            if(arr.length > 1)
                outLinks = arr[1].split(" ");
            Iterable<String> iter = Arrays.asList(outLinks);
            return new Tuple2<> (arr[0],iter);
        });

        JavaPairRDD<String, String> names = namesFile.zipWithIndex().mapToPair(x-> new
                Tuple2<>(Long.toString(x._2()+1),x._1()));

        /*
        JavaPairRDD<String, Double> ranks = links.mapValues(v -> 1.0);

        for (int i = 0; i < 25; i++) {
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });
            ranks = contribs.reduceByKey(new Sum());
        }

        JavaPairRDD<Double,Tuple2<String,String>> finalRanks = names.join(ranks).mapToPair(s->{
            return new Tuple2<>(s._2()._2(),new Tuple2<>(s._2()._1(),s._1() ));
        }).sortByKey(false);
        System.out.println("OUTPUT NON-TAXED");
        List<Tuple2<Double, Tuple2<String, String>>> ranksList = finalRanks.take(10);
        for(Tuple2<Double, Tuple2<String, String>> it : ranksList){
            System.out.println(it);
        }
        */
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        JavaPairRDD<String, Double> ranksTaxed = links.mapValues(v -> 1.0);
        for (int i = 0; i < 25; i++) {
            JavaPairRDD<String, Double> contribs = links.join(ranksTaxed).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });
            ranksTaxed = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        JavaPairRDD<Double,Tuple2<String,String>> finalRanksTaxed = names.join(ranksTaxed).mapToPair(s->{
            return new Tuple2<>(s._2()._2(),new Tuple2<>(s._2()._1(),s._1() ));
        }).sortByKey(false);
        System.out.println("OUTPUT TAXED");
        List<Tuple2<Double, Tuple2<String, String>>> ranksListTaxed = finalRanksTaxed.take(10);
        for(Tuple2<Double, Tuple2<String, String>> it : ranksListTaxed){
            System.out.println(it);
        }


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /*
        JavaPairRDD<String, String> rocky = names.filter(s->s._2().contains("Rocky_Mountain_National_Park"));
        List<Tuple2<String, String>> collectL = rocky.collect();
        Long num = new Long(0);
        for (Tuple2<String,String> tuple : collectL) {
            num = Long.parseLong(tuple._1());
        }

        final Broadcast<Long> rockNum = sc.broadcast(num);

        JavaPairRDD<String, Tuple2<String, Iterable<String>>> subGraph = names
                .filter(s->s._2().contains("surfing") || s._2().contains("Rocky_Mountain_National_Park"))
                .join(links).mapToPair(t->{
                    ArrayList<String> list = new ArrayList<>();
                    for (String item : t._2()._2()){
                        list.add(item);
                    }
                    list.add(rockNum.getValue().toString());
                    return new Tuple2<> (t._1(),new Tuple2<>(t._2()._1, list));
                });

        JavaPairRDD<String, Double> subRanks = subGraph.mapValues(v -> 1.0);
        for (int i = 0; i < 25; i++) {
            JavaPairRDD<String, Double> contribs = subGraph.join(subRanks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1()._2());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1()._2()) {
                            results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            subRanks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }


        JavaPairRDD<Double,Tuple2<String,String>> finalSubRanks = names.join(subRanks).mapToPair(s->{
            return new Tuple2<>(s._2()._2(),new Tuple2<>(s._2()._1(),s._1() ));
        }).sortByKey(false);
        System.out.println("OUTPUT WIKIBOMB");
        List<Tuple2<Double, Tuple2<String, String>>> subRanksList = finalSubRanks.take(10);
        for(Tuple2<Double, Tuple2<String, String>> it : subRanksList){
            System.out.println(it);
        }
        */
    }

}