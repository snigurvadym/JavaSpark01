import com.google.gson.Gson;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Spark {

    public static void main(String[] args) {
        System.out.println("Hello Spark!");

        // Disable logging loggers;
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create a Java Spark configuration;
        SparkConf sparkConf = new SparkConf();

        // set Spark configuration name;
        sparkConf.setAppName("Hello Spark - process string input data.");

        // set Spark cores usages count [*]-all, [2]-two cores in the local system;
        sparkConf.setMaster("local[*]");

        //create Spark context using spark configuration defined above
        SparkContext sparkContext = new SparkContext(sparkConf);

        //make RDD object using spark context; load input data from the text file; 1 is count of files for results
        JavaRDD<String> lines = sparkContext.textFile("input.txt",1).toJavaRDD();

        // split json data into collection of the objects and add into RDD
        JavaRDD<InfoData> usersRDD = lines.flatMap(new FlatMapFunction<String, InfoData>() {
            @Override
            public Iterator<InfoData> call(String s) throws Exception {
                Pattern p = Pattern.compile("\\{.*?\\}");   // the pattern to search for
                Matcher m = p.matcher(s);
                ArrayList<InfoData> ar = new ArrayList<>();
                Gson gson = new Gson();
                InfoData fooFromJson;
                while (m.find()) {
                    fooFromJson = gson.fromJson(m.group(), InfoData.class);
                    ar.add(fooFromJson);
                }
                return ar.iterator();
            }
        });

        // Transform users RDD using mapping into pair RDD<constructed unique string, user info data>
        JavaPairRDD<String,InfoData> mapToPairTransformation = usersRDD.mapToPair(new PairFunction<InfoData, String, InfoData>() {
            @Override
            public Tuple2<String, InfoData> call(InfoData s) throws Exception {
                return new Tuple2<>(s.getName()+s.getCity(), s);
            }
        });

        // make reduce pair RDD by same string key constructed for reduce according logic
        JavaPairRDD<String, InfoData> reduceByKeyAction = mapToPairTransformation.reduceByKey(new Function2<InfoData,InfoData, InfoData>() {
            @Override
            public InfoData call(InfoData v1, InfoData v2) throws Exception {
                return v1;
            }
        });

        // transform pair RDD into RDD using new map <name key, user info data>
        JavaRDD<String> transformValuesIntoLevel2 = reduceByKeyAction.map(new Function<Tuple2<String, InfoData>, String>() {
            @Override
            public String call(Tuple2<String, InfoData> s) throws Exception {
                return s._2().getName();
            }
        });

        // Save temp data to see intermediate results
         transformValuesIntoLevel2.saveAsTextFile("result-temp"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

        // Transform previously created RDD into pair RDD to count user unique visited city
        JavaPairRDD<String,Integer> mapToPairTransformation2 = transformValuesIntoLevel2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // make pair RDD using reduce by key and sum pair RDD value into result pair RDD
        JavaPairRDD<String, Integer> reduceByKeyAction2 = mapToPairTransformation2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        // make RDD from pair RDD <user> has visited count of different cities for result
        JavaRDD<String> transformValuesIntoReadableResults = reduceByKeyAction2.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return "[" + v1._1()+"] has visited "+v1._2()+" city(ies)";
            }
        });

        // Save constructed result into file in the folder with timestamp
        transformValuesIntoReadableResults.saveAsTextFile("result"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    }
}
