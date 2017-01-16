package io.woolford;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class Main {

    /**
     *
     */

    static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        new Main().start();
    }

    public void start() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark-hbase-demo");
        sparkConf.setMaster("local[*]"); // comment this out when running via `spark-submit` on a cluster

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Map<String, String> options = new HashMap<String, String>();
        options.put("url","jdbc:mysql://deepthought.woolford.io:3306/davita_demo?user=davita&password=davita");
        options.put("dbtable", "transactions");
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        DataFrame jdbcDF = sqlContext.jdbc(options.get("url"),options.get("dbtable"));

        JavaRDD<Row> jdbcRDD = jdbcDF.javaRDD();

        logger.info(String.valueOf(jdbcDF.count()));

        for (String col : jdbcDF.columns()){
            logger.info(col);
        }

    }

}