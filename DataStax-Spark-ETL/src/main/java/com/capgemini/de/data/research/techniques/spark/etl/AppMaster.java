package com.capgemini.de.data.research.techniques.spark.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class AppMaster {

    public static final String APP_NAME = "cleaningEtl";


    public static void main(final String[] args) {

        final SparkConf conf = new SparkConf().setAppName(APP_NAME);
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> flightsLean = sc.textFile(ParseLine.PATH_LEAN).cache();
        final JavaRDD<String> flightsWide = sc.textFile(ParseLine.PATH_WIDE).cache();

        flightsLean.filter(a -> true).count();
    }
}
