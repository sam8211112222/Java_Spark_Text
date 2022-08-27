package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        initRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .collect().forEach(System.out::println);
        sc.close();
    }
}
