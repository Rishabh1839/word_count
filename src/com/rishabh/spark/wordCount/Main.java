package com.rishabh.spark.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;
import java.util.Arrays;

public class Main {

    public static void wordCount(String args) {
        // we start by defining an object for the spark conf class
        // this class is used to set spark parameters as key value pairs for our program
        SparkConf sparkConf = new SparkConf().setMaster("local").setName("word_count");
        // The master specifies local which means this program connects to spark thread
        // it runs on the localhost
        JavaSparkContext sc = new SparkContext(conf);
        // we use RDD to process the file and split the words
        JavaRDD<String> fileInput = sparkContext.textFile(fileName);
        JavaRDD<String> words = fileInput.flatMap(content -> Arrays.asList(content.split(" ")));
        // we use maptopair method to count the words
        // by also providing a word and number pair which can be presented as output
        JavaPairRDD count = words.mapToPair(a -> new Tuple2(a, 1)).reduceByKey((x, y) -> (int) x + (int) y);
        // saves the output file as a text file
        count.saveAsTextFile("dataCount")
    }
    // using our main method for providing the entry point to our program
    public static void main(String[] args){
        if (args.length == 0)
        {
            System.out.println("There are no files provided");
            System.exit(0);
        }
        wordCount(args[0]);
    }
}

