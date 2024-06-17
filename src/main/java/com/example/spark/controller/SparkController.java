package com.example.spark.controller;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import scala.Tuple2;

@RestController
@RequestMapping("/spark")
public class SparkController {

	@GetMapping("/count")
	public void printWordsFromFile() {

		// Create Spark Session
		SparkSession spark = SparkSession.builder().appName("Word Count").master("local[*]").getOrCreate(); // Run Spark locally with as many working processors as logical cores on your machine

		// Get Spark Context from Spark Session
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		// Load text file into RDD
		JavaRDD<String> lines = sc.textFile("D:\\projects\\prometheus.yaml");

		// Split each line into words
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		// Map each word to (word, 1)
		JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);

		// Print the word counts
		wordCounts.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));

		// Stop Spark Context
		sc.stop();
	}

}
