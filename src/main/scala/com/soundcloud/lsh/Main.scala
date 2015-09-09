package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Main {

  def main(args: Array[String]) {

    // init spark context
    val numPartitions = 8
    val input = "data/example.tsv"
    val conf = new SparkConf()
      .setAppName("LSH-Cosine")
      .setMaster("local[4]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    // read in an example data set of word embeddings
    val data = sc.textFile(input, numPartitions).map {
      line =>
        val split = line.split(" ")
        val word = split.head
        val features = split.tail.map(_.toDouble)
        (word, features)
    }


    // create an unique id for each word by zipping with the RDD index
    val indexed = data.zipWithIndex.persist(storageLevel)

    // create indexed row matrix where every row represents one word
    val rows = indexed.map {
      case ((word, features), index) =>
        IndexedRow(index, Vectors.dense(features))
    }

    // store index for later re-mapping (index to word)
    val index = indexed.map {
      case ((word, features), index) =>
        (index, word)
    }.persist(storageLevel)

    // create an input matrix from all rows and run lsh on it
    val matrix = new IndexedRowMatrix(rows)
    val lsh = new Lsh(
      minCosineSimilarity = 0.5,
      dimensions = 20,
      numNeighbours = 200,
      numPermutations = 10,
      partitions = numPartitions,
      storageLevel = storageLevel
    )
    val similarityMatrix = lsh.join(matrix)

    // remap both ids back to words
    val remapFirst = similarityMatrix.entries.keyBy(_.i).join(index).values
    val remapSecond = remapFirst.keyBy { case (entry, word1) => entry.j }.join(index).values.map {
      case ((entry, word1), word2) =>
        (word1, word2, entry.value)
    }

    // group by neighbours to get a list of similar words and then take top k
    val result = remapSecond.groupBy(_._1).map {
      case (word1, similarWords) =>
        // sort by score desc. and take top 10 entries
        val similar = similarWords.toSeq.sortBy(-1 * _._3).take(10).map(_._2).mkString(",")
        s"$word1 --> $similar"
    }

    // print out the results for the first 10 words
    result.take(20).foreach(println)

    sc.stop()

  }
}
