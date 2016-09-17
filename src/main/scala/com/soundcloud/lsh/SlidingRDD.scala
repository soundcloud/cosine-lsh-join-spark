package com.soundcloud.lsh


import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

/**
 * NOTE: both classes are copied from mllib and slightly modified since these classes are mllib private!
 */

class SlidingRDDPartition[T](val idx: Int, val prev: Partition, val tail: Seq[T])
  extends Partition with Serializable {
  override val index: Int = idx
}

/**
 * Represents a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
 * window over them. The ordering is first based on the partition index and then the ordering of
 * items within each partition. This is similar to sliding in Scala collections, except that it
 * becomes an empty RDD if the window size is greater than the total number of items. It needs to
 * trigger a Spark job if the parent RDD has more than one partitions. To make this operation
 * efficient, the number of items per partition should be larger than the window size and the
 * window size should be small, e.g., 2.
 *
 * @param parent the parent RDD
 * @param windowSize the window size, must be greater than 1
 *
 * @see [[org.apache.spark.mllib.rdd.RDDFunctions#sliding]]
 */

class SlidingRDD[T: ClassTag](@transient val parent: RDD[T], val windowSize: Int)
  extends RDD[Seq[T]](parent) {

  require(windowSize > 1, s"Window size must be greater than 1, but got $windowSize.")

  override def compute(split: Partition, context: TaskContext): Iterator[Seq[T]] = {
    val part = split.asInstanceOf[SlidingRDDPartition[T]]
    (firstParent[T].iterator(part.prev, context) ++ part.tail)
      .sliding(windowSize, windowSize)
      .withPartial(true) // modified
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SlidingRDDPartition[T]].prev)

  override def getPartitions: Array[Partition] = {
    val parentPartitions = parent.partitions
    val n = parentPartitions.size
    if (n == 0) {
      Array.empty
    } else if (n == 1) {
      Array(new SlidingRDDPartition[T](0, parentPartitions(0), Seq.empty))
    } else {
      val numPartitionsMinusOne = n - 1
      val windowSizeMinusOne = windowSize - 1
      // Get the first w1 items of each partition, starting from the second partition.
      val nextHeads =
        parent.context.runJob(parent, (iter: Iterator[T]) => iter.take(windowSizeMinusOne).toArray, 1 until n)
      val partitions = mutable.ArrayBuffer[SlidingRDDPartition[T]]()
      var i = 0
      var partitionIndex = 0
      while (i < numPartitionsMinusOne) {
        var j = i
        val tail = mutable.ListBuffer[T]()
        // Keep appending to the current tail until appended a head of size w1.
        while (j < numPartitionsMinusOne && nextHeads(j).size < windowSizeMinusOne) {
          tail ++= nextHeads(j)
          j += 1
        }
        if (j < numPartitionsMinusOne) {
          tail ++= nextHeads(j)
          j += 1
        }
        partitions += new SlidingRDDPartition[T](partitionIndex, parentPartitions(i), tail)
        partitionIndex += 1
        // Skip appended heads.
        i = j
      }
      // If the head of last partition has size w1, we also need to add this partition.
      if (nextHeads.last.size == windowSizeMinusOne) {
        partitions += new SlidingRDDPartition[T](partitionIndex, parentPartitions(numPartitionsMinusOne), Seq.empty)
      }
      partitions.toArray
    }
  }
}
