package com.soundcloud.lsh

import com.soundcloud.lsh.SparkImplicits._
import org.apache.spark.ReExports.BoundedPriorityQueue
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

case class SubBucket(bucketHash: Int, subBucketId: Int = -1)

/**
 * Standard Lsh implementation. The queryMatrix is hashed multiple times and exact hash matches are
 * searched for in the dbMatrix. These candidates are used to compute the cosine distance.
 *
 * @param minCosineSimilarity minimum similarity two items need to have
 *                            otherwise they are discarded from the result set
 * @param dimensions          number of random vectors (hyperplanes) to generate bit
 *                            vectors of length d
 * @param numNeighbours       maximum number of catalogItems to be matched for a query when there is
 *                            a matching LSH hash
 * @param maxMatches          maximum number of candidates to return for each query
 * @param rounds              number of hash rounds. The more the better the approximation but
 *                            longer the computation.
 * @param randomReplications  The number of times that catalog set should be replicated in order
 *                            to increase LSH recall.
 * @param bucketSplittingPercentile The percentile of the query bucket size that is used as reference for the maximal
 *                                  bucket size. Any bucket bigger than that will be split in sub-buckets that are smaller.
 *
 */
class QueryLsh(minCosineSimilarity: Double,
               dimensions: Int,
               numNeighbours: Int,
               maxMatches: Int,
               rounds: Int,
               randomReplications: Int = 0,
               bucketSplittingPercentile: Double = 0.95)(sparkSession: SparkSession) extends QueryJoiner with Serializable {

  override def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = queryMatrix.numCols().toInt

    val neighbours = 0 until rounds map {
      i =>
        val randomMatrix = localRandomMatrix(dimensions, numFeatures)
        val querySignatures: RDD[(Int, Signature)] = matrixToBitSet(queryMatrix, randomMatrix).
          keyBy(_.bitSet.hashCode())

        querySignatures.setName(s"querySignatures run $i").cache()

        val bucketSplits: Map[Int, Int] = computeBucketSplits(querySignatures)
        val bucketSplitsBc = sparkSession.sparkContext.broadcast(bucketSplits)


        val splitQuerySignatures: RDD[(SubBucket, Signature)] = splitIntoSubBuckets(querySignatures, bucketSplitsBc)

        val duplicatedCatalogSignatures: RDD[(SubBucket, Signature)] =
          matrixToBitSet(catalogMatrix, randomMatrix).
            andThen(replicateAndRehash(randomReplications, _)).
            andThen(duplicateInSubBuckets(_, bucketSplitsBc))

        joinWithRightBounded(numNeighbours, splitQuerySignatures, duplicatedCatalogSignatures).
          values.
          flatMap {
            case (query, catalog) =>
              val cosine = Cosine(query.vector, catalog.vector)
              if(cosine < minCosineSimilarity)
                None
              else
                Some(MatrixEntry(query.index, catalog.index, cosine))
          }
    }

    val mergedNeighbours = distinctTopK(neighbours.reduce(_ ++ _), maxMatches)

    new CoordinateMatrix(mergedNeighbours)
  }

  private def distinctTopK(rdd: RDD[MatrixEntry], k: Int): RDD[MatrixEntry] = {
    rdd.mapPartitions { partition =>
      // It is assumed that the partitions do not contain duplicates (this is mostly true, but might be false if a query has been split in multiple subbuckets,
      // and a catalog item is replicated in two subbuckets, and the two subbuckets end up in the same partition).
      // As a result, we don't deduplicate before shuffling, and might have less than k unique items per partition in some cases.
      val m = mutable.Map.empty[Long, BoundedPriorityQueue[MatrixEntry]]

      for (entry <- partition) {
        val key = entry.i
        val queue = m.getOrElseUpdate(key, new BoundedPriorityQueue[MatrixEntry](k)(Ordering.by(_.value)))
        queue += entry
      }

      m.toIterator
    }.reduceByKey { case (matches1, matches2) =>
      // This is a pretty naive implementation of distinct+topk. It's likely possible to get better performance by writing a custom
      // priority queue that has no duplicates.
      val distinctMatches: Set[MatrixEntry] = matches1.toSet ++ matches2
      new BoundedPriorityQueue[MatrixEntry](k)(Ordering.by(_.value)) ++= distinctMatches
    }.flatMap(_._2)
  }

  private def duplicateInSubBuckets(signatures: RDD[Signature], bucketSplitsBc: Broadcast[Map[Int, Int]]): RDD[(SubBucket, Signature)] = {
    val newCatalogSignatures = signatures.
      flatMap { signature =>
        val hash = signature.bitSet.hashCode()
        bucketSplitsBc.value.get(hash) match {
          case None => Seq((SubBucket(hash), signature))
          case Some(numSplits) => 0 until numSplits map { i => (SubBucket(hash, i), signature) }
        }
      }
    newCatalogSignatures
  }

  private def splitIntoSubBuckets(querySignatures: RDD[(Int, Signature)], bucketSplitsBc: Broadcast[Map[Int, Int]]): RDD[(SubBucket, Signature)] = {
    querySignatures.mapPartitions { partition =>
      val random = new Random()
      partition.map { case (hash, signature) =>
        bucketSplitsBc.value.get(hash) match {
          case None => (SubBucket(hash), signature)
          case Some(numSplits) => (SubBucket(hash, random.nextInt(numSplits)), signature)
        }
      }
    }
  }

  /*
     * Compute how many times each bucket needs to be split
     *
     * First, compute `bucketSplittingPercentile` percentile of bucket sizes, then determine the bucket splits so that
     * no bucket is bigger than that percentile.
     */
  private def computeBucketSplits(querySignatures: RDD[(Int, Signature)]): Map[Int, Int] = {
    import sparkSession.implicits._

    val bucketSizes: RDD[(Int, Int)] = querySignatures.mapValues(_ => 1).reduceByKey(_ + _).cache

    val Array(maxBucketSize) = bucketSizes.toDF("hash", "queryCount")
      .stat.approxQuantile("queryCount", Array(bucketSplittingPercentile), 0.001)
    println("maximum bucket size: " + maxBucketSize)

    val bucketSplits = bucketSizes.filter(_._2 > maxBucketSize).mapValues(_ / maxBucketSize.toInt + 1).collect().toMap
    bucketSizes.unpersist()
    println("number of buckets to split: " + bucketSplits.size)
    println("buckets to split: " + bucketSplits)
    bucketSplits
  }

  private def joinWithRightBounded[K: ClassTag, V: ClassTag, W: ClassTag]
                                  (bound: Int, rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]): RDD[(K, (V, W))] = {
    rdd1.cogroup(rdd2).flatMapValues( pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator.take(bound)) yield (v, w)
    )
  }


  /*
   * Replicate data into neighboring hashes to increase recall.
   *
   * Each row of the RDD is replicated `replications` times by generating a new row and randomly
   * flipping a bit of the hash to put it into a different neighboring hash bucket.
   */
  private def replicateAndRehash(replications: Int, sigs: RDD[Signature]): RDD[Signature] = {
    if(replications==0) sigs
    else
      sigs.mapPartitions{ partition =>
        val random = new Random()
        partition.flatMap{ sig =>
          val indices = (0 until sig.bitSet.numBits).toSeq
          val selectedIndices = random.shuffle[Int, IndexedSeq](indices).take(replications)
          val perturbedBitSets = selectedIndices.map{ix => val bitSet = BitSet(sig.bitSet); bitSet.flip(ix); bitSet}
          sig +: perturbedBitSets.map(bitSet => sig.copy(bitSet=bitSet))
        }
      }
  }

}
