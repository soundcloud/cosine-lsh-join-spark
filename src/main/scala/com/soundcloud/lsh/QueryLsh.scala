package com.soundcloud.lsh

import scala.reflect.ClassTag

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import scala.util.Random

import com.soundcloud.lsh.SparkImplicits._


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
 * @param rounds              number of hash rounds. The more the better the approximation but
 *                            longer the computation.
 * @param flippableBits       A value different than zero enables random bit flipping of hash
 *                            to improve data distribution. The more bits are subject to flipping,
 *                            the better the data distribution (and the faster the job runs).
 *                            However, the recall will drop.
 * @param randomReplications  The number of times that catalog set should be replicated in order
 *                            to increase LSH recall.
 *
 */
class QueryLsh(minCosineSimilarity: Double,
               dimensions: Int,
               numNeighbours: Int,
               rounds: Int,
               flippableBits: Int = 0,
               randomReplications: Int = 0) extends QueryJoiner with Serializable {

  override def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = queryMatrix.numCols().toInt

    val neighbours = 0 until rounds map {
      _ =>
        val randomMatrix = localRandomMatrix(dimensions, numFeatures)
        val querySignatures = matrixToBitSet(queryMatrix, randomMatrix).
          andThen(flipRandomBitUpTo(flippableBits, _)).
          keyBy(_.bitSet.hashCode)
        val catalogSignatures = matrixToBitSet(catalogMatrix, randomMatrix).
          andThen(flipRandomBitUpTo(flippableBits, _)).
          andThen(replicateAndRehash(randomReplications, _)).
          keyBy(_.bitSet.hashCode)
        joinWithRightBounded(numNeighbours, querySignatures, catalogSignatures).
          values.
          map {
            case (query, catalog) =>
              new MatrixEntry(query.index, catalog.index, Cosine(query.vector, catalog.vector))
          }.
          filter(_.value >= minCosineSimilarity)
    }

    val mergedNeighbours = neighbours.reduce(_ ++ _).distinct

    new CoordinateMatrix(mergedNeighbours)
  }

  private def joinWithRightBounded[K: ClassTag, V: ClassTag, W: ClassTag]
                                  (bound: Int, rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]): RDD[(K, (V, W))] = {
    rdd1.cogroup(rdd2).flatMapValues( pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator.take(bound)) yield (v, w)
    )
  }

  /*
   * Randomly flip a bit randomly selected from the prefix of the hash with the
   * prefix size specified by `flipBound`.
   *
   * This function is used to randomly split data sets that fall into the hash in order
   * to improve the distribution of data across Spark partitions. This can speed up runtime
   * significantly.
   *
   */
  private def flipRandomBitUpTo[K, V](flipBound: Int, sigs: RDD[Signature]): RDD[Signature] = {
    if(flipBound==0) sigs
    else
      sigs.mapPartitions{ partition =>
        val random = new Random()
        partition.map{ sig =>
          val ix = random.nextInt(flipBound)
          sig.bitSet.flip(ix)
          sig
        }
      }
  }

  /*
   * Replicate data into neighboring hashes to increase recall.
   *
   * Each row of the RDD is replicated `replications` times by generating a new row and randomly
   * flipping a bit of the hash to put it into a different hash bucket.
   */
  private def replicateAndRehash[K, V](replications: Int, sigs: RDD[Signature]): RDD[Signature] = {
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
