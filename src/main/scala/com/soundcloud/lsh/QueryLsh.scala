package com.soundcloud.lsh

import scala.reflect.ClassTag

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD


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
 *
 */
class QueryLsh(minCosineSimilarity: Double,
               dimensions: Int,
               numNeighbours: Int,
               rounds: Int) extends QueryJoiner with Serializable {

  override def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = queryMatrix.numCols().toInt

    val neighbours = 0 until rounds map {
      _ =>
        val randomMatrix = localRandomMatrix(dimensions, numFeatures)
        val querySignatures = matrixToBitSet(queryMatrix, randomMatrix).keyBy(_.bitSet.hashCode)
        val catalogSignatures = matrixToBitSet(catalogMatrix, randomMatrix).keyBy(_.bitSet.hashCode)
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

}
