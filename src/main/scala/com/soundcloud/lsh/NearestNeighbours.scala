package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry}

/**
 * Brute force O(n^2) method to compute exact nearest neighbours.
 * As this is a very expensive computation O(n^2) an additional sample parameter may be passed such
 * that neighbours are just computed for a random fraction.
 *
 * @param distance  a function defining a metric over a vector space
 * @param threshold pairs that are >= to the distance are discarded
 * @param fraction  compute neighbours for the given fraction
 *
 */
class NearestNeighbours(
                         distance: VectorDistance,
                         threshold: Double,
                         fraction: Double) extends Joiner with Serializable {

  def join(inputMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val rows = inputMatrix.rows
    val sampledRows = rows.sample(false, fraction)
    sampledRows.cache()

    val joined = sampledRows.cartesian(rows)

    val similarity = joined.map {
      case ((rowA: IndexedRow), (rowB: IndexedRow)) =>
        ((rowA.index, rowB.index), distance(rowA.vector, rowB.vector))
    }

    val neighbours = similarity.filter {
      case ((indexA: Long, indexB: Long), similarity) =>
        similarity >= threshold &&
          indexA < indexB // make upper triangular and remove self similarities
    }

    val resultRows = neighbours.map {
      case ((indexA: Long, indexB: Long), similarity) =>
        MatrixEntry(indexA, indexB, similarity)
    }

    new CoordinateMatrix(resultRows)
  }
}
