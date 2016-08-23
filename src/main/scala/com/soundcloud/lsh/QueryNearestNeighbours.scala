package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry}

/**
 * Brute force O(size(query) * size(catalog)) method to compute exact nearest neighbours for
 * rows in the query matrix. As this is a very expensive computation, additional sample
 * parameters may be passed such that neighbours are just computed for a random fraction of
 * the data set.
 *
 * @param distance        a function defining a metric over a vector space
 * @param threshold       pairs that are >= to the distance are discarded
 * @param queryFraction   compute for the random fraction of queries
 * @param catalogFraction compute for the random fraction of the catalog
 *
 */
class QueryNearestNeighbours(
                              distance: VectorDistance,
                              threshold: Double,
                              queryFraction: Double,
                              catalogFraction: Double
                            ) extends QueryJoiner with Serializable {

  def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val sampledQueries = queryMatrix.rows.sample(false, queryFraction)
    val sampledCatalog = catalogMatrix.rows.sample(false, catalogFraction)

    val joined = sampledQueries.cartesian(sampledCatalog)

    val neighbours = joined.map { case ((query: IndexedRow), (catalogEntry: IndexedRow)) =>
      new MatrixEntry(query.index, catalogEntry.index, distance(query.vector, catalogEntry.vector))
    }.filter(_.value >= threshold)

    new CoordinateMatrix(neighbours)
  }
}
