package com.soundcloud.lsh

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
 * @param rounds              number of hash rounds. The more the better the approximation but
 *                            longer the computation.
 *
 */
class QueryLsh(minCosineSimilarity: Double,
               dimensions: Int,
               rounds: Int) extends QueryJoiner with Serializable {

  override def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = queryMatrix.numCols().toInt

    val neighbours = 0 until rounds map {
      _ =>
        val randomMatrix = localRandomMatrix(dimensions, numFeatures)
        val querySignatures = matrixToBitSetSparse(queryMatrix, randomMatrix)
        val catalogSignatures = matrixToBitSetSparse(catalogMatrix, randomMatrix)
        querySignatures.keyBy(s => bitSetToString(s.bitSet)).
          join(catalogSignatures.keyBy(s => bitSetToString(s.bitSet))).
          values.
          map {
            case (query, catalog) =>
              (query.index, catalog.index)
          }
    }

    val mergedNeighbours = neighbours.reduce(_ ++ _).distinct

    val joined =
      mergedNeighbours.
        keyBy { case (query, catalog) => query }.
        join(queryMatrix.rows.keyBy(_.index)).
        values.
        keyBy { case ((query, catalog), queryVector) => catalog }.
        join(catalogMatrix.rows.keyBy(_.index)).
        values.
        map { case (((query, catalog), queryVector), catalogVector) =>
          new MatrixEntry(query, catalog, Cosine(queryVector.vector, catalogVector.vector))
        }.
        filter(_.value >= minCosineSimilarity)

    new CoordinateMatrix(joined)
  }

  def computeCosine(candidate: (Signature, Signature)): MatrixEntry = {
    candidate match {
      case (query, candidate) =>
        val cosine = Cosine(query.vector, candidate.vector)
        MatrixEntry(query.index, candidate.index, cosine)
    }
  }

}
