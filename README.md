# Cosine LSH Join Spark

A spark library for approximate nearest neighbours (ANN).

# Background

In many computational problems such as NLP, Recommendation Systems and Search,
items (e.g. words) are represented as vectors in a multidimensional space.
Then given a specific item it's nearest neighbours need to be find e.g. given
a query find the most similar ones. A naive liner scan over the data set might
be too slow for most data sets.

Hence, more efficient algorithms are needed. One of the most widely used
approaches is Locality Sensitive Hashing (LSH). This family of algorithms are
very fast but might not give the exact solution and are hence called
approximate nearest neighbours (ANN). The trade off between accuracy and speed
is generally set via parameters of the algorithm.

# Algorithm
This is an implementation of the following paper for Spark:

[Randomized Algorithms and NLP: Using Locality Sensitive Hash Function for High Speed Noun Clustering](http://dl.acm.org/citation.cfm?id=1219917)

-- <cite>Ravichandran et al.</cite>

The algorithms performs a so called **ANN-Join** (Approximate Nearest Neighbours Join). That is for every item in the input dataset find the k-nearest neighbours above a certain threshold similarity. 

The algorithm determines a set of candidate items in the first stage and only computes the exact cosine similarity for those candidates. It has been succesfully used in production with typical run times of a couple of minutes for millions of items. 

Note that candidates are ranked by their exact cosine similarity. Hence, this algorithm will not return any false positives (items that the system thinks are nearby but are actually not). Most real world applications require this e.g. in recommendation systems it is ok to return similar items which are almost as good as the exact nearest neighbours but showing false positives would result in senseless recommendations.

#Usage
Input to the algorithm is an IndexedRowMatrix where each row reprents a single item e.g.

	// item_a --> (1.0, 1.0, 1.0)
	// item_b --> (2.0, 2.0, 2.0) 
	// item_c --> (6.0, 3.0, 2.0) 

	val rows = Seq(
      IndexedRow(1, Vectors.dense(1.0, 1.0, 1.0)),
      IndexedRow(2, Vectors.dense(2.0, 2.0, 2.0)),
      IndexedRow(5, Vectors.dense(6.0, 3.0, 2.0))
    )
    val matrix = new IndexedRowMatrix(sc.parallelize(rows))

    val lsh = new Lsh(
      minCosineSimilarity = 0.5,
      dimensions = 2,
      numNeighbours = 3,
      numPermutations = 1,
      partitions = 1,
      storageLevel = StorageLevel.MEMORY_ONLY
    )

    val similariyMatrix = lsh.join(matrix)

    val results = similariyMatrix.entries.map {
      entry =>
        "item:%d item:%d cosine:%.2f".format(entry.i, entry.j, entry.value)
    }

    results.foreach(println)
    
	// above will print:
	// item:2 item:3 cosine:0,87
	// item:1 item:3 cosine:0,87
	// item:1 item:2 cosine:1,00
  
Please see included Main.scala file for a more detailed example.

#Parameters
Please see the original publication for a detailed description of the parameters. 

#Contributors
Özgür Demir

Rany Keddo

