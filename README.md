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
is set via parameters of the algorithm.

