Java Code for Tracking Named Entity Diffusion on Reddit
---------

The code is run from the main.java file and contains a pom.xml file that allows you to download and install all necessary libraries via Maven.

Run the sequence of commands (that are commented out) in order in order to:

1. Build HBase tables from the Reddit data to load posts and replies into an indexable-form for quick reading

2. Build the HBase tables covering the training data portion - e.g. entity-propagation counts and influence windows.

3. Run the model testing phase once the tables have been loaded

* * *

All of the above requires the following to be installed on a distributed cluster:
-   [Apache Spark](https://spark.apache.org/)
-   [Apache Hadoop (for HDFS)](https://hadoop.apache.org/)
-   [HBase](https://hbase.apache.org/)

We recommend using [Apache Mesos](http://mesos.apache.org/) for cluster job management together with [Cloudera Manager](https://www.cloudera.com/content/www/en-us/products/cloudera-manager.html).