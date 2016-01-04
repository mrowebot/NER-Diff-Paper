package uk.ac.lancs.dsrg.diffusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import uk.ac.lancs.dsrg.HBaseWriter;
import uk.ac.lancs.dsrg.SparkCorpus.ConfigLoader;

import java.io.IOException;
import java.util.*;

/**
 * Created by rowem on 18/11/15.
 * Notes: basic implementation of WSDM2012 paper
 */
public class GeneralThresholdAPTrain {
    final static Logger logger = Logger.getLogger(GeneralThresholdAPTrain.class);

    /*
     * Step 1 in the training process is to derive the action tables - save into HDFS the results
     */
    public static void storeAv2uMatrix(int mode) {
        // get the list of the top-500 entities from HDFS
        String appName = "NER Diffusion - Basic GTAP Diffusion Model - Actions Table Computation - Java";

        // Configure Spark Context for submitting jobs
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appName);
        sparkConf = new SparkConf()
                .setMaster(ConfigLoader.getInstance().getValue("mesos"))
                .setAppName(appName);
        sparkConf.set("spark.executor.uri", ConfigLoader.getInstance().getValue("spark.executor.uri"));
        sparkConf.set("spark.mesos.coarse", ConfigLoader.getInstance().getValue("spark.mesos.coarse"));
        sparkConf.set("spark.driver.maxResultSize", "0");
        sparkConf.set("spark.akka.frameSize", "2047");
        sparkConf.set("spark.driver.memory", "10g");
        sparkConf.set("spark.executor.memory", "10g");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        sparkConf.set("spark.cores.max", ConfigLoader.getInstance().getValue("spark.cores.max"));

        // initialise the Spark context based on the configuration
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // get the data to run the job with - depending on the mode of operation
        int minPartitions = 30;
        JavaRDD<String> lines = null;
        if (mode == 1) {
            //  change these lines to the placement of the test and train entities within your HDFS dir
            lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/test_entities.csv",
                minPartitions);
        } else {
            lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/train_entities.csv",
                minPartitions);
        }
        // use the first 80% for training and the remainder for testing

        // stage 1 (train): compute tau
        // stage 1.a)
        // Input: entity
        // Output: {entity : {source, {target, a_v2u_count}}} - per entity A_v2u map
        JavaRDD<Tuple2<String, HashMap<String, Integer>>> entityAv2uRDD =
        lines.flatMap(new FlatMapFunction<String, Tuple2<String, HashMap<String, Integer>>>() {

            // lines.mapToPair(new PairFunction<String, String, HashMap<String, HashMap<String, Integer>>>() {

            public Iterable<Tuple2<String, HashMap<String, Integer>>> call(String entity) {

                // prep the entity
                entity = entity.replace("\"", "").trim().replace(" ", "_");

                // set up the ADT to the returned
                HashMap<String, HashMap<String, Integer>> entityAv2u =
                        new HashMap<String, HashMap<String, Integer>>();
                logger.info("Processing entity: " + entity);

                // Stage 1. get the posts that the entity is cited within, and the users who mention the posts
                logger.info("Connecting to HBASE");
                Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
                HashSet<String> entityUsers = new HashSet<String>();
                HashSet<String> entityPosts = new HashSet<String>();
                HashMap<String, String> postToUser = new HashMap<String, String>();
                TreeMap<Long, HashSet<String>> dateToPosts = new TreeMap<Long, HashSet<String>>();

                logger.info("Loading entity posts");
                try {
                    HTable table = new HTable(config, TableName.valueOf("ner_diffusion_entity_posts"));
                    Get get = new Get(Bytes.toBytes(entity));
                    Result result = table.get(get); // query the table for the entity row
                    // get the cells that are in the row - expensive but gets everything in to per-node memory from hbase
                    List<Cell> cells = result.listCells();
                    for (Cell cell : cells) {
                        long stamp = cell.getTimestamp();
                        String userid = Bytes.toString(cell.getValue());
                        if (!userid.contains("deleted")) {
                            String postid = Bytes.toString(cell.getQualifier());

                            // map the relevant elements
                            entityUsers.add(userid);
                            entityPosts.add(postid);
                            postToUser.put(postid, userid);

                            if (dateToPosts.containsKey(stamp)) {
                                HashSet<String> datePosts = dateToPosts.get(stamp);
                                datePosts.add(postid);
                                dateToPosts.put(stamp, datePosts);
                            } else {
                                HashSet<String> datePosts = new HashSet<String>();
                                datePosts.add(postid);
                                dateToPosts.put(stamp, datePosts);
                            }
                            logger.info("Entering: " + userid + ", " + postid + ", " + stamp);
                        }
                    }
                    table.close();
                } catch (Exception e) {
                    logger.info("Exception raised during loading entity posts: " + e.getMessage());
                    logger.info("Exception entity: " + entity);
                }

                // 3. derive the activation point distribution (exposureCounts) -
                HashSet<String> activatedUsers = new HashSet<String>();
                for (String user : entityUsers) {
//                    logger.info("Processing user: " + user);
                    // get the interactions between the user and other users
                    // iterate through the user's posts
                    for (Long date : dateToPosts.keySet()) {
                        // ensure that the date could be the first time that the user cites the entity in question
                        if (!activatedUsers.contains(user)) {
                            HashSet<String> datePosts = dateToPosts.get(date);
                            for (String post : datePosts) {
                                if (postToUser.containsKey(post)) {
//                                    logger.info("Matched user to post");
                                    // if the post is by the user then we have our first adoption by the user
                                    if (postToUser.get(post).equals(user)) {
                                        // get how many time the user was exposed to the post
                                        // get the prior users that the user had interacted with before adoption point
//                                        logger.info("Getting prior user interactions: " + user);
                                        HashSet<String> priorUsers = ExposureDynamics.getUserPriorInteractions(user, date);

                                        // get the prior authors of posts citing the entity and count if that author is in the prior interactions
                                        int exposureCount = 0;
                                        if (!priorUsers.isEmpty()) {
                                            for (Long priorDate : dateToPosts.keySet()) {
                                                if (priorDate < date) {
                                                    HashSet<String> priorDatePosts = dateToPosts.get(priorDate);
                                                    for (String priorDatePost : priorDatePosts) {
                                                        String priorUser = postToUser.get(priorDatePost);
                                                        if (priorUsers.contains(priorUser)) {
                                                            // log propagation
                                                            if (entityAv2u.containsKey(priorUser)) {
                                                                HashMap<String, Integer> propCounts = entityAv2u.get(priorUser);
                                                                propCounts.put(user, 1);
                                                                entityAv2u.put(priorUser, propCounts);
                                                            } else {
                                                                HashMap<String, Integer> propCounts = new HashMap<String, Integer>();
                                                                propCounts.put(user, 1);
                                                                entityAv2u.put(priorUser, propCounts);
                                                            }
                                                            break;
                                                        }
                                                    }
                                                } else {
                                                    break;
                                                }
                                            }
                                        }

                                        activatedUsers.add(user);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                logger.info("Finished processing: " + entity);
                logger.info("Logged entity Av2u: " + entityAv2u.size());
                // return the tuple form and remove the entity
                List<Tuple2<String, HashMap<String, Integer>>> tuples = new ArrayList<Tuple2<String, HashMap<String, Integer>>>();
                for (String sourceUser : entityAv2u.keySet()) {
                    HashMap<String, Integer> adoptees = entityAv2u.get(sourceUser);
                    tuples.add(new Tuple2<String, HashMap<String, Integer>>(sourceUser, adoptees));
                }
                return tuples;
            }
        });
        logger.info("Derived the entity Av2u values");

        // map the resultant list to a pair RDD and then reduce by key
        JavaPairRDD<String, HashMap<String, Integer>> globalAv2u = entityAv2uRDD.mapToPair(new PairFunction<Tuple2<String, HashMap<String, Integer>>, String, HashMap<String, Integer>>() {
            @Override
            public Tuple2<String, HashMap<String, Integer>> call(Tuple2<String, HashMap<String, Integer>> tuple2) throws Exception {
                return new Tuple2<String, HashMap<String, Integer>>(tuple2._1(), tuple2._2());
            }
        }).reduceByKey(new Function2<HashMap<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>>() {
            public HashMap<String, Integer> call(HashMap<String, Integer> map1, HashMap<String, Integer> map2) {
                HashMap<String, Integer> mergedMap = map1;
                for (String map2key : map2.keySet()) {
                    if (mergedMap.containsKey(map2key)) {
                        mergedMap.put(map2key, mergedMap.get(map2key) + map2.get(map2key));
                    } else {
                        mergedMap.put(map2key, map2.get(map2key));
                    }
                }
                return mergedMap;
            }
        });
        logger.info("Derived the global Av2u values");

        // write to Hbase
        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_global_av2u";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("v");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        logger.info("Writing to HBase");
        globalAv2u.foreachPartition(new VoidFunction<Iterator<Tuple2<String, HashMap<String, Integer>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, HashMap<String, Integer>>> iterator) throws Exception {
                // connect to hbase
                logger.info("Connecting to HBASE");
                //Create config core connecting to hbase
                Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

                try {
                    //Check to see if the config can connect to HBASE
                    HBaseAdmin.checkHBaseAvailable(config);
                } catch (MasterNotRunningException e) {
                    logger.error("HBase is not running.", e);
                    System.exit(1);
                }
                HTable table = new HTable(config, TableName.valueOf(tableName));

                //Iteracte over observations and insert them into HBASE
                List<Put> puts = new ArrayList<Put>();
                while(iterator.hasNext()) {
                    // get the json string
                    Tuple2<String, HashMap<String, Integer>> tuple = iterator.next();
                    try {
                        String sourceUserID = tuple._1();
                        Put p = new Put(Bytes.toBytes(sourceUserID));

                        // get each action prop count
                        for (String targetUserID : tuple._2().keySet()) {
                            int a = tuple._2().get(targetUserID);
                            p.add(Bytes.toBytes("v"),
                                    Bytes.toBytes(targetUserID),
                                    Bytes.toBytes("" + a));
                        }
                        puts.add(p);

                        String consoleString = "Mapping: " + sourceUserID + " to users: " + tuple._2().size();
//                        logger.info("Saving: " + consoleString);
                    } catch(Exception e) {
//                        logger.error(e.getMessage());
                    }
                }

                try {
                    table.put(puts);
                } catch (IOException e) {
                    logger.error(e);
                }
                logger.info("Closing connection to HBASE");

                //Close connection to HBASE for this partitino
                table.flushCommits();
                table.close();
            }
        });

        // print to console first
//        List<Tuple2<String, HashMap<String, Integer>>> tuples = globalAv2u.collect();
//        StringBuffer outputString = new StringBuffer();
//        for (Tuple2<String, HashMap<String, Integer>> tuple : tuples) {
//            outputString.append(tuple._1());
//            for (String target : tuple._2().keySet()) {
//                outputString.append("\t" + target + "," + tuple._2().get(target));
//            }
//            outputString.append("\n");
//        }

        System.out.println("Finished output");
//        System.out.println(outputString);
    }

    public static void computeTaus(int mode) {
        logger.info("Computing taus");

        // get the list of the top-500 entities from HDFS
        String appName = "NER Diffusion - Basic GTAP Diffusion Model - Computing Taus - Java";

        // Configure Spark Context for submitting jobs
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appName);
        sparkConf = new SparkConf()
                .setMaster(ConfigLoader.getInstance().getValue("mesos"))
                .setAppName(appName);
        sparkConf.set("spark.executor.uri", ConfigLoader.getInstance().getValue("spark.executor.uri"));
        sparkConf.set("spark.mesos.coarse", ConfigLoader.getInstance().getValue("spark.mesos.coarse"));
        sparkConf.set("spark.driver.maxResultSize", "0");
        sparkConf.set("spark.akka.frameSize", "2047");
        sparkConf.set("spark.driver.memory", "10g");
        sparkConf.set("spark.executor.memory", "10g");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        sparkConf.set("spark.cores.max", ConfigLoader.getInstance().getValue("spark.cores.max"));

        // initialise the Spark context based on the configuration
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // get the data to run the job with - depending on the mode of operation
        int minPartitions = 30;
        JavaRDD<String> lines = null;
        if (mode == 1) {
            lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/test_entities.csv",
                    minPartitions);
        } else {
            lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/train_entities.csv",
                    minPartitions);
        }

        JavaRDD<Tuple2<String, HashMap<String, Long>>> entityDiffs =
                lines.flatMap(new FlatMapFunction<String, Tuple2<String, HashMap<String, Long>>>() {

                    // lines.mapToPair(new PairFunction<String, String, HashMap<String, HashMap<String, Integer>>>() {
                    public Iterable<Tuple2<String, HashMap<String, Long>>> call(String entity) {

                        // prep the entity
                        entity = entity.replace("\"", "").trim().replace(" ", "_");

                        // set up the ADT to the returned
                        HashMap<String, HashMap<String, Long>> entityDiffs =
                                new HashMap<String, HashMap<String, Long>>();
                        logger.info("Processing entity: " + entity);

                        // Stage 1. get the posts that the entity is cited within, and the users who mention the posts
                        logger.info("Connecting to HBASE");
                        Configuration config = HBaseConfiguration.create();
                        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
                        HashSet<String> entityUsers = new HashSet<String>();
                        HashSet<String> entityPosts = new HashSet<String>();
                        HashMap<String, String> postToUser = new HashMap<String, String>();
                        TreeMap<Long, HashSet<String>> dateToPosts = new TreeMap<Long, HashSet<String>>();

                        logger.info("Loading entity posts");
                        try {
                            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_entity_posts"));
                            Get get = new Get(Bytes.toBytes(entity));
                            Result result = table.get(get); // query the table for the entity row
                            // get the cells that are in the row - expensive but gets everything in to per-node memory from hbase
                            List<Cell> cells = result.listCells();
                            for (Cell cell : cells) {
                                long stamp = cell.getTimestamp();
                                String userid = Bytes.toString(cell.getValue());
                                if (!userid.contains("deleted")) {
                                    String postid = Bytes.toString(cell.getQualifier());

                                    // map the relevant elements
                                    entityUsers.add(userid);
                                    entityPosts.add(postid);
                                    postToUser.put(postid, userid);

                                    if (dateToPosts.containsKey(stamp)) {
                                        HashSet<String> datePosts = dateToPosts.get(stamp);
                                        datePosts.add(postid);
                                        dateToPosts.put(stamp, datePosts);
                                    } else {
                                        HashSet<String> datePosts = new HashSet<String>();
                                        datePosts.add(postid);
                                        dateToPosts.put(stamp, datePosts);
                                    }
                                    logger.info("Entering: " + userid + ", " + postid + ", " + stamp);
                                }
                            }
                            table.close();
                        } catch (Exception e) {
                            logger.info("Exception raised during loading entity posts: " + e.getMessage());
                            logger.info("Exception entity: " + entity);
                        }

                        // 3. derive the activation point distribution (exposureCounts) -
                        HashSet<String> activatedUsers = new HashSet<String>();
                        for (String user : entityUsers) {
//                            logger.info("Processing user: " + user);
                            // get the interactions between the user and other users
                            // iterate through the user's posts
                            for (Long date : dateToPosts.keySet()) {
                                // ensure that the date could be the first time that the user cites the entity in question
                                if (!activatedUsers.contains(user)) {
                                    HashSet<String> datePosts = dateToPosts.get(date);
                                    for (String post : datePosts) {
                                        if (postToUser.containsKey(post)) {
//                                            logger.info("Matched user to post");
                                            // if the post is by the user then we have our first adoption by the user
                                            if (postToUser.get(post).equals(user)) {
                                                // get how many time the user was exposed to the post
                                                // get the prior users that the user had interacted with before adoption point
//                                                logger.info("Getting prior user interactions: " + user);
                                                HashSet<String> priorUsers = ExposureDynamics.getUserPriorInteractions(user, date);

                                                // get the prior authors of posts citing the entity and count if that author is in the prior interactions
                                                if (!priorUsers.isEmpty()) {
                                                    for (Long priorDate : dateToPosts.keySet()) {
                                                        if (priorDate < date) {
                                                            HashSet<String> priorDatePosts = dateToPosts.get(priorDate);
                                                            for (String priorDatePost : priorDatePosts) {
                                                                String priorUser = postToUser.get(priorDatePost);
                                                                if (priorUsers.contains(priorUser)) {
                                                                    // get time delta
                                                                    long timeDelta = date - priorDate;
                                                                    if (entityDiffs.containsKey(priorUser)) {
                                                                        HashMap<String, Long> userDiffs = entityDiffs.get(priorUser);
                                                                        userDiffs.put(user, timeDelta);
                                                                        entityDiffs.put(priorUser, userDiffs);
                                                                    } else {
                                                                        HashMap<String, Long> userDiffs = new HashMap<String, Long>();
                                                                        userDiffs.put(user, timeDelta);
                                                                        entityDiffs.put(priorUser, userDiffs);
                                                                    }
                                                                    break;
                                                                }
                                                            }
                                                        } else {
                                                            break;
                                                        }
                                                    }
                                                }

                                                activatedUsers.add(user);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        logger.info("Finished processing: " + entity);
                        logger.info("Logged entity Av2u: " + entityDiffs.size());
                        // return the tuple form and remove the entity
                        List<Tuple2<String, HashMap<String, Long>>> tuples = new ArrayList<Tuple2<String, HashMap<String, Long>>>();
                        for (String sourceUser : entityDiffs.keySet()) {
                            HashMap<String, Long> adoptees = entityDiffs.get(sourceUser);
                            tuples.add(new Tuple2<String, HashMap<String, Long>>(sourceUser, adoptees));
                        }
                        return tuples;
                    }
                });
        logger.info("Derived the entity diff values");


        // map the resultant list to a pair RDD and then reduce by key
        JavaPairRDD<String, HashMap<String, Long>> globalDiffs = entityDiffs.mapToPair(new PairFunction<Tuple2<String, HashMap<String, Long>>, String, HashMap<String, Long>>() {
            @Override
            public Tuple2<String, HashMap<String, Long>> call(Tuple2<String, HashMap<String, Long>> tuple2) throws Exception {
                return new Tuple2<String, HashMap<String, Long>>(tuple2._1(), tuple2._2());
            }
        }).reduceByKey(new Function2<HashMap<String, Long>, HashMap<String, Long>, HashMap<String, Long>>() {
            public HashMap<String, Long> call(HashMap<String, Long> map1, HashMap<String, Long> map2) {
                HashMap<String, Long> mergedMap = map1;
                for (String map2key : map2.keySet()) {
                    if (mergedMap.containsKey(map2key)) {
                        mergedMap.put(map2key, mergedMap.get(map2key) + map2.get(map2key));
                    } else {
                        mergedMap.put(map2key, map2.get(map2key));
                    }
                }
                return mergedMap;
            }
        });
        logger.info("Derived the global diff values");


        // Compute taus
        logger.info("Computing pairwise tau values - i.e. normalising the diffs");
        // write to Hbase
        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_taus";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("tau");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        logger.info("Writing to HBase");
        globalDiffs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, HashMap<String, Long>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, HashMap<String, Long>>> iterator) throws Exception {
                logger.info("Getting user taus for parition");
                List<Put> puts = new ArrayList<Put>();
                //Iteracte over observations and insert them into HBASE
                while(iterator.hasNext()) {
                    // get the json string
                    Tuple2<String, HashMap<String, Long>> tuple = iterator.next();
//                    logger.info("Computing tau for: " + tuple._1());
                    try {
                        String sourceUserID = tuple._1();
                        // get the row that corresponds to the source id
                        HashMap<String, Integer> av2u = GeneralThresholdAPTrain.getSourceAv2u(sourceUserID);
                        HashMap<String, Long> macroDiffs = tuple._2();
                        // work out the tau
                        HashMap<String, Double> userTaus = new HashMap<String, Double>();
                        for (String targetUserID : macroDiffs.keySet()) {
                            double tau = 0;
//                            logger.info("Macro diff = " + macroDiffs.get(targetUserID));
                            if (av2u.containsKey(targetUserID)) {
//                                logger.info("Av2u = " + av2u.get(targetUserID));
                                tau = (double)macroDiffs.get(targetUserID) / (double)av2u.get(targetUserID);
                            }
                            userTaus.put(targetUserID, tau);
//                            logger.info(sourceUserID + " -> " + targetUserID + ", Tau = " + tau);
                        }

                        Put p = new Put(Bytes.toBytes(sourceUserID));

                        // get each action prop count
                        for (String targetUserID : userTaus.keySet()) {
                            double tau = userTaus.get(targetUserID);
                            p.add(Bytes.toBytes("tau"),
                                    Bytes.toBytes(targetUserID),
                                    Bytes.toBytes("" + tau));
                        }
                        puts.add(p);

                        String consoleString = "Mapping: " + sourceUserID + " to users: " + tuple._2().size();
//                        logger.info("Saving: " + consoleString);
                    } catch(Exception e) {
                        logger.error(e.getMessage());
                    }
                }


                logger.info("Writing partition tau values into HBase");
                //Create config core connecting to hbase
                Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

                try {
                    //Check to see if the config can connect to HBASE
                    HBaseAdmin.checkHBaseAvailable(config);
                } catch (MasterNotRunningException e) {
                    logger.error("HBase is not running.", e);
                    System.exit(1);
                }
                HTable table = new HTable(config, TableName.valueOf(tableName));
                try {
                    table.put(puts);
                } catch (IOException e) {
                    logger.error(e);
                }
                logger.info("Closing connection to HBASE");

                //Close connection to HBASE for this partitino
                table.flushCommits();
                table.close();
            }
        });
        logger.info("Finishing writing taus into HBase");
    }

    private static HashMap<String, Integer> getSourceAv2u(String sourceUser) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
        HashMap<String, Integer> targetAv2us = new HashMap<String, Integer>();
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_global_av2u"));
            Get get = new Get(Bytes.toBytes(sourceUser));
            Result result = table.get(get); // query the table for the entity row
            // get the cells that are in the row - expensive but gets everything in to per-node memory from hbase
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String targetUserID = Bytes.toString(cell.getQualifier());
                int av2uValue = Integer.parseInt(Bytes.toString(cell.getValue()));
                targetAv2us.put(targetUserID, av2uValue);
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get user interactions for: " + sourceUser);
        }
        return targetAv2us;
    }

    public static void storeAv2uMatrixPostTaus(int mode) {

//        logger.setLevel(Level.OFF);

        // get the list of the top-500 entities from HDFS
        String appName = "NER Diffusion - Basic GTAP Diffusion Model - Actions Table Computation Post Tau - Java";

        // Configure Spark Context for submitting jobs
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appName);
        sparkConf = new SparkConf()
                .setMaster(ConfigLoader.getInstance().getValue("mesos"))
                .setAppName(appName);
        sparkConf.set("spark.executor.uri", ConfigLoader.getInstance().getValue("spark.executor.uri"));
        sparkConf.set("spark.mesos.coarse", ConfigLoader.getInstance().getValue("spark.mesos.coarse"));
        sparkConf.set("spark.driver.maxResultSize", "0");
        sparkConf.set("spark.akka.frameSize", "2047");
        sparkConf.set("spark.driver.memory", "10g");
        sparkConf.set("spark.executor.memory", "10g");
        sparkConf.set("spark.shuffle.consolidateFiles", "true");
        sparkConf.set("spark.cores.max", ConfigLoader.getInstance().getValue("spark.cores.max"));

        // initialise the Spark context based on the configuration
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // get the data to run the job with - depending on the mode of operation
        int minPartitions = 30;
        JavaRDD<String> lines = null;
        if (mode == 1) {
            lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/test_entities.csv",
                    minPartitions);
        } else {
            lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/train_entities.csv",
                    minPartitions);
        }
        // use the first 80% for training and the remainder for testing

        // stage 1 (train): compute tau
        // stage 1.a)
        // Input: entity
        // Output: {entity : {source, {target, a_v2u_count}}} - per entity A_v2u map
        JavaRDD<Tuple2<String, HashMap<String, Integer>>> entityAv2uRDD =
                lines.flatMap(new FlatMapFunction<String, Tuple2<String, HashMap<String, Integer>>>() {

                    // lines.mapToPair(new PairFunction<String, String, HashMap<String, HashMap<String, Integer>>>() {

                    public Iterable<Tuple2<String, HashMap<String, Integer>>> call(String entity) {

                        // prep the entity
                        entity = entity.replace("\"", "").trim().replace(" ", "_");

                        // set up the ADT to the returned
                        HashMap<String, HashMap<String, Integer>> entityAv2u =
                                new HashMap<String, HashMap<String, Integer>>();
                        logger.info("Processing entity: " + entity);

                        // Stage 1. get the posts that the entity is cited within, and the users who mention the posts
                        logger.info("Connecting to HBASE");
                        Configuration config = HBaseConfiguration.create();
                        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
                        HashSet<String> entityUsers = new HashSet<String>();
                        HashSet<String> entityPosts = new HashSet<String>();
                        HashMap<String, String> postToUser = new HashMap<String, String>();
                        TreeMap<Long, HashSet<String>> dateToPosts = new TreeMap<Long, HashSet<String>>();

                        logger.info("Loading entity posts");
                        try {
                            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_entity_posts"));
                            Get get = new Get(Bytes.toBytes(entity));
                            Result result = table.get(get); // query the table for the entity row
                            // get the cells that are in the row - expensive but gets everything in to per-node memory from hbase
                            List<Cell> cells = result.listCells();
                            for (Cell cell : cells) {
                                long stamp = cell.getTimestamp();
                                String userid = Bytes.toString(cell.getValue());
                                if (!userid.contains("deleted")) {
                                    String postid = Bytes.toString(cell.getQualifier());

                                    // map the relevant elements
                                    entityUsers.add(userid);
                                    entityPosts.add(postid);
                                    postToUser.put(postid, userid);

                                    if (dateToPosts.containsKey(stamp)) {
                                        HashSet<String> datePosts = dateToPosts.get(stamp);
                                        datePosts.add(postid);
                                        dateToPosts.put(stamp, datePosts);
                                    } else {
                                        HashSet<String> datePosts = new HashSet<String>();
                                        datePosts.add(postid);
                                        dateToPosts.put(stamp, datePosts);
                                    }
//                                    logger.info("Entering: " + userid + ", " + postid + ", " + stamp);
                                }
                            }
                            table.close();
                        } catch (Exception e) {
                            logger.info("Exception raised during loading entity posts: " + e.getMessage());
                            logger.info("Exception entity: " + entity);
                        }

                        // 3. derive the activation point distribution (exposureCounts) -
                        HashSet<String> activatedUsers = new HashSet<String>();
                        for (String user : entityUsers) {
                            logger.info("Processing user: " + user);
                            // get the interactions between the user and other users
                            // iterate through the user's posts
                            for (Long date : dateToPosts.keySet()) {
                                // ensure that the date could be the first time that the user cites the entity in question
                                if (!activatedUsers.contains(user)) {
                                    HashSet<String> datePosts = dateToPosts.get(date);
                                    for (String post : datePosts) {
                                        if (postToUser.containsKey(post)) {
//                                            logger.info("Matched user to post");
                                            // if the post is by the user then we have our first adoption by the user
                                            if (postToUser.get(post).equals(user)) {
                                                // get how many time the user was exposed to the post
                                                // get the prior users that the user had interacted with before adoption point
//                                                logger.info("Getting prior user interactions: " + user);
                                                HashSet<String> priorUsers = ExposureDynamics.getUserPriorInteractions(user, date);

                                                // get the prior authors of posts citing the entity and count if that author is in the prior interactions
                                                if (!priorUsers.isEmpty()) {
                                                    for (Long priorDate : dateToPosts.keySet()) {
                                                        if (priorDate < date) {
                                                            HashSet<String> priorDatePosts = dateToPosts.get(priorDate);
                                                            for (String priorDatePost : priorDatePosts) {
                                                                String priorUser = postToUser.get(priorDatePost);
                                                                if (priorUsers.contains(priorUser)) {

                                                                    long timeDiff = date - priorDate;

                                                                    // check that the action takes place within the tau connecting the users
                                                                    boolean withinTau = GeneralThresholdAPTrain.checkTauWindow(priorUser, user, timeDiff);
                                                                    if(withinTau) {
                                                                        // log propagation
                                                                        if (entityAv2u.containsKey(priorUser)) {
                                                                            HashMap<String, Integer> propCounts = entityAv2u.get(priorUser);
                                                                            propCounts.put(user, 1);
                                                                            entityAv2u.put(priorUser, propCounts);
                                                                        } else {
                                                                            HashMap<String, Integer> propCounts = new HashMap<String, Integer>();
                                                                            propCounts.put(user, 1);
                                                                            entityAv2u.put(priorUser, propCounts);
                                                                        }
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            break;
                                                        }
                                                    }
                                                }

                                                activatedUsers.add(user);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        logger.info("Finished processing: " + entity);
                        logger.info("Logged entity Av2u: " + entityAv2u.size());
                        // return the tuple form and remove the entity
                        List<Tuple2<String, HashMap<String, Integer>>> tuples = new ArrayList<Tuple2<String, HashMap<String, Integer>>>();
                        for (String sourceUser : entityAv2u.keySet()) {
                            HashMap<String, Integer> adoptees = entityAv2u.get(sourceUser);
                            tuples.add(new Tuple2<String, HashMap<String, Integer>>(sourceUser, adoptees));
                        }
                        return tuples;
                    }
                });
        logger.info("Derived the entity Av2u values");

        // map the resultant list to a pair RDD and then reduce by key
        JavaPairRDD<String, HashMap<String, Integer>> globalAv2u = entityAv2uRDD.mapToPair(new PairFunction<Tuple2<String, HashMap<String, Integer>>, String, HashMap<String, Integer>>() {
            @Override
            public Tuple2<String, HashMap<String, Integer>> call(Tuple2<String, HashMap<String, Integer>> tuple2) throws Exception {
                return new Tuple2<String, HashMap<String, Integer>>(tuple2._1(), tuple2._2());
            }
        }).reduceByKey(new Function2<HashMap<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>>() {
            public HashMap<String, Integer> call(HashMap<String, Integer> map1, HashMap<String, Integer> map2) {
                HashMap<String, Integer> mergedMap = map1;
                for (String map2key : map2.keySet()) {
                    if (mergedMap.containsKey(map2key)) {
                        mergedMap.put(map2key, mergedMap.get(map2key) + map2.get(map2key));
                    } else {
                        mergedMap.put(map2key, map2.get(map2key));
                    }
                }
                return mergedMap;
            }
        });
        logger.info("Derived the global Av2u values post tau");

        // write to Hbase
        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_global_av2u_post_tau";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("v");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        logger.info("Writing to HBase");
        globalAv2u.foreachPartition(new VoidFunction<Iterator<Tuple2<String, HashMap<String, Integer>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, HashMap<String, Integer>>> iterator) throws Exception {
                // connect to hbase
                logger.info("Connecting to HBASE");
                //Create config core connecting to hbase
                Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

                try {
                    //Check to see if the config can connect to HBASE
                    HBaseAdmin.checkHBaseAvailable(config);
                } catch (MasterNotRunningException e) {
                    logger.error("HBase is not running.", e);
                    System.exit(1);
                }
                HTable table = new HTable(config, TableName.valueOf(tableName));

                //Iteracte over observations and insert them into HBASE
                List<Put> puts = new ArrayList<Put>();
                while(iterator.hasNext()) {
                    // get the json string
                    Tuple2<String, HashMap<String, Integer>> tuple = iterator.next();
                    try {
                        String sourceUserID = tuple._1();
                        Put p = new Put(Bytes.toBytes(sourceUserID));

                        // get each action prop count
                        for (String targetUserID : tuple._2().keySet()) {
                            int a = tuple._2().get(targetUserID);
                            p.add(Bytes.toBytes("v"),
                                    Bytes.toBytes(targetUserID),
                                    Bytes.toBytes("" + a));
                        }
                        puts.add(p);

                        String consoleString = "Mapping: " + sourceUserID + " to users: " + tuple._2().size();
                        logger.info("Saving: " + consoleString);
                    } catch(Exception e) {
                        logger.error(e.getMessage());
                    }
                }

                try {
                    table.put(puts);
                } catch (IOException e) {
                    logger.error(e);
                }
                logger.info("Closing connection to HBASE");

                //Close connection to HBASE for this partitino
                table.flushCommits();
                table.close();
            }
        });

        // print to console first
//        List<Tuple2<String, HashMap<String, Integer>>> tuples = globalAv2u.collect();
//        StringBuffer outputString = new StringBuffer();
//        for (Tuple2<String, HashMap<String, Integer>> tuple : tuples) {
//            outputString.append(tuple._1());
//            for (String target : tuple._2().keySet()) {
//                outputString.append("\t" + target + "," + tuple._2().get(target));
//            }
//            outputString.append("\n");
//        }

        System.out.println("Finished output");
//        System.out.println(outputString);
    }

    private static boolean checkTauWindow(String sourceUserID, String targetUserID, long timeDiff) {
        // get the tau entry for the user
        long tau = 0;
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_taus"));
            Get get = new Get(Bytes.toBytes(sourceUserID));
            Result result = table.get(get); // query the table for the entity row
//            List<Cell> cells = result.getColumnCells(Bytes.toBytes("tau"), Bytes.toBytes(targetUserID));
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                if(Bytes.toString(cell.getQualifier()).contains(targetUserID))
                    tau = Double.valueOf(Bytes.toString(cell.getValue())).longValue();
//                    tau = Long.parseLong(Bytes.toString(cell.getValue()));
            }
            table.close();
//            logger.info(sourceUserID + " -> " + targetUserID + " with diff " + timeDiff + " has tau= " + tau);
        } catch (Exception e) {
//            logger.info("Couldn't get tau for: " + sourceUserID);
//            logger.info(e.getMessage());
        }

        // check that the timediff is within the tau window
        if (timeDiff <= tau) {
//            logger.info("time diff in tau");
            return true;
        } else {
            return false;
        }
    }
}
