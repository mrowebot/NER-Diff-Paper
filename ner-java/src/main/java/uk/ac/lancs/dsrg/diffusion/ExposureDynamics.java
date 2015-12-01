package uk.ac.lancs.dsrg.diffusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import uk.ac.lancs.dsrg.SparkCorpus.ConfigLoader;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by rowem on 18/11/15.
 */
public class ExposureDynamics {

    final static Logger logger = Logger.getLogger(ExposureDynamics.class);

    public static void deriveExposurCurvesTopEntities() {
        // get the list of the top-500 entities from HDFS
        String appName = "NER Diffusion - Exposure Distribution Inference - Java";

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

        // run a basic map reduce job over the line lengths of the reddit sample data
        int minPartitions = 30;
//        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated-sample",
//                minPartitions);
//        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/top500_entities.csv",
//                minPartitions);
        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/50-499_entities.csv",
                minPartitions);

        JavaPairRDD<String, TreeMap<Integer, Integer>> entityExposureDistribution =
                lines.mapToPair(new PairFunction<String, String, TreeMap<Integer, Integer>>() {
            public Tuple2<String, TreeMap<Integer, Integer>> call(String entity) {
                logger.info("Processing entity: " + entity);
                TreeMap<Integer, Integer> exposureCounts = new TreeMap<Integer, Integer>();
//                exposureCounts.put(1, 2);
//                exposureCounts.put(2, 1);

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
                } catch(Exception e) {
                    logger.info("Exception raised during loading entity posts: " + e.getMessage());
                    logger.info("Exception entity: " + entity);
                }
//                exposureCounts.put(entityUsers.size(), entityPosts.size());
//                exposureCounts.put(postToUser.size(), dateToPosts.size());

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
                                    logger.info("Matched user to post");
                                    // if the post is by the user then we have our first adoption by the user
                                    if (postToUser.get(post).equals(user)) {
                                        // get how many time the user was exposed to the post
                                        // get the prior users that the user had interacted with before adoption point
                                        logger.info("Getting prior user interactions: " + user);
                                        HashSet<String> priorUsers = ExposureDynamics.getUserPriorInteractions(user, date);

                                        // get the prior authors of posts citing the entity and count if that author is in the prior interactions
                                        int exposureCount = 0;
                                        if (!priorUsers.isEmpty()) {
                                            for (Long priorDate : dateToPosts.keySet()) {
                                                if (priorDate < date) {
                                                    HashSet<String> priorDatePosts = dateToPosts.get(priorDate);
                                                    for (String priorDatePost : priorDatePosts) {
                                                        String priorUser = postToUser.get(priorDatePost);
                                                        if (priorUsers.contains(priorUser))
                                                            exposureCount++;
                                                    }
                                                } else {
                                                    break;
                                                }
                                            }
                                        }

                                        // map the exposure count to the frequency of occurrence
                                        if (exposureCounts.containsKey(exposureCount)) {
                                            exposureCounts.put(exposureCount, exposureCounts.get(exposureCount) + 1);
                                        } else {
                                            exposureCounts.put(exposureCount, 1);
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
                return new Tuple2<String, TreeMap<Integer, Integer>>(entity, exposureCounts);
            }
        });

        // collect the RDD and write the result to a local file - the overall RDD size should not be too large
        Map<String, TreeMap<Integer, Integer>> entityExposureDistributionCollected = entityExposureDistribution.collectAsMap();
        StringBuffer outputString = new StringBuffer();
        for (String entity : entityExposureDistributionCollected.keySet()) {
            outputString.append(entity);
            for (Integer exposureCount : entityExposureDistributionCollected.get(entity).keySet()) {
                outputString.append("\t"+ exposureCount
                        + "," + entityExposureDistributionCollected.get(entity).get(exposureCount));
            }
            outputString.append("\n");
        }

        System.out.println("Finished output");
        System.out.println(outputString);

        // write to a local file
        try {
            PrintWriter writer = new PrintWriter("../NER-Diffusion/data/exposure_curves.tsv", "UTF-8");
            writer.println(outputString);
            writer.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
        logger.info("Finished processing");

    }


    public static HashSet<String> getUserPriorInteractions(String sourceUser, long activationPoint) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.OFF);
        Logger.getLogger("org.apache.hadoop.hbase.zookeeper").setLevel(Level.OFF);
        Logger.getLogger("org.apache.hadoop.hbase.client").setLevel(Level.OFF);
        HashSet<String> priorInteractions = new HashSet<String>();
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_edges"));
            Get get = new Get(Bytes.toBytes(sourceUser));
            Result result = table.get(get); // query the table for the entity row
            // get the cells that are in the row - expensive but gets everything in to per-node memory from hbase
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String targetUserID = Bytes.toString(cell.getQualifier());
                long interactionDate = cell.getTimestamp();
                if (interactionDate < activationPoint) {
                    priorInteractions.add(targetUserID);
                }
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get user interactions for: " + sourceUser);
        }
        return priorInteractions;
    }

    public static HashSet<String> getUserPriorInteractionsInWindow(String sourceUser,
                                                                   long activationPoint,
                                                                   long timeWindow) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "scc-culture-slave3.lancs.ac.uk");
        HashSet<String> priorInteractions = new HashSet<String>();
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_edges"));
            Get get = new Get(Bytes.toBytes(sourceUser));
            Result result = table.get(get); // query the table for the entity row
            long startWindow = activationPoint - timeWindow;
            // get the cells that are in the row - expensive but gets everything in to per-node memory from hbase
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String targetUserID = Bytes.toString(cell.getQualifier());
                long interactionDate = cell.getTimestamp();
                if (interactionDate < activationPoint && interactionDate >= startWindow) {
                    priorInteractions.add(targetUserID);
                }
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get user interactions for: " + sourceUser);
        }
        return priorInteractions;
    }
}
