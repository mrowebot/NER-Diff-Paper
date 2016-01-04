package uk.ac.lancs.dsrg.diffusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import uk.ac.lancs.dsrg.HBaseWriter;
import uk.ac.lancs.dsrg.SparkCorpus.ConfigLoader;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by rowem on 18/11/15.
 * Notes: basic implementation of WSDM2012 paper
 */
public class GeneralThresholdAPTest implements ModelSettings {

    final static Logger logger = Logger.getLogger(GeneralThresholdAPTest.class);
    static Configuration config;

    /*
     * Runs the test model based on the probability setting using just action propagations
     * @param   probSetting - takes a value of 1 if the static Bernoulli is to be used, 2 if the discrete time version
     * @param   modelSetting - 1 if just adoptions, 2 if just interactions, 3 if just community-based, 4 if linear combination
     */
    public static void testModel() {
        // get the list of the top-500 entities from HDFS
        String appName = "NER Diffusion - Basic GTAP Diffusion Model - Community Homophily, DT Bernoulli";

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
        lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/test_entities.csv",
                    minPartitions);



        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_results";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("prob_1_m_1");
        columnFamilies.add("prob_1_m_2");
        columnFamilies.add("prob_1_m_3");
        columnFamilies.add("prob_1_m_4");
        columnFamilies.add("prob_2_m_1");
        columnFamilies.add("prob_2_m_2");
        columnFamilies.add("prob_2_m_3");
        columnFamilies.add("prob_2_m_4");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // output: entity to roc values
        logger.info("--- Beginning to process partitions");
        long start = System.currentTimeMillis();
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {

                // get the probability setting from the broadcast variable
                int probSetting = DT_BERNOULLI;
//                int probSetting = STATIC_BERNOULLI;
                // get the model setting from the broadcast variable
                int modelSetting = COMMUNITY_HOMOPHILY;

                while (stringIterator.hasNext()) {
                    String entity = stringIterator.next();

                    // convert the entity into a type that can be queried in the db
                    entity = entity.replace("\"", "").trim().replace(" ", "_");

                    config = HBaseConfiguration.create();
                    config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

                    // perform flags:
                    // 0 = never mentioned entity, 1 = mentioned entity after neighbour, 2 = initiator of entity
                    HashMap<String, Tuple2<Double, Integer>> resultsTable = new HashMap<String, Tuple2<Double, Integer>>();
                    TreeMap<Long, HashSet<String>> dateToPosts = new TreeMap<Long, HashSet<String>>();
                    HashMap<String, String> postToUser = new HashMap<String, String>();

                    logger.info("Loading entity posts: " + entity);
                    logger.info("Prob setting = " + probSetting + " | Model setting = " + modelSetting);
                    try {
                        logger.info("Connecting to HBASE");
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
    //                                    entityUsers.add(userid);
    //                                    entityPosts.add(postid);
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
                    logger.info("Post to user. Size= " + postToUser.size());
                    logger.info("Date to posts. Size = " + dateToPosts.size());

                    // initialise the config object
//                    Configuration config = HBaseConfiguration.create();
//                    config.set("hbase.zookeeper.quorum", "scc-culture-slave3.lancs.ac.uk");

                    double count = 1;
                    double total = dateToPosts.size();
                    for (Long date : dateToPosts.keySet()) {
                        HashSet<String> posts = dateToPosts.get(date);
                        for (String post : posts) {
                            String v = postToUser.get(post);
                            // check if the user is already in the results table
                            if (resultsTable.containsKey(v)) {
                                resultsTable.put(v, new Tuple2<Double, Integer>(0.0, 1));
                            } else {
                                resultsTable.put(v, new Tuple2<Double, Integer>(0.0, 2));
                            }

                            HashSet<String> priorUsers = ExposureDynamics.getUserPriorInteractions(v, date);

                            // THREADED VERSION
                            ExecutorService service = Executors.newFixedThreadPool(10);
                            Collection<ProbCalculationCallable> tasks = new ArrayList<ProbCalculationCallable>();
                            for (String u : priorUsers) {
                                tasks.add(new ProbCalculationCallable(probSetting, modelSetting,
                                        u, v,
                                        date,
                                        resultsTable.containsKey(u),
                                        config));
                            }
                            try {
                                List<Future<HashMap<String,Object>>> results = service.invokeAll(tasks);
                                for (Future<HashMap<String, Object>> result : results) {
                                    HashMap<String, Object> resultMap = result.get();
                                    double p_vu = (Double)resultMap.get("prob");
                                    String uLocal = (String)resultMap.get("u");
                                    if (resultsTable.containsKey(uLocal)) {

                                        // update the probability
                                        double p_u = resultsTable.get(uLocal)._1();
                                        double updatedProb = GeneralThresholdAPTest.updateProb(p_u, p_vu);
                                        // update the results table
                                        resultsTable.put(uLocal, new Tuple2<Double, Integer>(updatedProb,
                                                resultsTable.get(uLocal)._2()));
                                    } else {
                                        resultsTable.put(uLocal, new Tuple2<Double, Integer>(0.0, 0));
                                    }
                                }
                            } catch(final InterruptedException ex) {
                                ex.printStackTrace();
                            } catch(final Exception ex) {
                                ex.printStackTrace();
                            }
                            service.shutdownNow();

                            // NON-THREADED VERSION
//                            for (String u : priorUsers) {
//                                if (resultsTable.containsKey(u)) {
//                                    // update p_u incrementally and add back into the results table
//                                    double p_u = resultsTable.get(u)._1();
//                                    double p_vu = 0;
//                                    // update based on the prob setting
//                                    if(probSetting == STATIC_BERNOULLI) {
//                                        // calculate the influence probability between v and u based on the model setting
//                                        switch (modelSetting) {
//                                            case ACTIONS_ONLY:
//                                                p_vu = GeneralThresholdAPTest.computeActionBernoulliStatic(u, v);
//                                                break;
//                                            case INTERACTIONS_ONLY:
//                                                p_vu = GeneralThresholdAPTest.computeInteractionBernoulliStatic(u, v, date);
//                                                break;
//                                            case COMMUNITY_HOMOPHILY:
//                                                p_vu = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliStatic(u, v, date);
//                                                break;
//                                            case LINEAR_COMBINATION:
//                                                double p_vu_a = GeneralThresholdAPTest.computeActionBernoulliStatic(u, v);
//                                                double p_vu_i = GeneralThresholdAPTest.computeInteractionBernoulliStatic(u, v, date);
//                                                double p_vu_c = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliStatic(u, v, date);
//                                                p_vu = ((1/3) * p_vu_a) + ((1/3) * p_vu_i) + ((1/3) * p_vu_c);
//                                                break;
//                                        }
//                                    } else {
//                                        // calculate the influence probability between v and u based on the model setting
//                                        switch (modelSetting) {
//                                            case ACTIONS_ONLY:
//                                                p_vu = GeneralThresholdAPTest.computeActionBernoulliTau(u, v);
//                                                break;
//                                            case INTERACTIONS_ONLY:
//                                                p_vu = GeneralThresholdAPTest.computeInteractionBernoulliTau(u, v, date);
//                                                break;
//                                            case COMMUNITY_HOMOPHILY:
//                                                p_vu = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliTau(u, v, date);
//                                                break;
//                                            case LINEAR_COMBINATION:
//                                                double p_vu_a = GeneralThresholdAPTest.computeActionBernoulliTau(u, v);
//                                                double p_vu_i = GeneralThresholdAPTest.computeInteractionBernoulliTau(u, v, date);
//                                                double p_vu_c = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliTau(u, v, date);
//                                                p_vu = ((1/3) * p_vu_a) + ((1/3) * p_vu_i) + ((1/3) * p_vu_c);
//                                                break;
//                                        }
//                                    }
//                                    // update the probability
//                                    double updatedProb = GeneralThresholdAPTest.updateProb(p_u, p_vu);
//                                    resultsTable.put(u, new Tuple2<Double, Integer>(updatedProb, resultsTable.get(u)._2()));
//                                } else {
//                                    resultsTable.put(u, new Tuple2<Double, Integer>(0.0, 0));
//                                }
//                            }
                        }

                        // output the progress to the console
                        double progress = (count / total) * 100;
                        DecimalFormat df = new DecimalFormat("#.###");
                        logger.info("Entity Progress for : " + entity + " @ " + df.format(progress) + "%");
                        count++;

                        // for now break so test that everything is being written in as it should be
//                        if (progress > 2)
//                            break;
                    }

                    // generate the puts here for each user within the results table
                    logger.info("ResultsTableSize for Entity: " + entity + ". Size = " + resultsTable);
                    List<Put> puts = new ArrayList<Put>();
                    for (String u : resultsTable.keySet()) {
                        Tuple2<Double, Integer> result = resultsTable.get(u);
                        double probU = result._1();
                        double resultVal = (double)result._2();

                        Put p = new Put(Bytes.toBytes(entity + "-" + u));

                        p.add(Bytes.toBytes("prob_" + probSetting + "_m_" + modelSetting),
                                Bytes.toBytes("prob"),
                                Bytes.toBytes("" + probU));
                        p.add(Bytes.toBytes("prob_" + probSetting + "_m_" + modelSetting),
                                Bytes.toBytes("r_val"),
                                Bytes.toBytes("" + resultVal));
                        puts.add(p);
//                        logger.info("Saving: " + consoleString);
                        logger.info(p.toString());
                    }


                    // Hold off on puts for now to run the code and log how long it takes
                    logger.info("Writing puts into hbase: size = " + puts.size());
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

            }
        });

        logger.info("Finished processing results partitions");
        long duration = System.currentTimeMillis() - start;
        long durationMins = duration / 1000 / 60;
        logger.info("Running the code took (mins): " + durationMins);
    }


    /*
     * Computes the Bernoulli as a static metric from action propagations
     */
    public static double computeActionBernoulliStatic(String u, String v) {
        // compute the bernoulli
        double p = 0;

        // initialise the connection info
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

        // get the av2u entry for these users (key should be v)
        double av2u = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_global_av2u"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.getColumnCells(Bytes.toBytes("v"), Bytes.toBytes(u));
            for (Cell cell : cells) {
                av2u = Double.parseDouble(Bytes.toString(cell.getValue()));
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get av2u_post_tau for: " + v + " to " + u);
        }

        // get the count of how many actions v has performed: key should be v
        double av = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_posts"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.listCells();
            av = (double)cells.size();
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get av for: " + v);
        }

        // do we have influence probabilities? If so, then compute the bernoulli
        if (av2u >0 &&  av > 0) {
            p = av2u / av;
        }

        return p;
    }

    /*
     * Computes the discrete time bernoulli from action propagations
     */
    public static double computeActionBernoulliTau(String u, String v) {
        // compute the bernoulli
        double p = 0;

        // initialise the connection info
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

        // get the av2u entry for these users (key should be v)
        double av2u = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_global_av2u_post_tau"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.getColumnCells(Bytes.toBytes("v"), Bytes.toBytes(u));
            for (Cell cell : cells) {
                av2u = Double.parseDouble(Bytes.toString(cell.getValue()));
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get av2u_post_tau for: " + v + " to " + u);
        }

        // get the count of how many actions v has performed: key should be v
        double av = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_posts"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.listCells();
            av = (double)cells.size();
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get av for: " + v);
        }


        // do we have influence probabilities? If so, then compute the bernoulli
        if (av2u >0 &&  av > 0) {
            p = av2u / av;
        }

        return p;
    }

    /*
     *  Computes the static Bernoulli from interactions
     */
    public static double computeInteractionBernoulliStatic(String u, String v, long t_u) {
        double prob = 0;

        // get the interactions between u and all other users before t_u
        // initialise the connection info
//        Configuration config = HBaseConfiguration.create();
//        config.set("hbase.zookeeper.quorum", "scc-culture-slave3.lancs.ac.uk");

        // get the av2u entry for these users (key should be v)
        double totalInteractionCount = 0;
        double vInteractions = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_edges"));
            Get get = new Get(Bytes.toBytes(u));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                if (cell.getTimestamp() < t_u) {
                    totalInteractionCount++;
                    if(Bytes.toString(cell.getQualifier()).contains(v))
                        vInteractions++;
                }
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get posts for: " + u);
        }

        if(totalInteractionCount > 0 && vInteractions > 0)
            prob = vInteractions / totalInteractionCount;

        // get how many times u has interacted with v before t_u
        return prob;
    }

    /*
    *  Computes the tau-based Bernoulli from interactions
    */
    public static double computeInteractionBernoulliTau(String u, String v, long t_u) {
        double prob = 0;

        // get the interactions between u and all other users before t_u
        // initialise the connection info
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

        // get the tau between the users - if none exists, then return 0 - as we have no prior influence to go on
        double tau = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_taus"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
//            List<Cell> cells = result.getColumnCells(Bytes.toBytes("tau"), Bytes.toBytes(targetUserID));
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                if(Bytes.toString(cell.getQualifier()).contains(u))
                    tau = Double.valueOf(Bytes.toString(cell.getValue())).longValue();
            }
            table.close();
        } catch (Exception e) {
//            logger.info("Couldn't get tau for: " + sourceUserID);
//            logger.info(e.getMessage());
        }

        // get the av2u entry for these users (key should be v)
        double totalInteractionCount = 0;
        double vInteractions = 0;
        if (tau > 0) {
            try {
                // get the interactions between the source and the target and when these occurred
                HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_edges"));
                Get get = new Get(Bytes.toBytes(u));
                Result result = table.get(get); // query the table for the entity row
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    long tInteraction = cell.getTimestamp();
                    if (tInteraction < t_u) {
                        // was the interaction within the user to user influence window?
                        long timeDiff = t_u - tInteraction;
                        if (timeDiff < tau) {
                            totalInteractionCount++;
                            if(Bytes.toString(cell.getQualifier()).contains(v))
                                vInteractions++;
                        }
                    }
                }
                table.close();
            } catch (Exception e) {
                logger.info("Couldn't get interaction edges for: " + u);
            }
        }

        if(totalInteractionCount > 0 && vInteractions > 0)
            prob = vInteractions / totalInteractionCount;

        // get how many times u has interacted with v before t_u
        return prob;
    }

    /*
     * Computes the static Bernoulli from pairwise community comparisons - using basic Cosining
     */
    public static double computeCommunityHomophilyBernoulliStatic(String u, String v, long t_u) {
        // initialise the connection info
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

        // get the communities that each user has posted within prior to t_u
        HashSet<String> communitiesU = new HashSet<String>();
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_posts"));
            Get get = new Get(Bytes.toBytes(u));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.listCells();
//            logger.info(u + " - U Cells Size: " + cells.size() + ", v=" + v);
            for (Cell cell : cells) {
//                logger.info(u + " - Community U from Array = " + Bytes.toString(cell.getQualifierArray()));
//                logger.info(u + " - Community U from val = " + Bytes.toString(cell.getQualifier()));
//                long t_u_formatted = t_u / 1000;
//                long localTS = cell.getTimestamp() / 1000;
//                logger.info("u = " + u  + " post ts = " + localTS + " compared to t_u = " + t_u_formatted);
                if (cell.getTimestamp() < t_u) {
                    // the qualifier is the subreddit id - so get that
                    communitiesU.add(Bytes.toString(cell.getQualifier()));
                }
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get posts for: " + u);
        }

        // and for v
        HashSet<String> communitiesV = new HashSet<String>();
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_posts"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
            List<Cell> cells = result.listCells();
//            logger.info(v + " - V Cells Size: " + cells.size());
            for (Cell cell : cells) {
//                logger.info(v + " - Community V from Array = " + Bytes.toString(cell.getQualifierArray()));
//                logger.info(v + " - Community V from val = " + Bytes.toString(cell.getQualifier()));
//                long localTS = cell.getTimestamp() / 1000;
//                long t_u_formatted = t_u / 1000;
//                System.out.println("v = " + v  + " post ts = " + localTS + " compared to t_u = " + t_u_formatted);
                if (cell.getTimestamp() < t_u) {
                    // the qualifier is the subreddit id - so get that
                    communitiesV.add(Bytes.toString(cell.getQualifier()));
                }
            }
            table.close();
        } catch (Exception e) {
            logger.info("Couldn't get posts for: " + v);
        }

        // get the intersection of the two sets
        double numerator = 0;
        for (String communityU : communitiesU) {
            if(communitiesV.contains(communityU))
                numerator++;
        }

        // get the cardinality of the union of the two sets
        HashSet<String> unionCommunities = communitiesU;
        unionCommunities.addAll(communitiesV);
        double denominator = unionCommunities.size();

        double prob = 0;
        if(numerator > 0 && denominator > 0) {
            prob = numerator / denominator;
//            logger.info("Communities U = " + communitiesU.size() + ", Communities V = " + communitiesV.size());
//            logger.info("Numerator = " + numerator + ", Denominator = " + denominator);
        }
        return prob;
    }

    /*
    * Computes the tau Bernoulli from pairwise community comparisons - using basic Cosining
    */
    public static double computeCommunityHomophilyBernoulliTau(String u, String v, long t_u) {
        // initialise the connection info
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

        double prob = 0;

        // get the tau between the users
        double tau = 0;
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_taus"));
            Get get = new Get(Bytes.toBytes(v));
            Result result = table.get(get); // query the table for the entity row
//            List<Cell> cells = result.getColumnCells(Bytes.toBytes("tau"), Bytes.toBytes(targetUserID));
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                if(Bytes.toString(cell.getQualifier()).contains(u))
                    tau = Double.valueOf(Bytes.toString(cell.getValue())).longValue();
            }
            table.close();
        } catch (Exception e) {
//            logger.info("Couldn't get tau for: " + sourceUserID);
//            logger.info(e.getMessage());
        }

        // only do this if we have a window of influence between the users
        if (tau > 0) {
            // get the communities that each user has posted within prior to t_u
            HashSet<String> communitiesU = new HashSet<String>();
            try {
                // get the interactions between the source and the target and when these occurred
                HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_posts"));
                Get get = new Get(Bytes.toBytes(u));
                Result result = table.get(get); // query the table for the entity row
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    long ts = cell.getTimestamp();
                    if (ts < t_u) {
                        // was the community post within the window of influence
                        long timeDiff = t_u - ts;
                        if(timeDiff < tau)
                            communitiesU.add(Bytes.toString(cell.getQualifier()));

                    }
                }
                table.close();
            } catch (Exception e) {
                logger.info("Couldn't get posts for: " + u);
            }

            // and for v
            HashSet<String> communitiesV = new HashSet<String>();
            try {
                // get the interactions between the source and the target and when these occurred
                HTable table = new HTable(config, TableName.valueOf("ner_diffusion_user_posts"));
                Get get = new Get(Bytes.toBytes(v));
                Result result = table.get(get); // query the table for the entity row
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    long ts = cell.getTimestamp();
                    if (ts < t_u) {
                        // was the community post within the window of influence
                        long timeDiff = t_u - ts;
                        if(timeDiff < tau)
                            communitiesV.add(Bytes.toString(cell.getQualifier()));
                    }
                }
                table.close();
            } catch (Exception e) {
                logger.info("Couldn't get posts for: " + v);
            }


            // get the intersection of the two sets
            double numerator = 0;
            for (String communityU : communitiesU) {
                if(communitiesV.contains(communityU))
                    numerator++;
            }

            // get the cardinality of the union of the two sets
            HashSet<String> unionCommunities = communitiesU;
            unionCommunities.addAll(communitiesV);
            double denominator = unionCommunities.size();


            if(numerator > 0 && denominator > 0)
                prob = numerator / denominator;
        }
        return prob;
    }

    private static double updateProb(Double p_u, double p_vu) {
        double newProb = p_u + ((1 - p_u) * p_vu);
        return newProb;
    }
}
