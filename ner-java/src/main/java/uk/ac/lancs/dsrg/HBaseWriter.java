package uk.ac.lancs.dsrg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import uk.ac.lancs.dsrg.SparkCorpus.ConfigLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rowem on 16/11/15.
 */
public class HBaseWriter {

    final static Logger logger = Logger.getLogger(HBaseWriter.class);
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void buildTable(List<String> columnFamilies, String name) {
        try {
            //Load admin connection to HBASe
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));

            try {
                //Check to see if the config can connect to HBASE
                HBaseAdmin.checkHBaseAvailable(config);
            } catch (Exception e) {
                logger.error("HBase is not running.", e);
                System.exit(1);
            }
            HBaseAdmin hba = new HBaseAdmin(config);

            //Check to see if the table exsists
            if (hba.tableExists(name) == false) {

                //If not build new table
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(name));
                RegionSplitter.SplitAlgorithm algo = new RegionSplitter.UniformSplit();
                byte[][] splits = algo.split(12);
//                hba.disableTable(name);

                //Build columns
                for (String family : columnFamilies) {
                    logger.info("Building column family");
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                    columnDescriptor.setMaxVersions(365*10);
                    tableDescriptor.addFamily(columnDescriptor);
                }
//                hba.enableTable(name);

                //create table
                hba.createTable(tableDescriptor, splits);
            }

            //Close connection
            hba.close();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public static void writePostDetails() {
        String appName = "NER Diffusion - Posts Writer - Java";

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
//        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample",
//                minPartitions);
        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json",
                minPartitions);
        final ObjectMapper mapper = new ObjectMapper();

        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_post_details";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("details");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
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
                while(stringIterator.hasNext()) {
                    // get the json string
                    String s = stringIterator.next();
                    try {
                        // parse the json and pull out the parent_id
                        JsonNode node = mapper.readValue(s, JsonNode.class);

                        // get post details
                        String postID = node.get("name").asText();
                        Long createdLong = Long.parseLong(node.get("created_utc").asText()) * 1000;
                        String userID = node.get("author").asText();
                        String subredditID = node.get("subreddit_id").asText();

                        String consoleString = postID + " @ " + createdLong + " by " + userID + " in " + subredditID;

                        // format the input into the hbase table
                        // set the row id as the source post id
                        Put p = new Put(Bytes.toBytes(postID));

                        // details:user:user_id
                        p.add(Bytes.toBytes("details"),
                                Bytes.toBytes("user"),
                                createdLong,
                                Bytes.toBytes(userID));
                        // details:community:subreddit_id
                        p.add(Bytes.toBytes("details"),
                                Bytes.toBytes("c"),
                                createdLong,
                                Bytes.toBytes(subredditID));
                        puts.add(p);
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
//                    Put i = new Put(Bytes.toBytes(o.getDimention(Dimension.Y).getString()),
//                            ((uk.ac.lancs.dsrg.SparkCorpus.Models.Date) o.getDimention(Dimension.X).getObject()).getDateTime().getMillis());
//                    i.add(Bytes.toBytes("com" + ((Community) o.getDimention(Dimension.Z).getObject()).getLevel()),
//                            Bytes.toBytes(o.getDimention(Dimension.Z).getString()),
//                            ((uk.ac.lancs.dsrg.SparkCorpus.Models.Date) o.getDimention(Dimension.X).getObject()).getDateTime().getMillis(),
//                            Bytes.toBytes(o.getObservation().toString()));
//                    puts.add(i);
            }
        });

        logger.info("Finished Writing Data to HBase");

    }

    public static void writeUserPosts() {
        String appName = "NER Diffusion - User Posts Writer - Java";

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
//        sparkConf.set("spark.cores.max", ConfigLoader.getInstance().getValue("spark.cores.max"));
        sparkConf.set("spark.cores.max", "10");

        // initialise the Spark context based on the configuration
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // run a basic map reduce job over the line lengths of the reddit sample data
        int minPartitions = 30;
//        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample",
//                minPartitions);
        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json",
                minPartitions);
        final ObjectMapper mapper = new ObjectMapper();

        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_user_posts";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("posts");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
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
                while(stringIterator.hasNext()) {
                    // get the json string
                    String s = stringIterator.next();
                    try {
                        // parse the json and pull out the parent_id
                        JsonNode node = mapper.readValue(s, JsonNode.class);

                        // get post details
                        String postID = node.get("name").asText();
                        Long createdLong = Long.parseLong(node.get("created_utc").asText()) * 1000;
                        String userID = node.get("author").asText();
                        String subredditID = node.get("subreddit_id").asText();

                        String consoleString = postID + " @ " + createdLong + " by " + userID + " in " + subredditID;

                        // format the input into the hbase table
                        // set the row id as the source post id
                        Put p = new Put(Bytes.toBytes(userID));

                        // posts:subreddit_id:post_id - time
                        p.add(Bytes.toBytes("posts"),
                                Bytes.toBytes(subredditID),
                                createdLong,
                                Bytes.toBytes(postID));
                        puts.add(p);
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
//                    Put i = new Put(Bytes.toBytes(o.getDimention(Dimension.Y).getString()),
//                            ((uk.ac.lancs.dsrg.SparkCorpus.Models.Date) o.getDimention(Dimension.X).getObject()).getDateTime().getMillis());
//                    i.add(Bytes.toBytes("com" + ((Community) o.getDimention(Dimension.Z).getObject()).getLevel()),
//                            Bytes.toBytes(o.getDimention(Dimension.Z).getString()),
//                            ((uk.ac.lancs.dsrg.SparkCorpus.Models.Date) o.getDimention(Dimension.X).getObject()).getDateTime().getMillis(),
//                            Bytes.toBytes(o.getObservation().toString()));
//                    puts.add(i);
            }
        });

        logger.info("Finished Writing Data to HBase");

    }

    public static void WriteEdges() {

        // Configure Spark Context for submitting jobs
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("NER Diffusion - Edge Writer - Java");
        sparkConf = new SparkConf()
                .setMaster(ConfigLoader.getInstance().getValue("mesos"))
                .setAppName(ConfigLoader.getInstance().getValue("appname"));
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
//        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample/thinned1.json",
//                minPartitions);
        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json",
                minPartitions);
        final ObjectMapper mapper = new ObjectMapper();

        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_edges";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("replies");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
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
                while(stringIterator.hasNext()) {
                    // get the json string
                    String s = stringIterator.next();
                    try {
                        // parse the json and pull out the parent_id
                        JsonNode node = mapper.readValue(s, JsonNode.class);
                        // get the parent_id element (if it exists)
                        if (node.has("parent_id")) {
                            String targetPostID = node.get("parent_id").asText();
                            String sourcePostID = node.get("name").asText();
                            // get the datetime object as a long when the post was made
                            Long createdLong = Long.parseLong(node.get("created_utc").asText()) * 1000;
                            String consoleString = sourcePostID + " -> " + targetPostID + " @ " + createdLong;

                            // format the input into the hbase table
                            // set the row id as the source post id
                            Put p = new Put(Bytes.toBytes(sourcePostID));

                            // replies:target:post_id
                            p.add(Bytes.toBytes("replies"),
                                    Bytes.toBytes("target"),
                                    createdLong,
                                    Bytes.toBytes(targetPostID));
                            puts.add(p);
                            logger.info("Saving: " + consoleString);
                        }

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

        logger.info("Finished Writing Data to HBase");

    }

    public static void writeTSUserEdges() {

        // Configure Spark Context for submitting jobs
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("NER Diffusion - Edge Writer - Java");
        sparkConf = new SparkConf()
                .setMaster(ConfigLoader.getInstance().getValue("mesos"))
                .setAppName(ConfigLoader.getInstance().getValue("appname"));
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
//        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample/thinned1.json",
//                minPartitions);
        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json",
                minPartitions);
        final ObjectMapper mapper = new ObjectMapper();

        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_user_edges";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("interaction");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
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

                // column family = interaction
                // column_qual = target_id
                // value = subreddit
                // ts = date of source post

                //Iteracte over observations and insert them into HBASE
                List<Put> puts = new ArrayList<Put>();
                while(stringIterator.hasNext()) {
                    // get the json string
                    String s = stringIterator.next();
                    try {
                        // parse the json and pull out the parent_id
                        JsonNode node = mapper.readValue(s, JsonNode.class);
                        // get the parent_id element (if it exists)
                        if (node.has("parent_id")) {
                            String targetPostID = node.get("parent_id").asText();
//                            String sourcePostID = node.get("name").asText();
                            String sourceUserID = node.get("author").asText();
                            String subredditID = node.get("subreddit_id").asText();
                            // get the datetime object as a long when the post was made
                            Long createdLong = Long.parseLong(node.get("created_utc").asText()) * 1000;
//                            String consoleString = sourcePostID + " -> " + targetPostID + " @ " + createdLong;

                            // query HBase for the postdetails of the target
                            HTable table2 = new HTable(config, TableName.valueOf("ner_diffusion_post_details"));
                            Get get = new Get(Bytes.toBytes(targetPostID));
                            Result result = table2.get(get);
                            String targetUserID = Bytes.toString(result.getValue(Bytes.toBytes("details"),
                                    Bytes.toBytes("user")));
                            table2.close();

                            // format the input into the hbase table
                            // set the row id as the source post id
                            Put p = new Put(Bytes.toBytes(sourceUserID));

                            // replies:target:post_id
                            p.add(Bytes.toBytes("interaction"),
                                    Bytes.toBytes(targetUserID),
                                    createdLong,
                                    Bytes.toBytes(subredditID));
                            puts.add(p);
                            logger.info("Saving: " + targetUserID);
                        }

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

        logger.info("Finished Writing Data to HBase");


    }

    public static void writeEntityPosts() {
        String appName = "NER Diffusion - Entity Posts Writer - Java";

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
        JavaRDD<String> lines = ctx.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated",
                minPartitions);
        final ObjectMapper mapper = new ObjectMapper();

        // prepare the HBase table for data entry
        final String tableName = "ner_diffusion_entity_posts";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add("posts");
        HBaseWriter.buildTable(columnFamilies, tableName);

        // force partitioning to enable scaling
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
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
                while(stringIterator.hasNext()) {
                    // get the json string
                    String s = stringIterator.next();
                    try {

                        JsonNode node = mapper.readTree(s);
                        String postID = node.get("name").asText();
                        String userID = node.get("author").asText();

                        // use a regex to extract the section containing the entities data as jackson is screwing up
                        // the char encoding of the entity array values
                        HashSet<String> entities = new HashSet<String>();
                        Pattern pattern = Pattern.compile("\\[\".+\"\\]");
                        Matcher m = pattern.matcher(s);
                        if (m.find()) {
                            String matchedList = m.group();
                            if (matchedList.length() > 2) {
                                String[] entityToks = matchedList.replace("[","").replace("]","").split("\", \"");
                                for (String entity : entityToks) {
                                    // convert the entity into a usable key
                                    entities.add(entity.replace("\"", "").trim().replace(" ", "_"));
                                }
                            }
                        }
                        Long createdLong = Long.parseLong(node.get("created_utc").asText()) * 1000;

                        // for each entity insert the record into the table
                        for (String entity : entities) {
                            String consoleString = postID
                                    + " @ " + createdLong
                                    + " by " + userID
                                    + " contains " + entity;
                            // format the input into the hbase table
                            // set the row id as the entity
                            Put p = new Put(Bytes.toBytes(entity));
                            // posts:post_id:userid - with ts
                            p.add(Bytes.toBytes("posts"),
                                    Bytes.toBytes(postID),
                                    createdLong,
                                    Bytes.toBytes(userID));

                            puts.add(p);
                            logger.info("Saving: " + consoleString);

                        }
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
//                    Put i = new Put(Bytes.toBytes(o.getDimention(Dimension.Y).getString()),
//                            ((uk.ac.lancs.dsrg.SparkCorpus.Models.Date) o.getDimention(Dimension.X).getObject()).getDateTime().getMillis());
//                    i.add(Bytes.toBytes("com" + ((Community) o.getDimention(Dimension.Z).getObject()).getLevel()),
//                            Bytes.toBytes(o.getDimention(Dimension.Z).getString()),
//                            ((uk.ac.lancs.dsrg.SparkCorpus.Models.Date) o.getDimention(Dimension.X).getObject()).getDateTime().getMillis(),
//                            Bytes.toBytes(o.getObservation().toString()));
//                    puts.add(i);
            }
        });

        logger.info("Finished Writing Data to HBase");
    }

}
