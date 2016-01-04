package uk.ac.lancs.dsrg.diffusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import uk.ac.lancs.dsrg.SparkCorpus.ConfigLoader;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Matthew Rowe
 * Email: m.rowe@lancaster.ac.uk
 * Date / Time : 20/11/15 / 16:12
 */
public class TausAnalysis {

    final static Logger logger = Logger.getLogger(TausAnalysis.class);

    public static void logTaus() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
        try {
            // get the interactions between the source and the target and when these occurred
            HTable table = new HTable(config, TableName.valueOf("ner_diffusion_taus"));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            ArrayList<Double> taus = new ArrayList<Double>();
            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    taus.add(Double.parseDouble(Bytes.toString(cell.getValue())));
                }
            }

            logger.info("Writing the taus");

            StringBuffer outputString = new StringBuffer();
            for (Double tau : taus) {
                outputString.append(tau + "\n");
            }
            System.out.println("Finished output");
//            System.out.println(outputString);

            // write to a local file
            try {
                // change this to your logging dir position
                PrintWriter writer = new PrintWriter("../NER-Diffusion/data/taus_dist.tsv", "UTF-8");
                writer.println(outputString);
                writer.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
            logger.info("Finished processing");


        } catch (Exception e) {
            logger.error("Failed: " + e.getLocalizedMessage());
        }

        // write the taus to a local file

    }
}
