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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Author: Matthew Rowe
 * Email: m.rowe@lancaster.ac.uk
 * Date / Time : 20/11/15 / 16:12
 */
public class DiffusionResultsCompiler {

    final static Logger logger = Logger.getLogger(DiffusionResultsCompiler.class);

    /*
     * Generates a large tsv file of (outcome, prob) pairs over all entities for probSetting and modelSettings in the method
     */
    public static void logFullResults() {

        int probSetting = 2;
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ConfigLoader.getInstance().getValue("hbase.zookeeper.quorum"));
        int[] modelSettings = {2, 3};
        for (int modelSetting : modelSettings) {

            try {
                // get the interactions between the source and the target and when these occurred
                HTable table = new HTable(config, TableName.valueOf("ner_diffusion_results"));
                Scan scan = new Scan();
                ResultScanner scanner = table.getScanner(scan);
                StringBuffer outputString = new StringBuffer();
                String columnFamily = "prob_" + probSetting + "_m_" + modelSetting;
                System.out.println("Reading results for: " + probSetting + " with model " + modelSetting);
                for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                    String key = Bytes.toString(result.getRow());

//                    double probVal = 0;
//                    List<Cell> probCells = result.getColumnCells(Bytes.toBytes(columnFamily), Bytes.toBytes("prob"));
//                    for (Cell probCell : probCells) {
//                        probVal = Double.parseDouble(Bytes.toString(probCell.getValue()));
//                        break;
//                    }
                    try {
                        Cell probCell = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("prob"));
                        double probVal = Double.parseDouble(Bytes.toString(probCell.getValue()));

//                    double resultVal = 0;
//                    List<Cell> resultCells = result.getColumnCells(Bytes.toBytes(columnFamily), Bytes.toBytes("r_val"));
//                    for (Cell resultCell : resultCells) {
//                        resultVal = Double.parseDouble(Bytes.toString(resultCell.getValue()));
//                        break;
//                    }
                        Cell resultCell = result.getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("r_val"));
                        double resultVal = Double.parseDouble(Bytes.toString(resultCell.getValue()));

                        outputString.append(key + "\t" + resultVal + "\t" + probVal + "\n");
                    } catch(Exception e) {
                        System.err.println(e.getMessage());
                    }
                }

                System.out.println("Writing the results");

                // write to a local file
                try {
                    PrintWriter writer = new PrintWriter("../NER-Diffusion/data/roc_results_p_"
                            + probSetting + "_m_" + modelSetting + ".tsv", "UTF-8");
                    writer.println(outputString);
                    writer.close();
                } catch(Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Finished processing");
                table.close();


            } catch (Exception e) {
                System.err.println("Failed: " + e.getLocalizedMessage());
                e.printStackTrace();
            }

        }
    }

}
