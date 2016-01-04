package uk.ac.lancs.dsrg;

import uk.ac.lancs.dsrg.diffusion.DiffusionResultsCompiler;
import uk.ac.lancs.dsrg.diffusion.GeneralThresholdAPTest;
import uk.ac.lancs.dsrg.diffusion.ModelSettings;

/**
 * Created by rowem on 16/11/15.
 */
public class Main implements ModelSettings {


    public static void main(String[] args) {


        // Step 1 - load data into HBase from Reddit JSON
        // write the edges into hbase
//        HBaseWriter.WriteEdges();

        // write the post details into hbase
//        HBaseWriter.WritePostDetails();

        // write the entity to post information into Hbase
//        HBaseWriter.writeEntityPosts();

        // write the TS edges between users
//        HBaseWriter.writeTSUserEdges();

        // write a mapping betwen user and his posts
//        HBaseWriter.writeUserPosts();


        // Step 2 - build the training data and store in HBase
        // set the induction mode. 1 = dev, 2 = full
        int mode = 2;

        // stage 1:
//        GeneralThresholdAPTrain.storeAv2uMatrix(mode);
        // stage 2:
//        GeneralThresholdAPTrain.computeTaus(mode);

        // stage 3:
//        GeneralThresholdAPTrain.storeAv2uMatrixPostTaus(mode);

        // stage 4: log the taus for analysis
//        TausAnalysis.logTaus();

        // stage 5: derive the exposure curves for the top-500 entities
//        ExposureDynamics.deriveExposurCurvesTopEntities();


        // Step 3 - test the model (configured within the testModel() method)
//        run the model to calculate the adoption probabilities vs outcomes
//        GeneralThresholdAPTest.testModel();

        // Step 4 -  write the results to a local file for computation of the ROC values
        DiffusionResultsCompiler.logFullResults();

    }
}
