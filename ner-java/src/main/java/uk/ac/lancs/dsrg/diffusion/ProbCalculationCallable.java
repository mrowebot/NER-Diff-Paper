package uk.ac.lancs.dsrg.diffusion;

import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.concurrent.Callable;

/**
 * Author: Matthew Rowe
 * Email: m.rowe@lancaster.ac.uk
 * Date / Time : 24/11/15 / 12:44
 */
public class ProbCalculationCallable implements Callable<HashMap<String, Object>>, ModelSettings {

    int probSetting;
    int modelSetting;
    String u;
    String v;
    long date;
    boolean inResultsTable;
    Configuration config;

    public ProbCalculationCallable(int probSetting, int modelSetting,
                                   String u, String v,
                                   long date,
                                   boolean inResultsTable,
                                   Configuration config) {
        this.probSetting = probSetting;
        this.modelSetting = modelSetting;
        this.date = date;
        this.u = u;
        this.v = v;
        this.inResultsTable = inResultsTable;
        this.config = config;
    }

    public HashMap<String, Object> call() {
        double p_vu = 0;
        // only run this if in the resultsTable
        if (this.inResultsTable) {
            // update based on the prob setting
            if(probSetting == STATIC_BERNOULLI) {

                // calculate the influence probability between v and u based on the model setting
                switch (modelSetting) {
                    case ACTIONS_ONLY:
                        p_vu = GeneralThresholdAPTest.computeActionBernoulliStatic(u, v);
                        break;
                    case INTERACTIONS_ONLY:
                        p_vu = GeneralThresholdAPTest.computeInteractionBernoulliStatic(u, v, date);
                        break;
                    case COMMUNITY_HOMOPHILY:
                        p_vu = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliStatic(u, v, date);
                        break;
                    case LINEAR_COMBINATION:
                        double p_vu_a = GeneralThresholdAPTest.computeActionBernoulliStatic(u, v);
                        double p_vu_i = GeneralThresholdAPTest.computeInteractionBernoulliStatic(u, v, date);
                        double p_vu_c = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliStatic(u, v, date);
                        p_vu = ((1/3) * p_vu_a) + ((1/3) * p_vu_i) + ((1/3) * p_vu_c);
                        break;
                }

            } else {
                // calculate the influence probability between v and u based on the model setting
                switch (modelSetting) {
                    case ACTIONS_ONLY:
                        p_vu = GeneralThresholdAPTest.computeActionBernoulliTau(u, v);
                        break;
                    case INTERACTIONS_ONLY:
                        p_vu = GeneralThresholdAPTest.computeInteractionBernoulliTau(u, v, date);
                        break;
                    case COMMUNITY_HOMOPHILY:
                        p_vu = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliTau(u, v, date);
                        break;
                    case LINEAR_COMBINATION:
                        double p_vu_a = GeneralThresholdAPTest.computeActionBernoulliTau(u, v);
                        double p_vu_i = GeneralThresholdAPTest.computeInteractionBernoulliTau(u, v, date);
                        double p_vu_c = GeneralThresholdAPTest.computeCommunityHomophilyBernoulliTau(u, v, date);
                        p_vu = ((1/3) * p_vu_a) + ((1/3) * p_vu_i) + ((1/3) * p_vu_c);
                        break;
                }
            }
        }

        // return map
        HashMap<String, Object> result = new HashMap<String, Object>();
        result.put("u", this.u);
        result.put("v", this.v);
        result.put("prob", p_vu);

//        return rand.nextDouble();
        return result;
    }

}
