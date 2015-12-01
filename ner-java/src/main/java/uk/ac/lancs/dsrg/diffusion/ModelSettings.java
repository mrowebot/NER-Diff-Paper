package uk.ac.lancs.dsrg.diffusion;

/**
 * Author: Matthew Rowe
 * Email: m.rowe@lancaster.ac.uk
 * Date / Time : 23/11/15 / 15:22
 */
public interface ModelSettings {

    final int ACTIONS_ONLY = 1;
    final int INTERACTIONS_ONLY = 2;
    final int COMMUNITY_HOMOPHILY = 3;
    final int LINEAR_COMBINATION = 4;

    final int STATIC_BERNOULLI = 1;
    final int DT_BERNOULLI = 2;
}
