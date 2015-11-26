import csv
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import matplotlib.pyplot as plt

def compute_per_entity_rocs(prob_setting, model_setting):
    with open('../data/logs/roc_results_per_entity_p_' + str(prob_setting)
                      + "_m_" + str(model_setting)
                      + '.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            try:
                entity = row[0]
                labels = []
                probs = []
                for i in range(1, len(row)):
                    label = float(row[i].split(",")[0])
                    if label is not 2:
                        prob = float(row[i].split(",")[1])
                        labels.append(label)
                        probs.append(prob)
                labels_array = np.array(labels)
                probs_array = np.array(probs)
                auc = roc_auc_score(labels_array, probs_array)
                print entity + " - AUC for p_s " + str(prob_setting) \
                      + " with m_s " + str(model_setting) \
                      + " = " + str(auc)

            except:
                pass

            # Use scikit learn to compute the roc values


def compute_total_roc(prob_setting, model_setting):
    with open('../data/logs/roc_results_p_' + str(prob_setting)
                      + "_m_" + str(model_setting)
                      + '.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        row_count = 0
        labels = []
        probs = []
        for row in reader:
            try:
                label = float(row[0])
                if label < 2:
                    labels.append(label)
                    prob = float(row[1])
                    probs.append(prob)
            except:
                pass

            row_count += 1
            # if row_count > 10000:
            #     break

        # Use scikit learn to compute the roc values
        labels_array = np.array(labels)
        # scale the probs array to smooth out the roc calculation
        probs_array = np.array(probs)
        # scalar_transform = MinMaxScaler()
        # scaled_probs_array = scalar_transform.fit_transform(probs_array)
        roc_auc = roc_auc_score(labels_array, probs_array)
        print "AUC for p_s " + str(prob_setting) + " with m_s " + str(model_setting) + " = " + str(roc_auc)

        # Plot the ROC curve - only bother with this if we have the space
        # fpr, tpr, thresholds = roc_curve(labels_array, probs_array, pos_label=1.0)
        # # print fpr
        # # print tpr
        # plt.figure()
        # plt.plot(fpr, tpr)
        # plt.plot([0, 1], [0, 1], 'k--')
        # plt.xlim([0.0, 1.0])
        # plt.ylim([0.0, 1.05])
        # plt.xlabel('FPR')
        # plt.ylabel('TPR')
        # # plt.title('Receiver operating characteristic example')
        # # plt.legend(loc="lower right")
        # plt.savefig('../plots/roc.pdf', bbox_inches='tight')
        # plt.close()


#### Main execution area
prob_settings = [1]
model_settings = [1]
for prob_setting in prob_settings:
    for model_setting in model_settings:
        compute_total_roc(prob_setting, model_setting)
        # compute_per_entity_rocs(prob_setting, model_setting)

