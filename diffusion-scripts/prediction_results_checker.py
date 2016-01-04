import csv
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import matplotlib.pyplot as plt
import re

def compute_macro_avg_rocs(prob_setting, model_setting):
    with open('../data/logs/roc_results_p_' + str(prob_setting)
                      + "_m_" + str(model_setting)
                      + '.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        regex_str = compile_test_entities_regex()
        print regex_str

        entity_results = {}
        entity_users = set()
        row_count = 1
        for row in reader:
            try:
                # match the entity using the names of the test_entities
                row_key = row[0]
                if row_key not in entity_users:
                    entity_users.add(row_key)
                    # print row_key
                    for entity_key in re.findall(regex_str, row_key):
                        label = float(row[1])
                        prob = 0
                        # print entity_key
                        if label < 2:
                            prob = float(row[2])

                            # match the entity_key to its existing results
                            if entity_key in entity_results:
                                entity_result = entity_results[entity_key]
                                labels = entity_result['labels']
                                labels.append(label)
                                entity_result['labels'] = labels
                                probs = entity_result['probs']
                                probs.append(prob)
                                entity_result['probs'] = probs
                                entity_results[entity_key] = entity_result
                            else:
                                labels = []
                                labels.append(label)
                                probs = []
                                probs.append(prob)
                                entity_result = {"labels": labels,
                                                 "probs": probs}
                                entity_results[entity_key] = entity_result
            except:
                pass
            row_count += 1
            # if row_count > 1000:
            #     break

        rocs = []
        for entity in entity_results:
            labels = entity_results[entity]['labels']
            probs = entity_results[entity]['probs']
            labels_array = np.array(labels)
            probs_array = np.array(probs)
            try:
                roc_auc = roc_auc_score(labels_array, probs_array)
                rocs.append(roc_auc)
                # print entity + " - AUC for p_s " + str(prob_setting) + " with m_s " + str(model_setting) + " = " + str(roc_auc)
            except:
                pass

        # Calculate the mean roc
        roc_mean = np.mean(np.array(rocs))
        roc_std = np.std(np.array(rocs))
        print "Macro Mean ROC = " + str(roc_mean) + ", with std = " + str(roc_std)





def compile_test_entities_regex():
    entities = []
    with open('../data/logs/test_entities.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            entity = str(row[0]).replace(" ", "_").replace("\"", "").replace(".", "\.")
            entities.append(entity)

    # Build a regex to match the entities
    regex_str = "(.Net|"
    count = 1
    stop = 2
    for entity in entities:
        regex_str += entity + "|"
        # count += 1
        if count > stop:
            break
    regex_str = regex_str[:-1] + ")"
    return regex_str

def compute_micro_avg_roc(prob_setting, model_setting):
    with open('../data/logs/roc_results_p_' + str(prob_setting)
                      + "_m_" + str(model_setting)
                      + '.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        row_count = 0
        labels = []
        probs = []
        entity_users = set()
        for row in reader:
            try:
                entity_user = row[0]
                if entity_user not in entity_users:
                    entity_users.add(entity_user)
                    label = float(row[1])
                    if label < 2:
                        labels.append(label)
                        prob = float(row[2])
                        probs.append(prob)
                        # if prob > 0:
                        #     print str(prob)
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
        print "Micro AUC for p_s " + str(prob_setting) + " with m_s " + str(model_setting) + " = " + str(roc_auc)

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
prob_settings = [2]
model_settings = [2]
# model_settings = [1,2]
for prob_setting in prob_settings:
    for model_setting in model_settings:
        print str(prob_setting) + " with " + str(model_setting)
        compute_micro_avg_roc(prob_setting, model_setting)
        compute_macro_avg_rocs(prob_setting, model_setting)
        # compute_per_entity_rocs(prob_setting, model_setting)

