## Classifying

# Create target feature (expected_match)
#    Simple condition sum(row) == len(row)
#    **Only needed for supervised methods

from recordlinkage import NaiveBayesClassifier         # sklearn naive_bayes.BernoulliNB
from recordlinkage import LogisticRegressionClassifier # sklear logistic regresssion

import logging

classificationlogger = logging.getLogger(__name__)

def estimate_true(comparisons):
    rexp = len(comparisons.columns)
    # if len(comparisons.columns) > 3:
    #     rexp -= 1
    rsum = comparisons.sum(axis=1)
    return comparisons.index[rsum == rexp]


def build_classifier(name, comparisons, match_index=None, **kwargs):
    models = {
        'logistic': LogisticRegressionClassifier,
        'nbayes': NaiveBayesClassifier,
    }
    
    # Create classifier
    clf = models[name](**kwargs)
    
    # Fit to data, if supervised a match index is required
    if match_index is not None:
        clf.fit(comparison_vectors=comparisons, match_index=match_index)
    else:
        clf.fit(comparison_vectors=comparisons)
    
    classificationlogger.info(f'Successfully created and fit {name} model.')
    return clf
