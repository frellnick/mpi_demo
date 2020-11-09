#evaluate.py

from recordlinkage import reduction_ratio, confusion_matrix

def simple_evaluation(source, links_pred, links_true, links_candidates):
    return {
        'rratio': reduction_ratio(links_pred, source),
        'cmatrix': confusion_matrix(links_true, links_pred, links_candidates)
    }