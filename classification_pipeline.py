import csv
import numpy as np
import re
import sklearn
import json
from twokenize import tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import *
from sklearn.pipeline import Pipeline
from sklearn import cross_validation
from sklearn.metrics import *
from random import shuffle
import argparse
from warnings import *

#Regular expressions for stripping text
url_re = re.compile(r"(http|ftp|https)(:\/\/)([\w\-_]+(?:(?:\.[\w\-_]+)+))([\w\-\.,@?^=%&amp;:\/~\+#]*[\w\-\@?^=%&amp;\/~\+#])?")
mention_re = re.compile(r"(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z0-9\_]+[A-Za-z0-9\_]+)")
word_pad_re = re.compile(r"(.)\1\1*}")
#matches alphanumeric and # + @
alphanumeric_only_re = re.compile('([^\s\w#@]|_)+', re.UNICODE)

inputfile = None
outputfile = None

def prepare_and_tokenize(text, url_scheme='st', strip_word_padding=False, alphanumeric_only = False):
    #handle URLS
    # possible schemes 'st': single token, 'leave'
    if url_scheme != 'leave':
        text = re.sub(url_re, "hyperlinktoken", text)
    text = re.sub(mention_re, "mentiontoken", text)
    
    if strip_word_padding:
        #TODO: Make this actually work
        text = re.sub(word_pad_re, "", text)
    
    if alphanumeric_only:
        text = re.sub(alphanumeric_only_re, "", text)
    
    #TODO: Handle emoji
    return tokenize(text)

def train_pipeline(train_docs, train_targets):

    classifier = SGDClassifier(n_jobs=-1)
    vectorizer = TfidfVectorizer(tokenizer=prepare_and_tokenize, ngram_range=(1,2), min_df=25, max_df=0.7, sublinear_tf=True)

    pipeline = Pipeline([
        ('vectorizer', vectorizer),
        ('classifier', classifier)
    ])

    pipeline.fit(train_docs, train_targets)

    return pipeline

def assess_pipeline(test_docs, test_targets, pipeline):
    predictions = pipeline.predict(test_docs)
    result = accuracy_score(test_targets, predictions)
    return result


def run_cross_validation(input_docs, input_targets, folds=10, stratify=True):
    #TODO: add non stratify case
    kf = cross_validation.StratifiedKFold(input_targets, n_folds=folds, shuffle=True)
    results = []
    for train_index, test_index in kf:

        test_docs = [input_docs[idx] for idx in test_index]
        test_targets = [input_targets[idx] for idx in test_index]

        train_docs = [input_docs[idx] for idx in train_index]
        train_targets = [input_targets[idx] for idx in train_index]

        pipeline = train_pipeline(train_docs, train_targets)

        result = assess_pipeline(test_docs, test_targets, pipeline)
        print result
        results.append(result)

    return results

def gather_docs(indices):
    #take in ordered list of indices
    global inputfile
    docs = []
    with open(inputfile, "r") as f:
        idx = 0
        for doc_idx in indices:
            while idx < doc_idx:
                f.readline()
                idx += 1
            line = f.readline().strip()
            tweet = json.loads(line)
            text = " tweetboundary ".join([" ".join(t.split()) for t in tweet["tweets"]]).lower()
            docs.append(text)

            idx += 1
    return docs

def extract_label_subset(all_labels, target_number=2000):
    ''' attempts to extract a balanced subset with target_number members of each label '''
    label_counts = {label: 0 for label in np.unique(all_labels)}
    label_indices = range(len(all_labels))
    shuffle(label_indices)
    subset_indices = []

    for idx in label_indices:
        if label_counts[all_labels[idx]] < target_number:
            subset_indices.append(idx)
            label_counts[all_labels[idx]]+=1
    if not all(count == target_number for count in label_counts.values()):
        warn("The extracted subset was not balanced, try a lower target number.", Warning)
    return subset_indices


def init():
    global inputfile, outputfile, labels

    parser = argparse.ArgumentParser(description = "Classification pipeline used in the paper.")
    parser.add_argument('--inputfile', type=str, required = True, help = 'JSON array of Twitter user object in format: {"label":STRING, "tweets":[STRING 1,...,STRING N]}')
    parser.add_argument('--outputfile', type=str, help = 'File to ouput results to, will print to STDOUT if not set.')
    arguments = parser.parse_args()

    inputfile = arguments.inputfile
    print "Received input:",inputfile
    outputfile = arguments.outputfile
    print "Result to be outputed to:",
    if outputfile:
        print outputfile
    else:
        print "STDOUT"

    #Gather set of labels
    with open(inputfile, "r") as f:
        labels = []
        for line in f:
            tweet = json.loads(line.strip())
            labels.append(tweet["label"])
    print "Received dataset with",len(labels),"entries."

    # Indices
    print "Gather label subset..."
    subset_indices = sorted(extract_label_subset(labels,target_number=3000), key=int)
    label_subset = [labels[idx] for idx in subset_indices]
    print "Gathering documents subset..."
    docs_subset = gather_docs(subset_indices)
    print "Extracted subset with",len(subset_indices),"entries."

    results = run_cross_validation(docs_subset,label_subset)
    print "Avg results:",np.mean(results)

if __name__ == "__main__":
    init()
