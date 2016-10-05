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
from collections import Counter

#Regular expressions for stripping text
url_re = re.compile(r"(http|ftp|https)(:\/\/)([\w\-_]+(?:(?:\.[\w\-_]+)+))([\w\-\.,@?^=%&amp;:\/~\+#]*[\w\-\@?^=%&amp;\/~\+#])?")
mention_re = re.compile(r"(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z0-9\_]+[A-Za-z0-9\_]+)")
word_pad_re = re.compile(r"(.)\1\1*}")
#matches alphanumeric and # + @
alphanumeric_only_re = re.compile('([^\s\w#@]|_)+', re.UNICODE)

try:
    # Wide UCS-4 build
    emoji_re = re.compile(u'['
        u'\U0001F300-\U0001F64F'
        u'\U0001F680-\U0001F6FF'
        u'\u2600-\u26FF\u2700-\u27BF]', 
        re.UNICODE)
except re.error:
    # Narrow UCS-2 build
    emoji_re = re.compile(u'('
        u'\ud83c[\udf00-\udfff]|'
        u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
        u'[\u2600-\u26FF\u2700-\u27BF])', 
        re.UNICODE)

inputfile = None
outputfile = None

def prepare_and_tokenize(text, url_scheme='st', strip_word_padding=False, alphanumeric_only = False):
    """ 
        Prepares the raw tweet text for tokenisation, then tokenizes it using twokenize (https://github.com/ianozsvald/ark-tweet-nlp-python)
    """   
    #handle URLS
    # possible schemes 'st': single token, 'leave'
    if url_scheme != 'leave':
        text = re.sub(url_re, "hyperlinktoken", text)
    text = re.sub(mention_re, "mentiontoken", text)
    
    # reduce extended words to a shorter token. e.g. "reeeeeeeeeee"->"ree", "hahahaha" -> "haha"
    if strip_word_padding:
        #TODO: Make this actually work
        text = re.sub(word_pad_re, "", text)
    
    # strip out non AN characters (except # and @)
    if alphanumeric_only:
        text = re.sub(alphanumeric_only_re, "", text)

    # add spaces between emoji
    for match in list(set(emoji_re.findall(text))):
        text = text.replace(match," "+match+" ")

    return tokenize(text)

def train_pipeline(train_docs, train_targets):
    """
        fit classifier and vectorizer to input documents (tweets) and labels.
    """

    #SGDClassifier with default inputs equivalent to SVM with linear classifier but quicker.
    classifier = SGDClassifier(n_jobs=-1)
    #TFIDF UNI,BI, TRI grams
    # exclude stop words by doc frequency, ie if appear in 70% remove, also remove infrequent words
    vectorizer = TfidfVectorizer(tokenizer=prepare_and_tokenize, ngram_range=(1,3), min_df=25, max_df=0.7, sublinear_tf=True)

    # build simple pipeline

    pipeline = Pipeline([
        ('vectorizer', vectorizer),
        ('classifier', classifier)
    ])

    pipeline.fit(train_docs, train_targets)

    return pipeline

def assess_pipeline(test_docs, test_targets, pipeline):
    """ Apply pipeline to testing documents and return performance metrics. """
    predictions = pipeline.predict(test_docs)
    result = accuracy_score(test_targets, predictions)

    #TODO: Assess more than just accuracy

    return result


def run_cross_validation(input_docs, input_targets, folds=10):
    #TODO: add non stratify case
    kf = cross_validation.StratifiedKFold(input_targets, n_folds=folds, shuffle=True)

    print "Beginning cross validation with",folds,"folds."
    results = []
    fold = 1
    for train_index, test_index in kf:
        print "Beginning fold:",fold
        #extract testing docs and labels
        test_docs = (input_docs[idx] for idx in test_index)
        test_targets = (input_targets[idx] for idx in test_index)

        #extract training docs and labels
        train_docs = (input_docs[idx] for idx in train_index)
        train_targets = (input_targets[idx] for idx in train_index)
        print "Train/test split performed.\nBeginning training."
        pipeline = train_pipeline(train_docs, train_targets)
        print "Training complete."
        print "Assessing model."
        result = assess_pipeline(test_docs, test_targets, pipeline)
        print "Fold",fold,"accuracy:",result
        results.append(result)
        fold += 1 

    return results

def gather_docs(indices):
    """ Take an ordered list of document indices (lines of file) and extract the full document. """
    global inputfile
    docs = []
    with open(inputfile, "r") as f:
        idx = 0
        #iterate through input file, extracting target docs as index reached
        for doc_idx in indices:
            while idx < doc_idx:
                f.readline()
                idx += 1
            line = f.readline().strip()
            tweet = json.loads(line)
            text = "<tweetboundary> " + " <tweetboundary> ".join([" ".join(t.split()) for t in tweet["tweets"]]).lower() + " <tweetboundary>"
            docs.append(text)

            idx += 1
    return docs

def extract_label_subset(all_labels, target_number=2000, random=True):
    ''' Attempts to extract a balanced subset with target_number members of each label '''
    label_counts = {label: 0 for label in np.unique(all_labels)}
    label_indices = range(len(all_labels))
    if random:
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
    # set up, and run classification pipeline with input dataset
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
    print Counter(labels)
    
    # Indices
    print "Gather label subset..."
    subset_indices = sorted(extract_label_subset(labels,target_number=150), key=int)
    label_subset = [labels[idx] for idx in subset_indices]
    print "Gathering documents subset..."
    docs_subset = gather_docs(subset_indices)
    print "Extracted subset with",len(subset_indices),"entries."

    results = run_cross_validation(docs_subset,label_subset)
    print "Avg results:",np.mean(results)

if __name__ == "__main__":
    init()
