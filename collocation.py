__author__ = 'Daniel Kershaw <d.kershaw1@lancaster.ac.uk>'

from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from nltk.tokenize import wordpunct_tokenize

def collocation(text):
    #set all words to lower case
    words = [w.lower() for w in wordpunct_tokenize(text)]

    bfc = BigramCollocationFinder.from_words(words)
    stopset = set(line.strip() for line in open('english'))
    filter_stops = lambda w: len(w) < 3 or w in stopset
    bfc.apply_word_filter(filter_stops)
    bfcScore = [p for p, s in bfc.score_ngrams(BigramAssocMeasures.likelihood_ratio)]
    return bfcScore