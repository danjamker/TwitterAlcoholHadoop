__author__ = 'danielkershaw'
import csv
from nltk.tokenize import wordpunct_tokenize

class TwitterScore:

    def __init__(self):
        self.data = []

        for row in csv.DictReader(open('terms.csv', 'rb'), ["term","weight"]):
            self.data.append(row)

    def score(self, tweet):
        terms = [t["term"] for t in self.data]
        score = 0
        words = [x.lower() for x in wordpunct_tokenize(tweet)]
        tnmc = len(set(terms) & set(words))
        tnmu = 0
        for w in words:
            if w in terms:
                tnmu = tnmu + 1

        if tnmc or tnmu != 0.0:
            try:
                score = float(tnmc) / float(tnmu)
            except:
                return score

        return score
