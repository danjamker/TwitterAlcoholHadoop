#-*-coding: utf-8 -*-

'''
This module computes the number of movies rated by each
user.
 
'''

__author__ = 'Daniel Kershaw <d.kershaw1@lancaster.ac.uk>'

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import JSONProtocol
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from pprint import pprint
import time
import datetime
import csv
import numpy
from scipy.spatial import cKDTree
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
                score = float(tnmc) / len(words)
            except:
                return score

        return score

class Geo:

    def __init__(self):

        self.data = []
        self.regions = []

        for row in csv.DictReader(open('postcodes.csv', 'rb'), ["id", "postcode", "lat", "lng"]):
            self.data.append(row)

        for row in csv.DictReader(open('postcodeareas.csv', 'rU'), ["initial", "region"]):
            self.regions.append(row)

        for row in self.data:
            row["area"] = self.postcodetoareacode(row["postcode"])
            for r in self.regions:
                if r["initial"] == row["area"]:
                    row["region"] = r["region"]

        self.tree = cKDTree(self.coordinates())

    def postcodetoareacode(self, postcode):
        '''Returns the post code region for a post code

        This truncates the f substring of letter e.g. YO26 -> YO

        '''
        r = ""
        t = True
        for l in postcode:
            if l.isdigit() == False:
                if t == True:
                    r += str(l)
            else:
                t = False
        return r

    def coordinates(self):
        '''Returns a list of all the coordinates of all the postcodes in the list

        '''
        return [[float(record["lat"]), float(record["lng"])] for record in self.data]

    def postcodes(self):
        '''
        Returns a list of all the post codes
        '''
        return list(set([record["postcode"] for record in self.data]))

    def postcoderegions(self):
        '''
        Returnes a list of all the post code regions
        '''
        return list(set([record["area"] for record in self.data]))

    def regions(self):
        '''
        Returns a list of all the regions
        '''
        return list(set([record["region"] for record in self.data]))

    def findnearestpostcode(self, long, lat, k=2):
        '''
        Returns a list of k nearest nabours of points in the UK
        '''
        r = []
        dists, indexes = self.tree.query(numpy.array([long, lat]), k)
        if k > 1:
            for dist, index in zip(dists, indexes):
                tmp = self.data[index]
                tmp["distance"] = dist
                r.append(tmp)
        else:
            tmp = self.data[indexes]
            tmp["distance"] = dists
            r.append(tmp)

        return r

    def postcodesinarea(self, area):
        '''
        Returnes a list of all the postcodes in an area.
        '''
        tmp = []
        for row in self.data:
            if row["area"] == area:
                tmp.append(row)
        return tmp

    def postcoderegonsinregon(self, region):
        '''
        List all the post code in a region
        '''
        tmp = []
        for row in self.data:
            if row["region"] == region:
                tmp.append(row)
        return tmp

def yeildkey(locaiton, time, granularity):
    v1 ={}
    v1["location"] = locaiton
    v1["time"] = time
    v1["granularity"] = granularity
    return v1

def collocation(text):
    #set all words to lower case
    words = [w.lower() for w in wordpunct_tokenize(text)]

    bfc = BigramCollocationFinder.from_words(words)
    stopset = set(line.strip() for line in open('english'))
    filter_stops = lambda w: len(w) < 3 or w in stopset
    bfc.apply_word_filter(filter_stops)
    bfcScore = [p for p, s in bfc.score_ngrams(BigramAssocMeasures.likelihood_ratio)]
    return bfcScore

def empytDoct(terms):
    t = {}
    for w in terms:
        t[w['term']] = 0
    return t

def probabilityDistribution(terms, count):
    for key in terms:
        prob = 0
        try:
            prob = float(terms[key])/float(count)
        except:
            prob = 0
        terms[key] = float(prob)
    return terms

def addDicts(master, addition):
    for key in master:
        master[key] = master[key] + addition[key]

    return master

def termFrequancy(termsDic ,text):
    count = 0
    terms = {}
    for w in termsDic:
        terms[w['term']] = 0

    words = [x.lower() for x in wordpunct_tokenize(text)]

    for w in words:
        count = count + 1
        if w in terms:
            terms[w] = terms[w] + 1

    return count, terms

class AlcoholScore(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    INTERNAL_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper_init(self):
        self.g = Geo()
        self.ts = TwitterScore()

    def mapper(self, key, line):
        """
        Mapper: send score from a single movie to
        other movies
        """
        if "geo" in line:
	    tweet = line
            postcode = ""
            region = ""
            postcoderegion = ""
            aresarea = []
            if tweet["geo"] != None:
                nearesarea = self.g.findnearestpostcode(tweet["geo"]["coordinates"][0], tweet["geo"]["coordinates"][1], 1)
                postcode = nearesarea[0]["postcode"]
                postcoderegion = nearesarea[0]["area"]
                region = nearesarea[0]["region"]
            else:

                lot = 0
                lit = 0
                count = 0
                for lo, li in tweet["place"]["bounding_box"]["coordinates"][0]:
                    lit = lit + li
                    lot = lot + lo
                    count = count + 1

                nearesarea = self.g.findnearestpostcode(lit / count, lot / count, 1)
                postcode = nearesarea[0]["postcode"]
                postcoderegion = nearesarea[0]["area"]
                region = nearesarea[0]["region"]

            tweet["score"] = self.ts.score(tweet["text"])

            yield(yeildkey(region, time.strftime("%Y-%m-%d",time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "daily"),tweet)
            yield(yeildkey(region, time.strftime("%Y-%m-%dT%H",time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "hour"),tweet)
            yield(yeildkey(postcode, time.strftime("%Y-%m-%d",time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "daily"),tweet)
            yield(yeildkey(postcode, time.strftime("%Y-%m-%dT%H", time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "hour"),tweet)
            yield(yeildkey(postcoderegion, time.strftime("%Y-%m-%d", time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "daily"),tweet)
            yield(yeildkey(postcoderegion, time.strftime("%Y-%m-%dT%H", time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "hour"),tweet)
            yield(yeildkey("National-UK", time.strftime("%Y-%m-%d", time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "daily"),tweet)
            yield(yeildkey("National-UK", time.strftime("%Y-%m-%dT%H", time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), "hour"),tweet)

    def reducer_init(self):
        self.terms = [{'term':"drunk",'weight':1},
        {'term':"wine",'weight':1},
        {'term':"wasted",'weight':1},
        {'term':"pissed",'weight':1},
        {'term':"hungover",'weight':1},
        {'term':"hangover",'weight':1},
        {'term':"vodka",'weight':1}
        ]


        self.ts = TwitterScore()

    def combiner(self, key, values):
        scoresum = 0
        count = 0
        # corupus = ""

        et = empytDoct(self.terms)
        wordcount = 0
        total = 0

        for tweet in values:
            scoresum = scoresum + tweet["score"]
            count = count + 1
            # corupus += tweet["text"]
            #c, dc = termFrequancy(self.terms, tweet["text"])
            #et = addDicts(et , dc)
            #wordcount = wordcount + c

        tt = 0
        try:
            tt = float(scoresum) / float(count)
        except:
            tt = 0


        returnStrcut = {}
        returnStrcut["time"] = key["time"]
        returnStrcut["location"] = key["location"]
        returnStrcut["score"] = tt
        #returnStrcut["tweetCount"] = count
        #returnStrcut["termfreq"] = dict(et)
        #returnStrcut["termprob"] = probabilityDistribution(et , wordcount)
        returnStrcut["wordCount"] = wordcount
        # returnStrcut["collocation"] = words
        returnStrcut["type"] = key["granularity"]

        yield(None ,returnStrcut)

    def reducer(self, key, values):
        scoresum = 0
        count = 0
        # corupus = ""

        et = empytDoct(self.terms)
        wordcount = 0
        total = 0

        for tweet in values:
            scoresum = scoresum + tweet["score"]
            count = count + 1
            # corupus += tweet["text"]
            #c, dc = termFrequancy(self.terms, tweet["text"])
            #et = addDicts(et , dc)
            #wordcount = wordcount + c

        tt = 0
        try:
            tt = float(scoresum) / float(count)
        except:
            tt = 0




        # words = []
        # for coll in collocation(corupus):
        #     if len(set(coll) & set([t["term"] for t in self.ts.data])) > 0:
        #         words.append(coll)

        returnStrcut = {}
        returnStrcut["time"] = key["time"]
        returnStrcut["location"] = key["location"]
        returnStrcut["score"] = tt
        #returnStrcut["tweetCount"] = count
        #returnStrcut["termfreq"] = dict(et)
        #returnStrcut["termprob"] = probabilityDistribution(et , wordcount)
        #returnStrcut["wordCount"] = wordcount
        # returnStrcut["collocation"] = words
        returnStrcut["type"] = key["granularity"]

        yield(None ,returnStrcut)

    def steps(self):
        return [
            self.mr(mapper_init=self.mapper_init,
                    mapper=self.mapper,
                    combiner_init=self.reducer_init,
                    combiner = self.combiner,
                    reducer_init=self.reducer_init,
                    reducer=self.reducer)
        ]

if __name__ == '__main__':

    AlcoholScore.run()
