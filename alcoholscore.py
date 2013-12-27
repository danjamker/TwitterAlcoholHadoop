#-*-coding: utf-8 -*-

'''
This module computes the number of movies rated by each
user.
 
'''

__author__ = 'Daniel Kershaw <d.kershaw1@lancaster.ac.uk>'

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import JSONProtocol

import datetime
from Geo import Geo
from pprint import pprint
import json
from TwitterScore import TwitterScore
import collocation

def yeildkey(locaiton, time, granularity):
    v1 ={}
    v1["location"] = locaiton
    v1["time"] = time
    v1["granularity"] = granularity
    return v1

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
        tweet = line
        postcode = ""
        region = ""
        postcoderegion = ""
        aresarea = []
        if tweet["geo"] != None:

            nearesarea = self.g.findnearestpostcode(tweet["geo"]["coordinates"][1], tweet["geo"]["coordinates"][0], 1)
            postcode = nearesarea[0]["postcode"]
            postcoderegion = nearesarea[0]["area"]
            region = nearesarea[0]["region"]
        else:
            tweet["place"]
            tweet["place"]["bounding_box"]["coordinates"][0]
            lot = 0
            lit = 0
            count = 0
            for lo, li in tweet["place"]["bounding_box"]["coordinates"][0]:
                lit = lit + li
                lot = lot + lo
                count = count + 1

            nearesarea = self.g.findnearestpostcode(lot / count, lit / count, 1)

            postcode = nearesarea[0]["postcode"]
            postcoderegion = nearesarea[0]["area"]
            region = nearesarea[0]["region"]

        tweet["score"] = self.ts.score(tweet["text"])
        # if tweet["score"] > 0:
        #     print region.replace(' ', '-') + " " + datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%d")
        #     pprint(line)
        #     print tweet["score"]

        yield(yeildkey(region, datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%d"), "daily"),tweet)
        yield(yeildkey(region, datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%dT%H"), "hour"),tweet)
        yield(yeildkey(postcode, datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%d"), "daily"),tweet)
        yield(yeildkey(postcode, datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%dT%H"), "hour"),tweet)
        yield(yeildkey(postcoderegion, datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%d"), "daily"),tweet)
        yield(yeildkey(postcoderegion, datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%dT%H"), "hour"),tweet)
        yield(yeildkey("National-UK", datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%d"), "daily"),tweet)
        yield(yeildkey("National-UK", datetime.datetime.strptime(str(tweet["createdAt"]), "%d-%M-%Y %H:%m:%S").strftime("%Y-%m-%dT%H"), "hour"),tweet)

    def reducer_init(self):
        self.ts = TwitterScore()

    def reducer(self, key, values):
        ts = TwitterScore()
        scorearray = [t["score"] for t in values]

        tt = 0
        try:
            tt = float(sum(scorearray)) / float(len(scorearray))
        except:
            tt = 0

        corupus = ""
        count = 0
        for tweet in values:
            corupus += tweet["text"]
            count = count + 1
        words = []
        for coll in collocation.collocation(corupus):
            if len(set(coll) & set([t["term"] for t in ts.data])) > 0:
                words.append(coll)

        returnStrcut = {}
        returnStrcut["time"] = key["time"]
        returnStrcut["location"] = key["location"]
        returnStrcut["score"] = tt
        returnStrcut["tweetCount"] = count
        returnStrcut["collocation"] = words
        returnStrcut["type"] = key["granularity"]

        yield(None ,returnStrcut)

    def steps(self):
        return [
            self.mr(mapper_init=self.mapper_init,
                    mapper=self.mapper,
                    reducer=self.reducer)
        ]

if __name__ == '__main__':
    AlcoholScore.run()
