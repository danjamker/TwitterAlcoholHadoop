__author__ = 'danielkershaw'

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import JSONProtocol

from TwitterScore import TwitterScore
from Geo import Geo
import datetime

class AlcoholGraphs(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    INTERNAL_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper_init(self):
        self.g = Geo()
        self.ts = TwitterScore()

    def mapper(self, key, value):
        pass

    def reducer_init(self):
        pass

    def reducer(self, key, values):
        pass

    def steps(self):
        return [
            self.mr(mapper_init=self.mapper_init,
                    mapper=self.mapper,
                    reducer_init=self.reducer_init,
                    reducer=self.reducer)
        ]

if __name__ == '__main__':
    AlcoholGraphs.run()
