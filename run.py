__author__ = 'danielkershaw'
from alcoholscore import AlcoholScore
from pymongo import MongoClient
import os
import re

if __name__ == "__main__":
    tmp = ['-r', 'hadoop', '--conf-path', 'mrjob.conf', '--jobconf', 'mapred.reduce.tasks=42', '--jobconf', '--jobconf']
    #files = [ f[:-1] for f in open('files')]
    #
    # print files

    client = MongoClient('mongodb://scc-kershaw.lancs.ac.uk:27017/')
    db = client['AlcoholTweetsResults']
    results = db['Results']

    tmp1 = ['splitoutput-46.json','--conf-path', '.mrjob.conf', '--file', 'postcodes.csv', '--file', 'english', '--file', 'postcodeareas.csv', '--file', 'terms.csv']
    #files = ["splitoutput-2014-01-01.json"]
    for f in range(0,1):
	print "run"
        hadoopSettings = ['hdfs:///twitter1/*','-r','hadoop','--hadoop-version','2.4.0','--conf-path', '.mrjob.conf', '--file', 'postcodes.csv', '--file', 'english', '--file', 'postcodeareas.csv', '--file', 'terms.csv','--jobconf', 'mapred.reduce.tasks=42']
        print hadoopSettings
        mr_job = AlcoholScore(args=hadoopSettings)
       	with mr_job.make_runner() as runner:
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                value_id = results.insert(value)
                #print value
