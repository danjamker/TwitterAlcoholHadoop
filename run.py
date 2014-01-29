__author__ = 'danielkershaw'
from alcoholscore import AlcoholScore
from pymongo import MongoClient
import os
import re

if __name__ == "__main__":
    #tmp = ['-r', 'hadoop', '--conf-path', 'mrjob.conf', '--jobconf', 'mapred.reduce.tasks=42', '--jobconf', 'my.job.settings.startdate=2013-06-10', '--jobconf', 'my.job.settings.enddate=2013-06-11', meterFile]
    files = [ "./Data/"+f for f in os.listdir("./Data/") if f.endswith(".json")]
    files = ['s3://mrjob-f18cc7d334fb09e5/tmp/alcoholscore.danielkershaw.20140127.141825.320147/files/']
    client = MongoClient('mongodb://localhost:27017/')
    db = client['AlcoholTweetsResults']
    results = db['111127012014']
    tmp = files + ['-r','emr','--conf-path', '.mrjob.conf']

    tmp1 = ['./Unsplit Data/splitoutput-0.json','--conf-path', '.mrjob.conf', '--file', 'postcodes.csv', '--file', 'english', '--file', 'postcodeareas.csv', '--file', 'terms.csv']
    mr_job = AlcoholScore(args=tmp)
    with mr_job.make_runner() as runner:
        runner.run()

        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            value_id = results.insert(value)
            print value