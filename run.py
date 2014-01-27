__author__ = 'danielkershaw'
from alcoholscore import AlcoholScore
from pymongo import MongoClient

if __name__ == "__main__":
    #tmp = ['-r', 'hadoop', '--conf-path', 'mrjob.conf', '--jobconf', 'mapred.reduce.tasks=42', '--jobconf', 'my.job.settings.startdate=2013-06-10', '--jobconf', 'my.job.settings.enddate=2013-06-11', meterFile]

    client = MongoClient('mongodb://localhost:27017/')
    db = client['AlcoholTweetsResults']
    results = db.results
    print results
    # mr_job = AlcoholScore(args=['./testdata/TWEETDUMPS.json','-r','emr','--conf-path', '.mrjob.conf'])
    # with mr_job.make_runner() as runner:
    #     print  mr_job.show_steps()
    #     runner.run()
    #
    #     for line in runner.stream_output():
    #         key, value = mr_job.parse_output_line(line)
    #         value_id = results.insert(value)
    #         print value