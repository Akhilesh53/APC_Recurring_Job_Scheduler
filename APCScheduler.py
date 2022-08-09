# using flask_restful
from datetime import datetime
import sched
import time
from tracemalloc import start
from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
import requests
from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger


# creating the flask app
app = Flask(__name__)

# creating an API object
api = Api(app)

# DB to store the Jobs created
jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}

# Multiple threads to run
executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

# Choose which scheduler to use for the job scheduling
scheduler = BackgroundScheduler(
    jobstores=jobstores, executors=executors, job_defaults=job_defaults)
scheduler.start()

# UNCOMMENT THIS LINE IF YOU ARE STARTING WITH  THE PROJECT
# Deletes all the stored jobs in DB
# scheduler.remove_all_jobs()

# DEFINE YOUR JOB HERE, WHATEVER YOU WANT TO PERFORM
def Job(id):
    print("Saving Job for Id", id, time.ctime(time.time()))

#FLASK ROUTING FUNCTION TO STORE A NEW JOB ACCORDING TO THE PARAMS SEND
@app.route('/schedule', methods=['POST', 'GET'])
def getJobSchedule():

    if request.method == 'POST':
        try:
            data = request.json['Input']['Data']
            schedule = request.json['Schedule']

            print(schedule)

            # PROCESS THE SCHEDULE STRING ACCORDING TO APS CRON TRIGGER
            year = None
            month = None
            day = None
            week = None
            day_of_week = None
            hour = None
            minute = None
            second = None
            start_date = None
            end_date = None
            timezone = None
            jitter = None

            if 'year' in schedule:
                year = schedule['year']
            if 'month' in schedule:
                month = schedule['month']
            if 'day' in schedule:
                day = schedule['day']
            if 'day_of_week' in schedule:
                day_of_week = schedule['day_of_week']
            if 'hour' in schedule:
                hour = schedule['hour']
            if 'minute' in schedule:
                minute = schedule['minute']
            if 'second' in schedule:
                second = schedule['second']
            if 'start_date' in schedule:
                start_date = schedule['start_date']
                if len(start_date) > 10:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                else:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')

            if 'end_date' in schedule:
                end_date = schedule['end_date']
                if len(end_date) > 10:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d  %H:%M:%S')
                else:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

            if 'timezone' in schedule:
                timezone = schedule['timezone']
            if 'jitter' in schedule:
                jitter = schedule['jitter']

            # adding job to the scheduler
            jobId = scheduler.add_job(Job, 'cron', year=year, month=month, week=week, day=day, day_of_week=day_of_week, hour=hour, minute=minute,
                                      second=second, start_date=start_date, end_date=end_date, timezone=timezone, jitter=jitter, args=['startDate_enddate'])
            print(jobId.id)

            return jsonify({'Input': data, "Id": jobId.id})
        except Exception as e:
            print(e)
            return jsonify({})


# RUN THE FLASK APP
if __name__ == '__main__':

    print(scheduler.get_jobs())
    app.run(debug=True)
