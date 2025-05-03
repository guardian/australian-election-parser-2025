import schedule
import time
from burnelectorates import burnElectorates
from datetime import datetime

resultsTest = False

# True if we want to upload results to s3 on election night, false for local testing

upload = True

if not upload:
	print("WARNING: UPLOADING SWITCHED OFF")

# True for election night, false if you want to upload to results-data-test

uploadToProd = True

if not uploadToProd:
	print("WARNING: UPLOADING TO TEST DIRECTORY")

# The first part of the s3 upload path
uploadPath = "2025/05/aus-election/results-data"

if not uploadToProd:
	uploadPath = "2025/05/aus-election/results-data-test"

def doStuff():
	burnElectorates(uploadPath=uploadPath, uploadElectorates=upload)
# Use scheduler to time function every 2 minutes

if not resultsTest:
    doStuff()
    schedule.every(4).minutes.do(doStuff)

    while True:
        schedule.run_pending()
        time.sleep(1)
        print(datetime.now())


