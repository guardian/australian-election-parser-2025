import schedule
import time
from burnelectorates import burnElectorates
from datetime import datetime

resultsTest = False
def doStuff():
	burnElectorates()
# Use scheduler to time function every 2 minutes

if not resultsTest:
    doStuff()
    schedule.every(4).minutes.do(doStuff)

    while True:
        schedule.run_pending()
        time.sleep(1)
        print(datetime.now())


