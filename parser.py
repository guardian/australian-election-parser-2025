import json
from ftplib import FTP
import os
from zipfile import ZipFile
from io import StringIO
from io import BytesIO
from datetime import datetime
from datetime import timedelta
import emlparse
import logresults
import schedule
import time
from feedburner import burnfeeds

# the 2025 election ID is 31496
# the 2022 election ID for testing is 27966
# Make sure it is correct on the night

electionID = '31496'

if electionID != '31496':
	print("WARNING: USING OLD ELECTION ID")

# Set to mediafeed on election night
# ftpPath = 'mediafeedarchive.aec.gov.au'

ftpPath = 'mediafeed.aec.gov.au'	

if ftpPath != 'mediafeed.aec.gov.au':
	print("WARNING: USING THE MEDIA ARCHIVE")

# Set to true if you want more console output
verbose = False

# Loops through old election data to simulate results night
resultsTest = False

if resultsTest:
	print("WARNING: RUNNING A TEST USING OLD RESULTS")

# True if we want to upload results to s3 on election night, false for local testing
upload = True

if not upload:
	print("WARNING: UPLOADING SWITCHED OFF")

# Determines the upload path, True for election night, false if you want to upload to results-data-test

uploadToProd = False

if not uploadToProd:
	print("WARNING: UPLOADING TO TEST DIRECTORY")

# The first part of the s3 upload path
uploadPath = "2025/05/aus-election/results-data"

if not uploadToProd:
	uploadPath = "2025/05/aus-election/results-data-test"

# If we're running a test, the starting time of the test. Usually start 6pm on an election night

testTime = datetime.strptime("2022-05-21 18:00","%Y-%m-%d %H:%M")

# Make a folder for all the results files if it doesn't exist yet

os.makedirs("results", exist_ok=True) 

def parse_results(test):

	path = '/{electionID}/Standard/Verbose/'.format(electionID=electionID)
	print("Logging in to AEC FTP using", ftpPath)

	print(ftpPath)
	ftp = FTP(ftpPath)
	ftp.login()

	# print("yep")
	ftp.cwd(path)

	aec_files = []

	def get_filenames(ln):
		# global aec_files
		cols = ln.split(' ')
		objname = cols[len(cols)-1] # file or directory name
		if objname.endswith('.zip'):
			aec_files.append(objname) # full path

	print("Getting all the filenames")

	ftp.retrlines('LIST', get_filenames)

	# try:
	# 	ftp.retrlines('LIST', get_filenames)
	
	# except BrokenPipeError as e:
	# 	print(e)
	# 	print("Can't reach the AEC server, retrying in 20 seconds")
	# 	time.sleep(20)
	# 	ftp = FTP(ftpPath)
	# 	ftp.login()
	# 	ftp.cwd(path)
	# 	ftp.retrlines('LIST', get_filenames)

	timestamps = []

	if verbose:
		print(aec_files)

	#Get latest timestamp

	print("Getting latest timestamp")

	for f in aec_files:
		timestamp = f.split("-")[-1].replace(".zip","")

		if test:
			if datetime.strptime(timestamp,"%Y%m%d%H%M%S") < testTime:
				# print("test time is ", testTime)
				if verbose:
					print(timestamp)
				timestamps.append(datetime.strptime(timestamp,"%Y%m%d%H%M%S"))
		else:
			if verbose:
				print(timestamp)

			timestamps.append(datetime.strptime(timestamp,"%Y%m%d%H%M%S"))

	latestTimestamp = max(timestamps)
	latestTimestampStr = datetime.strftime(latestTimestamp, '%Y%m%d%H%M%S')

	print("latest timestamp is", latestTimestamp)

	# Check if results log exists

	if os.path.exists('recentResults.json'):

		# Get recent timestamps of results

		with open('recentResults.json','r') as recentResultsFile:
			recentResults = json.load(recentResultsFile)

		print(recentResults)

		# Check if we have it or not

		if latestTimestampStr not in recentResults:
			
			print("{timestamp} hasn't been saved, saving now".format(timestamp=latestTimestampStr))
			
			# Get latest file

			latestFile = "aec-mediafeed-Standard-Verbose-{electionID}-{timestamp}.zip".format(electionID=electionID,timestamp=datetime.strftime(latestTimestamp, '%Y%m%d%H%M%S'))
			r = BytesIO()

			print('Getting ' + latestFile)

			# Get file, read into memory

			ftp.retrbinary('RETR ' + latestFile, r.write)
			input_zip=ZipFile(r, 'r')
			ex_file = input_zip.open("xml/aec-mediafeed-results-standard-verbose-" + electionID + ".xml")
			content = ex_file.read()
			
			# print content

			print("Parsing the feed into JSON")

			# The function that actually parses all the XML into JSON
	
			emlparse.eml_to_JSON(eml_file=content,
						type='media feed',
						local=False, 
						timestamp=latestTimestampStr,
						uploadPath=uploadPath,
						upload=upload)
			
			logresults.saveRecentResults(timestamp=latestTimestampStr, 
								uploadPath=uploadPath,
								upload=upload)

		if latestTimestampStr in recentResults:
			print("{timestamp} has already been saved".format(timestamp=latestTimestampStr))

	# It doesn't exist, so treat timestamp as first

	else:
		print("Results file not found, saving {timestamp} as first entry".format(timestamp=latestTimestampStr))
			
		# Get latest file

		latestFile = "aec-mediafeed-Standard-Verbose-{electionID}-{timestamp}.zip".format(electionID=electionID,timestamp=datetime.strftime(latestTimestamp, '%Y%m%d%H%M%S'))
		r = BytesIO()

		print('Getting ' + latestFile)

		# Get file, read into memory

		ftp.retrbinary('RETR ' + latestFile, r.write)
		input_zip = ZipFile(r, 'r')
		ex_file = input_zip.open("xml/aec-mediafeed-results-standard-verbose-" + electionID + ".xml")
		content = ex_file.read()
		
		# print content

		print("Parsing the feed into JSON")

		emlparse.eml_to_JSON(eml_file=content,
						type='media feed',
						local=False, 
						timestamp=latestTimestampStr,
						uploadPath=uploadPath,
						upload=upload)
			
		logresults.saveRecentResults(timestamp=latestTimestampStr, 
								uploadPath=uploadPath,
								upload=upload)

	print("Done, results all saved")
	ftp.quit()
	burnfeeds(uploadPath=uploadPath)

# Use scheduler to time function every 2 minutes

if not resultsTest:

	parse_results(resultsTest)

	schedule.every(1).minutes.do(parse_results,resultsTest)

	while True:
		schedule.run_pending()
		time.sleep(1)
		print(datetime.now())

# Test function, counts from 6 pm to 11 pm on election night 2013    

elif resultsTest:

	def runTest():
		global testTime
		endTime = datetime.strptime("2022-05-23 23:00","%Y-%m-%d %H:%M")
		parse_results(resultsTest)
		schedule.every(1).minutes.do(parse_results,True)
		
		while testTime < endTime:
			schedule.run_pending()
			testTime = testTime + timedelta(minutes=1)
			print(testTime)
			time.sleep(1)

	runTest()

# parse_results(True)
# ftp.quit()

