import json
import xmltodict
import boto3
import os

AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

if 'AWS_SESSION_TOKEN' in os.environ:
	AWS_SESSION = os.environ['AWS_SESSION_TOKEN']

majorPartyCodes = [
	'ALP', 
	'CLP', 
	'CP', 
	'FLP', 
	'LCL', 
	'LCP', 
	'LIB', 
	'LNQ',
	'LP',
	'NAT',
	'NCP',
	'NP',
	'LNP'
]

majorVote2019 = 74.78
minorVote2019 = 25.22

majorVote2022 = 68.28
minorVote2022 = 31.72

with open('historic-data/electorate_votes_2019.json', 'r') as f:
	historicData2019 = json.load(f)

with open('historic-data/electorate_votes_2022.json', 'r') as f:
	historicData2022 = json.load(f)

def convertPartyCode(partycode):
	partyCodes = {'LP':'LIB', 'NP':'NAT'}
	if partycode in partyCodes:
		return partyCodes[partycode]
	else:
		return partycode	

def candidate_party(candidate,candidateType):
	if candidateType == 'short':
		if 'eml:AffiliationIdentifier' in candidate:
			return candidate['eml:AffiliationIdentifier']['@ShortCode']
		else:
			return 'IND'
	if candidateType == 'long':
		if 'eml:AffiliationIdentifier' in candidate:
			return candidate['eml:AffiliationIdentifier']['eml:RegisteredName']
		else:
			return 'Independent'

def eml_to_JSON(eml_file, type, local, timestamp, uploadPath, upload, electionID):
	
	# Set some variables

	# It's the 2025 election

	if electionID == '31496':
		print("Using 2022 variables")
		historicData = historicData2022
		majorVote = majorVote2022
		minorVote = minorVote2022

	elif electionID == '27966':
		print("Using 2019 variables")
		historicData = historicData2019
		majorVote = majorVote2019
		minorVote = minorVote2019

	#convert xml to json
	
	if local:
		elect_data = xmltodict.parse(open(eml_file, 'rb'))
	else:
		elect_data = xmltodict.parse(eml_file)	
	
	if type == "media feed":
	  
		#parse house of reps
		results_json = {}
		summary_json = {}
		swing_list = []
		electorates_list = []

		for election in elect_data['MediaFeed']['Results']['Election']:
			# House of Representative contests
			
			if 'House' in election:
				# National summary
				results_json['enrollment'] = int(election['House']['Analysis']['National']['Enrolment'])
				results_json['votesCountedPercent'] = float(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['@Percentage'])
				results_json['votesCounted'] = int(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['#text'])




				print("Votes counted", results_json['votesCounted'], "percent", results_json['votesCountedPercent'])

				natSwing = election['House']['Analysis']['National']['TwoPartyPreferred']

				results_json['nationalSwing'] = {}

				for coalition in election['House']['Analysis']['National']['TwoPartyPreferred']['Coalition']:
					print(coalition)
					if coalition['CoalitionIdentifier']['@ShortCode'] == "LNC":
						results_json['nationalSwing']['tppCoalition'] = float(coalition['Votes']['@Swing'])
					if coalition['CoalitionIdentifier']['@ShortCode'] == "ALP":
						results_json['nationalSwing']['tppLabor'] = float(coalition['Votes']['@Swing'])	

				print(election['House']['Analysis']['National']['TwoPartyPreferred']['Coalition'])
				partyNational = election['House']['Analysis']['National']['FirstPreferences']['PartyGroup']
					
				results_json['partyNationalResults'] = [
					{
						'partygroup_id': int(partygroup['PartyGroupIdentifier']['@Id']),
						'partygroup_name': partygroup['PartyGroupIdentifier']['PartyGroupName'],
						'coalition_short': partygroup['PartyGroupIdentifier']['@ShortCode'],
						'votesTotal': int(partygroup['Votes']['#text']),
						'votesPercent': float(partygroup['Votes']['@Percentage']),
						'swing':float(partygroup['Votes']['@Swing'])
					}
					for partygroup in partyNational
				]				

				# Get the national major -> Independent/minor swing

				major_national_new = 0
				minor_national_new = 0
				total_new = 0

				for partygroup in results_json['partyNationalResults']:
					# Check if major party
					if partygroup['coalition_short'] in majorPartyCodes:
						major_national_new += partygroup['votesTotal']
					else:
						minor_national_new += partygroup['votesTotal']

				total_new = major_national_new + minor_national_new
				print("major_national_new: ", major_national_new)
				print("minor_national_new: ", minor_national_new)
				print("total_new: ", total_new)

				if major_national_new > 0:
					major_national_pct = round(major_national_new / total_new * 100,2)
					minor_national_pct = round(minor_national_new / total_new * 100,2)


					print("major_national_pct: ", major_national_pct)
					print("minor_national_pct: ", minor_national_pct)

					major_national_swing = major_national_pct - majorVote
					minor_national_swing = minor_national_pct - minorVote
					print("major_national_swing: ", round(major_national_swing,2))
					print("minor_national_swing: ", round(minor_national_swing,2))
				else:
					major_national_swing = 0
					minor_national_swing = 0

				results_json['nationalSwing']['toMajor'] = round(major_national_swing,2)
				results_json['nationalSwing']['toMinor'] = round(minor_national_swing,2)

				summary_json['enrollment'] = int(election['House']['Analysis']['National']['Enrolment'])
				summary_json['votesCountedPercent'] = float(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['@Percentage'])
				summary_json['votesCounted'] = int(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['#text'])

				# Division summaries

				for contest in election['House']['Contests']['Contest']:

					# print("#######################")
					# print(contest['PollingDistrictIdentifier']['Name'])
				
					currentElectorate = contest['PollingDistrictIdentifier']['Name']

					# The object for the individual electorate JSON
					electorates_json = {}

					# The object for the swing JSON
					swing_json = {}


					electorates_json['id'] = int(contest['PollingDistrictIdentifier']['@Id'])
					swing_json['id'] = electorates_json['id']
					electorates_json['name'] = contest['PollingDistrictIdentifier']['Name']
					swing_json['name'] = electorates_json['name']
					
					electorates_json['state'] = contest['PollingDistrictIdentifier']['StateIdentifier']['@Id']
					swing_json['state'] = electorates_json['state']

					electorates_json['enrollment'] = int(contest['Enrolment']['#text'])
					electorates_json['votesCounted'] = int(contest['FirstPreferences']['Total']['Votes']['#text'])

					electorates_json['tcpCountProgress'] = f"{contest['TwoCandidatePreferred']['@PollingPlacesReturned']} / {contest['TwoCandidatePreferred']['@PollingPlacesExpected']}"

					candidates = contest['FirstPreferences']['Candidate']
					electorates_json['candidates'] = [
						{
							'candidate_id': int(candidate['eml:CandidateIdentifier']['@Id']),
							'candidate_name': candidate['eml:CandidateIdentifier']['eml:CandidateName'],
							'votesTotal': int(candidate['Votes']['#text']),
							'votesPercent': float(candidate['Votes']['@Percentage']),
							'votesHistoric': int(candidate['Votes']['@Historic']),
							'party_short': convertPartyCode(candidate_party(candidate,'short')),
							'party_long':candidate_party(candidate,'long'),
							'incumbent':candidate['Incumbent']['#text']
						}
						for candidate in candidates
					]
					# print contest['TwoCandidatePreferred']

					if "@Restricted" not in contest['TwoCandidatePreferred'] and "@Maverick" not in contest['TwoCandidatePreferred']:
						twoCandidatePreferred = contest['TwoCandidatePreferred']['Candidate']
						electorates_json['twoCandidatePreferred'] = [
							{
								'candidate_id': int(candidate['eml:CandidateIdentifier']['@Id']),
								'candidate_name': candidate['eml:CandidateIdentifier']['eml:CandidateName'],
								'votesTotal': int(candidate['Votes']['#text']),
								'votesPercent': float(candidate['Votes']['@Percentage']),
								'swing':float(candidate['Votes']['@Swing']),
								'party_short': convertPartyCode(candidate_party(candidate,'short')),
								'party_long':candidate_party(candidate,'long')
							}
							for candidate in twoCandidatePreferred
						]
						swing_json['tcp'] = electorates_json['twoCandidatePreferred']

					elif "@Restricted" in contest['TwoCandidatePreferred']:
						electorates_json['twoCandidatePreferred'] = "Restricted"
						swing_json['tcp'] = electorates_json['twoCandidatePreferred']

					elif "@Maverick" in contest['TwoCandidatePreferred']:
						electorates_json['twoCandidatePreferred'] = "Maverick"
						swing_json['tcp'] = electorates_json['twoCandidatePreferred']						

					twoPartyPreferred = contest['TwoPartyPreferred']['Coalition']
					
					electorates_json['twoPartyPreferred'] = [
						{
							'coalition_id': int(coalition['CoalitionIdentifier']['@Id']),
							'coalition_long': coalition['CoalitionIdentifier']['CoalitionName'],
							'coalition_short': coalition['CoalitionIdentifier']['@ShortCode'],
							'votesTotal': int(coalition['Votes']['#text']),
							'votesPercent': float(coalition['Votes']['@Percentage']),
							'swing':float(coalition['Votes']['@Swing'])
						}
						for coalition in twoPartyPreferred
					]		

					# AEC sometimes reports swing without votes? Not sure why	

					# print(electorates_json['twoPartyPreferred'])

					# This needs to be fixed next election as the AEC can change the order

					if electorates_json['twoPartyPreferred'][0]['votesTotal'] == 0 and electorates_json['twoPartyPreferred'][1]['votesTotal'] == 0:
						swing_json['tppCoalition'] = 0
						swing_json['tppLabor'] = 0
					else:	
						swing_json['tppCoalition'] = electorates_json['twoPartyPreferred'][1]['swing']
						swing_json['tppLabor'] = electorates_json['twoPartyPreferred'][0]['swing']

					# Calculate the major party - Independent/minor party swing

					if currentElectorate in historicData:
						major_party_last_election = historicData[currentElectorate]["Major"]
						minor_ind_last_election = historicData[currentElectorate]["Indie_and_Minor"]
						total_last_election = major_party_last_election + minor_ind_last_election

						major_party_this_election = 0
						minor_ind_this_election = 0
						total_this_election = 0

						# print(electorates_json['candidates'])

						for candidate in electorates_json['candidates']:
							# Check if major party

							if candidate['party_short'] in majorPartyCodes:
								major_party_this_election = major_party_this_election + candidate['votesTotal']
		
							# Else minor or independent
							else:
								minor_ind_this_election = minor_ind_this_election + candidate['votesTotal']


						total_this_election = major_party_this_election + minor_ind_this_election

						major_party_last_election_pct = major_party_last_election/total_last_election * 100
						
						# Check if there are votes yet

						if total_this_election > 0:
							
							major_party_this_election_pct = major_party_this_election/total_this_election * 100
							major_party_swing = major_party_this_election_pct - major_party_last_election_pct

						# No votes, set to zero

						else:
							major_party_this_election_pct = 0
							major_party_swing = 0

						# print("major_party_this_election", major_party_this_election,"major_party_last_election",major_party_last_election)
						# print("minor_party_this_election", minor_ind_this_election,"minor_party_last_election",minor_ind_last_election)
						# print("total_this_election", total_this_election,"total_last_election",total_last_election)

						# print("major_party_last_election_pct", major_party_last_election_pct, "%")
						# print("major_party_this_election_pct", major_party_this_election_pct,"%")
						# print("major_party_swing", major_party_swing,"%")

						swing_json['toMajor'] = round(major_party_swing,2)
						swing_json['toMinor'] = round((-1 * major_party_swing),2)
					else:
						swing_json['toMajor'] = 0
						swing_json['toMinor'] = 0

					electorates_list.append(electorates_json)
					swing_list.append(swing_json)			

				# print electorates_list
				results_json['divisions'] = electorates_list

			if 'Senate' in election:
				pass


		newJson = json.dumps(results_json, indent=4)
		summaryJson = json.dumps(summary_json, indent=4)
		swingJson = json.dumps(swing_list, indent=4)

		# Save the file locally

		with open('results/{timestamp}.json'.format(timestamp=timestamp),'w') as fileOut:
			print(f"saving {timestamp}.json locally")
			fileOut.write(newJson)	

		with open('results/{timestamp}-swing.json'.format(timestamp=timestamp),'w') as fileOut:
			print(f"saving {timestamp}-swing.json locally")
			fileOut.write(swingJson)		

		with open('summaryResults.json','w') as fileOut:
			print("saving summaryResults.json locally")
			fileOut.write(summaryJson)		

		# Upload the results to S3		

		if upload:
			print("Connecting to S3")

			bucket = 'gdn-cdn'

			if 'AWS_SESSION_TOKEN' in os.environ:
				session = boto3.Session(
				aws_access_key_id=AWS_KEY,
				aws_secret_access_key=AWS_SECRET,
				aws_session_token = AWS_SESSION
				)
			else:
				session = boto3.Session(
				aws_access_key_id=AWS_KEY,
				aws_secret_access_key=AWS_SECRET,
				)
			
			s3 = session.resource('s3')	
			
			key = f"{uploadPath}/{timestamp}.json"
			object = s3.Object(bucket, key)
			object.put(Body=newJson, CacheControl="max-age=60", ACL='public-read', ContentType="application/json")
			print("Uploaded:", f"https://interactive.guim.co.uk/{key}")

			key2 = f"{uploadPath}/summaryResults.json"	
			object = s3.Object(bucket, key2)
			object.put(Body=summaryJson, CacheControl="max-age=60", ACL='public-read', ContentType="application/json")
			print("Uploaded:", f"https://interactive.guim.co.uk/{key2}")

			key3 = f"{uploadPath}/{timestamp}-swing.json"
			object = s3.Object(bucket, key3)
			object.put(Body=swingJson, CacheControl="max-age=60", ACL='public-read', ContentType="application/json")
			print("Uploaded:", f"https://interactive.guim.co.uk/{key3}")

			print("Done, JSON is uploaded")

		else:
			print("Results not uploaded, uploading switched off")

# eml_to_JSON('aec-mediafeed-results-standard-verbose-24310.xml','media feed',True,'20190726164221', True)	
# def eml_to_JSON(eml_file, type, local, timestamp, uploadPath, upload):
# aec-mediafeed-results-standard-verbose-27966-end.xml 20220719103300
if __name__ == "__main__":
	print("Running parser")
	eml_to_JSON(eml_file='sample-data/aec-mediafeed-results-standard-verbose-27966-end.xml',
			 type='media feed',
			 local=True,
			 timestamp='20220719103300', 
			 uploadPath="2025/05/aus-election/results-data-test",
			 upload=False)	