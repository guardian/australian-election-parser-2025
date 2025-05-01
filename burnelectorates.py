from utils import get_json, get_local_json, get_latest_feed, upload_to_s3, save_to_file
import requests
from datetime import datetime
from typing import Dict, List
import json

googledoc_key = "1b6VI5L_olM-zpSLy2WkHQ7OXIysQ2KFOd-v5SztfoM0"
startTime = datetime.now()
# uploadElectorates = True

# config = {
#     "title": "",
#     "docData": "",
#     "path": "embed/aus/2025/02/aus-election/results-data"
# }

def select_electorate(id: str, electorate: str, results: Dict, divisions: Dict, swing: List[Dict], parties: Dict) -> Dict:
    swing_time = True
    
    party_names_map = {
        "Australian Labor Party": "Labor",
        "Liberal National Party of Queensland": "LNP",
        "The Nationals": "National",
        "The Greens (VIC)": "Greens",
        "Pauline Hanson's One Nation": "One Nation",
        "Independent": "Independent",
        "United Australia Party": "UAP",
        "Katter's Australian Party (KAP)": "Katter Party",
        "Centre Alliance": "Centre Alliance"
    }

    def party_names(party):
        return party_names_map.get(party, party)

    print(f"Selected electorate: {electorate}")
    result = results.get(electorate)
    aec_result = divisions.get(electorate)

    if not aec_result:
        return {}

    candidates = sorted(aec_result['candidates'], key=lambda x: x['votesTotal'], reverse=True)
    
    candidate_swing_data = next((item['tcp'] for item in swing if item['name'] == electorate), None)

    swing_info = {'status': False}

    if candidate_swing_data and len(candidate_swing_data) == 2 and swing_time:
        if candidate_swing_data[0]['swing'] < candidate_swing_data[1]['swing']:
            candidate_swing_data.append(candidate_swing_data.pop(0))

        if candidate_swing_data[1]['swing'] > 0:
            swing_info = {'status': False}
        else:
            multiplier = 3.333 if candidate_swing_data[0]['swing'] < 15 else 2 if candidate_swing_data[0]['swing'] < 25 else 1
            label = 15 if candidate_swing_data[0]['swing'] < 15 else 25 if candidate_swing_data[0]['swing'] < 25 else 50

            swing_info = {
                'status': True,
                'text': f"{candidate_swing_data[0]['swing']}% swing to {party_names(candidate_swing_data[0]['party_long'])}",
                'label': label,
                'swingLeft': candidate_swing_data[0]['swing'],
                'swingRight': candidate_swing_data[1]['swing'],
                'swingPartyLeft': party_names(candidate_swing_data[0]['party_long']),
                'swingPartyRight': party_names(candidate_swing_data[1]['party_long']),
                'swingLeftBar': candidate_swing_data[0]['swing'] * multiplier,
                'swingRightBar': candidate_swing_data[1]['swing'],
                'swingLeftShort': candidate_swing_data[0]['party_short'].lower(),
                'swingRightShort': candidate_swing_data[1]['party_short'].lower()
            }

    hide_two_party = True
    two_party = None
    name_field = None

    if isinstance(aec_result.get('twoCandidatePreferred'), list):
        two_party = sorted(aec_result['twoCandidatePreferred'], key=lambda x: x['votesTotal'], reverse=True)
        name_field = 'party_long'
        hide_two_party = False

    prediction = result['prediction'].lower() if result['prediction'] else ''
    prediction_name = parties[prediction]['partyName'] if prediction else ''

    info = {
        'display': True,
        'id': id,
        'electorate': electorate,
        'candidates': candidates,
        'hideTwoParty': hide_two_party,
        'twoParty': two_party,
        'nameField': name_field,
        'prediction': prediction,
        'status': bool(prediction),
        'predictionName': prediction_name,
        'heldBy': result['incumbent'].lower(),
        'heldByName': parties[result['incumbent'].lower()]['partyName'],
        'percentageCounted': round((aec_result['votesCounted'] / aec_result['enrollment']) * 100, 1),
        'swigInfo': swing_info
    }

    return info

def burnElectorates(uploadPath="2025/05/aus-election/results-data", uploadElectorates=True):
    # Fetch Google doc data
    
    googledoc = requests.get(f"https://interactive.guim.co.uk/docsdata/{googledoc_key}.json").json()['sheets']
    print(f"Latest googledoc: https://interactive.guim.co.uk/docsdata/{googledoc_key}.json")

    # Get latest feed data from the local file
    latest = get_local_json("recentResults.json")

    # Most recent timestamp from list
    latest_feed = get_latest_feed(latest)

    # Most recent results file
    latest_data = get_local_json(f"results/{latest_feed}.json")
    
    # Get summary results
    summary_results = get_local_json("summaryResults.json")
    
    # Get swing data
    swing = get_local_json(f"results/{latest_feed}-swing.json")
    
    # Create data maps
    electorates_map = {item['electorate']: item for item in googledoc['electorates']}
    places = [item['electorate'] for item in googledoc['electorates']]
    divisions = {item['name']: item for item in latest_data['divisions']}

    # Custom party names from the Google Doc
    
    parties = {item['partyCode'].lower(): item for item in googledoc['partyNames']}
    
    googledoc['parties'] = parties
    
    print(latest_data['nationalSwing'])
    
    # Process electorates

    electorates_data = googledoc['electorates']

    if uploadElectorates:
        for item in electorates_data:
            info = select_electorate(item['id'], item['electorate'], electorates_map, divisions, swing, parties)
            if info.get('twoParty'):
                item['byMargin'] = info['twoParty'][0]['swing']
            
            electorate_info = json.dumps(info).encode()
            upload_to_s3(f"{uploadPath}/electorates/{item['id']}.json", electorate_info)
    
    timeRun = datetime.now() - startTime
    print(f"Finished in {timeRun}")

if __name__ == "__main__":
    burnElectorates()