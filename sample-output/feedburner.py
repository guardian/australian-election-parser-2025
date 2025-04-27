import boto3
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Union
import math

config = {
    "title": "",
    "docData": "",
    "path": "embed/aus/2025/02/aus-election/results-data"
}

googledoc_key = "11WneZFp0CwnkDiBtQTTH_y-OjSzAlZR4c6rCPrQ_baA"

url = "https://interactive.guim.co.uk/2022/05/aus-election/results-data"

COALITION = ['lib', 'lnp', 'nat', 'clp']
SENATE = ['lib', 'nat', 'on', 'pending', 'other', 'alp', 'grn', 'ca']

def get_alignment(key: str) -> str:
    left_aligned = ["alp", "grn"]
    right_aligned = ["lib", "lnp", "nat", "ind", "kap", "ca"]
    
    key = key.lower()
    if key in left_aligned:
        return "left"
    elif key in right_aligned or key not in left_aligned:
        return "right"

def compile(electorates: List[Dict], parties: Dict, summary_results: Dict, national_swing: float) -> Dict:
    # Calculate tallies
    tallies = {}
    for electorate in electorates:
        party = electorate['prediction'].lower()
        if party:
            tallies[party] = tallies.get(party, 0) + 1

    party_data = []
    for party, value in tallies.items():
        if party:
            party_info = parties.get(party)
            if party_info:
                party_data.append({
                    'key': party,
                    'value': value,
                    'name': party_info['partyName'],
                    'shortName': party_info['shortName'],
                    'alignment': get_alignment(party)
                })

    # Sort party data by value
    party_data.sort(key=lambda x: x['value'], reverse=True)
    list_size = math.ceil(len(party_data) / 2)

    # Calculate Labor and Coalition seats
    lab_seats = next((p['value'] for p in party_data if p['key'] == 'alp'), 0)
    coalition_seats = sum(p['value'] for p in party_data if p['key'] in ['lib', 'lnp', 'nat', 'clp'])

    lab_percentage = (lab_seats / 151) * 100
    coalition_percentage = (coalition_seats / 151) * 100

    party_data_2pp = [
        {'name': 'Coalition', 'seats': coalition_seats, 'percentage': coalition_percentage, 'party': 'coal'},
        {'name': 'Labor', 'seats': lab_seats, 'percentage': lab_percentage, 'party': 'alp'}
    ]

    render_data = {
        'TOTAL_SEATS': 151,
        'MAJORITY_SEATS': 76,
        'partyData': party_data,
        'resultCount': len(tallies),
        'partyListLeft': party_data[:list_size],
        'header1': 'seats' if party_data else '',
        'partyListRight': party_data[list_size:],
        'header2': 'seats' if len(party_data) > 1 else '',
        'updated': datetime.now().isoformat(),
        'votesCountedPercent': summary_results['votesCountedPercent'],
        'nationalSwing': national_swing,
        'displaySwing': True
    }

    render_data['twoParty'] = party_data_2pp
    
    print(f"{summary_results['votesCountedPercent']}% of Australia has voted.")
    
    return render_data

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
    swig_info = {'status': False}

    if candidate_swing_data and len(candidate_swing_data) == 2 and swing_time:
        if candidate_swing_data[0]['swing'] < candidate_swing_data[1]['swing']:
            candidate_swing_data.append(candidate_swing_data.pop(0))

        if candidate_swing_data[1]['swing'] > 0:
            swig_info = {'status': False}
        else:
            multiplier = 3.333 if candidate_swing_data[0]['swing'] < 15 else 2 if candidate_swing_data[0]['swing'] < 25 else 1
            label = 15 if candidate_swing_data[0]['swing'] < 15 else 25 if candidate_swing_data[0]['swing'] < 25 else 50

            swig_info = {
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
        'swigInfo': swig_info
    }

    return info

def get_json(feed_url: str) -> Dict:
    response = requests.get(feed_url)
    return response.json()

def get_latest_feed(latest: List[str]) -> int:
    if not latest:
        raise ValueError("getLatestFeed expects a non-empty array")
    return max(map(int, latest))

def create_table(data: List[Dict], party_map: List[Dict], swing: bool) -> List[Dict]:
    # Build party map lookup
    p_map = {item['partyCode'].lower(): item for item in party_map}
    
    # Build initial table data
    table_data = []
    for item in data:
        base = {
            'Party': item['partygroup_name'],
            'Votes (%)': float(item['votesPercent']),
            'Total votes': float(item['votesTotal']),
            'Short': item['coalition_short'].lower()
        }
        if swing:
            base['Swing'] = float(item['swing'])
        table_data.append(base)

    # Process coalition items
    coalition = ["Liberal", "Liberal National Party of Queensland", "The Nationals", "Country Liberal Party (NT)"]
    coalition_items = [item for item in table_data if item['Party'] in coalition]
    
    coalition_summary = {
        'Party': 'Coalition',
        'Votes (%)': sum(item['Votes (%)'] for item in coalition_items),
        'Total votes': sum(item['Total votes'] for item in coalition_items)
    }
    
    if swing:
        coalition_summary['Swing'] = coalition_summary['Votes (%)'] - 41.44

    others = [
        "Australian Labor Party", "The Greens", "Independent",
        "Pauline Hanson's One Nation", "United Australia Party",
        "Katter's Australian Party (KAP)", "Centre Alliance"
    ]
    
    result = [item for item in table_data if item['Party'] in others]

    # Update party names
    for item in result:
        party_info = p_map.get(item['Short'])
        if party_info:
            item['Party'] = party_info['partyName']

    # Keep only desired properties
    result = [{k: v for k, v in item.items() if k in ['Party', 'Votes (%)', 'Total votes', 'Swing']} 
              for item in result]

    result.append(coalition_summary)
    result.sort(key=lambda x: x['Total votes'], reverse=True)

    # Format numeric values
    for record in result:
        record['Votes (%)'] = round(record['Votes (%)'], 2)
        if 'Swing' in record:
            record['Swing'] = round(record['Swing'], 2)

    return result

def create_ticker_feed(data: Dict) -> List[Dict]:
    predictions = [d for d in data['electorates'] if d['prediction']]
    party_map = {item['partyCode']: item for item in data['partyNames']}
    
    for prediction in predictions:
        prediction['announced'] = "Predicted"
        prediction['status'] = "hold" if prediction['prediction'] == prediction['incumbent'] else "wins"
        prediction['label'] = (party_map.get(prediction['prediction'], {}).get('partyName', '')
                             if prediction['prediction'] != "IND"
                             else prediction['prediction-name'])

    with_timestamp = []
    without_timestamp = []

    for item in predictions:
        if item['timestamp']:
            item['unix'] = int(datetime.fromisoformat(item['timestamp']).timestamp())
            with_timestamp.append(item)
        else:
            without_timestamp.append(item)

    with_timestamp.sort(key=lambda x: x['unix'], reverse=True)
    return (with_timestamp + without_timestamp)[:15]

def upload_to_s3(dest: str, buffer: bytes, content_type: str = 'application/json'):
    s3 = boto3.client('s3')
    try:
        s3.put_object(
            Bucket="gdn-cdn",
            Key=dest,
            Body=buffer,
            ContentType=content_type,
            ACL='public-read',
            CacheControl="max-age=30"
        )
        print(f"https://interactive.guim.co.uk/{dest}")
    except Exception as err:
        print(f"Error: {err}")

def senate_render(data: Dict) -> Dict:
    # Set up constants
    key = 'senatefull'
    party_field = 'party'
    total_seats = 76

    # Filter out records with an empty party field
    has_data = [d for d in data[key] if d[party_field]]

    # Tally each party's count (using a case-insensitive party key)
    party_data = []
    party_counts = {}
    for curr in has_data:
        party_key = curr[party_field].lower()
        if party_key not in party_counts:
            party_counts[party_key] = {'key': party_key, 'value': 0}
            party_data.append(party_counts[party_key])
        party_counts[party_key]['value'] += 1

    # Enhance partyData with additional properties
    for d in party_data:
        current_items = [item for item in has_data 
                        if item[party_field].lower() == d['key'] 
                        and item.get('current') == 'yes']
        
        party_info = data['parties'].get(d['key'])
        d['name'] = party_info['partyName'] if party_info else d['key']
        d['shortName'] = party_info['shortName'] if party_info else d['key']
        d['current'] = len(current_items)
        d['elected'] = d['value'] - d['current']

    # Create a lookup map for party data
    party_map = {item['key']: item for item in party_data}

    # Build senateData based on the global SENATE array
    senate_data = []
    for d in SENATE:
        party = party_map.get(d)
        seats = party['value'] if party else 0
        current = party['current'] if party else 0
        elected = party['elected'] if party else 0

        senate_data.append({
            'key': d,
            'name': party['name'] if party else d,
            'shortName': party['shortName'] if party else d,
            'value': seats,
            'current': current,
            'elected': elected,
            'seats': seats > 0,
            'currentSeats': current > 0,
            'electedSeats': elected > 0,
            'percentage': (seats / total_seats) * 100,
            'currentPercentage': (current / seats * 100) if seats else 0,
            'electedPercentage': (elected / seats * 100) if seats else 0,
            'notpending': d != 'pending'
        })

    # Create a Map from senateData for quick lookup
    senate_map = {item['key']: item for item in senate_data}

    # Define breakdown fields
    breakdowns = [
        {'value': 'value', 'seats': 'seats', 'percentage': 'percentage'},
        {'value': 'current', 'seats': 'currentSeats', 'percentage': 'currentPercentage'},
        {'value': 'elected', 'seats': 'electedSeats', 'percentage': 'electedPercentage'}
    ]

    # Calculate aggregate values for 'other' and adjust the 'lib' totals
    for field in breakdowns:
        other = senate_map['other']
        # Sum values for parties not in senateMap and not LNP/CLP
        other_sum = sum(d[field['value']] for d in party_data 
                       if d['key'] not in senate_map and d['key'] not in ['lnp', 'clp'])
        
        other[field['value']] = other_sum
        other[field['seats']] = other_sum > 0
        other[field['percentage']] = (
            (other_sum / total_seats * 100) if field['value'] == 'value'
            else (other_sum / other['value'] * 100) if other['value']
            else 0
        )

        # Add LNP and CLP numbers into the Liberal totals
        lib = senate_map['lib']
        lnp_value = party_map.get('lnp', {}).get(field['value'], 0)
        clp_value = party_map.get('clp', {}).get(field['value'], 0)
        lib[field['value']] += lnp_value + clp_value
        lib[field['seats']] = lib[field['value']] > 0
        lib[field['percentage']] = (
            (lib[field['value']] / total_seats * 100) if field['value'] == 'value'
            else (lib[field['value']] / lib['value'] * 100) if lib['value']
            else 0
        )

    # Calculate pending seats
    senate_map['pending']['value'] = total_seats - len(has_data)

    # Sort party_data
    party_data.sort(key=lambda x: x['value'], reverse=True)
    list_size = math.ceil(len(party_map) / 2)

    # Build final render data
    render_data = {
        'TOTAL_SEATS': total_seats,
        'MAJORITY_SEATS': math.ceil(total_seats / 2),
        'partyData': list(senate_map.values()),
        'resultCount': len(has_data),
        'partyListLeft': party_data[:list_size],
        'partyListRight': party_data[list_size:]
    }

    return render_data

def main():
    # Fetch Google doc data
    googledoc = requests.get(f"https://interactive.guim.co.uk/docsdata/{googledoc_key}.json").json()['sheets']
    
    # Get latest feed data
    latest = get_json(f"{url}/recentResults.json")
    latest_feed = get_latest_feed(latest)
    latest_data = get_json(f"{url}/{latest_feed}.json")
    
    # Get summary results
    summary_results = get_json("https://interactive.guim.co.uk/2022/05/aus-election/results-data/summaryResults.json")
    
    print(f"Latest feed: {url}/{latest_feed}.json")
    print(f"Latest googledoc: https://interactive.guim.co.uk/docsdata/{googledoc_key}.json")
    
    # Get swing data
    swing = get_json(f"{url}/{latest_feed}-swing.json")
    print(f"Latest swing: {url}/{latest_feed}-swing.json")
    
    # Create data maps
    electorates_map = {item['electorate']: item for item in googledoc['electorates']}
    places = [item['electorate'] for item in googledoc['electorates']]
    divisions = {item['name']: item for item in latest_data['divisions']}
    parties = {item['partyCode'].lower(): item for item in googledoc['partyNames']}
    
    # Process data
    parties_table_data = create_table(latest_data['partyNationalResults'], googledoc['partyNames'], True)
    ticker = create_ticker_feed(googledoc)
    googledoc['parties'] = parties
    
    print(latest_data['nationalSwing'])
    
    # Process electorates
    electorates_data = googledoc['electorates']
    for item in electorates_data:
        info = select_electorate(item['id'], item['electorate'], electorates_map, divisions, swing, parties)
        if info.get('twoParty'):
            item['byMargin'] = info['twoParty'][0]['swing']
        
        electorate_info = json.dumps(info).encode()
        upload_to_s3(f"{config['path']}/electorates/{item['id']}.json", electorate_info)
    
    # Upload electorates data
    electorates_data_buffer = json.dumps(electorates_data).encode()
    upload_to_s3(f"{config['path']}/electorates.json", electorates_data_buffer)
    
    # Process and upload firewire data
    firewire = compile(googledoc['electorates'], parties, summary_results, latest_data['nationalSwing'])
    firewire_data_buffer = json.dumps(firewire).encode()
    upload_to_s3(f"{config['path']}/firewire.json", firewire_data_buffer)
    
    # Process senate data
    senate_data = senate_render(googledoc)
    
    # Create and upload feed data
    updated = datetime.now().isoformat()
    feed = {
        'updated': updated,
        'ticker': ticker,
        'partiesTableData': parties_table_data,
        'senate': {'displaySenateData': True, 'senateData': senate_data}
    }
    
    feed_data_buffer = json.dumps(feed).encode()
    upload_to_s3(f"{config['path']}/feed.json", feed_data_buffer)
    
    # Upload last updated timestamp
    last_updated_buffer = json.dumps({'updated': updated}).encode()
    upload_to_s3(f"{config['path']}/lastUpdated.json", last_updated_buffer)
    
    # Upload swing data
    swing_feed_buffer = json.dumps(swing).encode()
    upload_to_s3(f"{config['path']}/swing.json", swing_feed_buffer)
    
    print("Cheque please")

if __name__ == "__main__":
    main() 