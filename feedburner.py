import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Union
import math
from utils import get_json, get_local_json, get_latest_feed, upload_to_s3, save_to_file

startTime = datetime.now()

# The primary vote for the coalition in 2019
coalition_old_primary = 41.44

# The primary vote for the coalition in 2022
# coalition_new_primary = 35.7

if coalition_old_primary == 41.44:
    print("WARNING: USING OLD PRIMARY COALITION")

upload = True

if not upload:
    print("WARNING: NOT UPLOADING TO SERVER")

uploadElectorates = False
if not uploadElectorates:
    print("WARNING: NOT UPLOADING ELECTORATES")

config = {
    "title": "",
    "docData": "",
    "path": "embed/aus/2025/02/aus-election/results-data"
}

# https://interactive.guim.co.uk/docsdata/1b6VI5L_olM-zpSLy2WkHQ7OXIysQ2KFOd-v5SztfoM0.json
googledoc_key = "1b6VI5L_olM-zpSLy2WkHQ7OXIysQ2KFOd-v5SztfoM0"

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

def compile(electorates: List[Dict], options: List[Dict], parties: Dict, summary_results: Dict, national_swing: float) -> Dict:
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

    displaySwing = False if options[0]['swing'] == "FALSE" else True

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
        'displaySwing': displaySwing,
        'outcome': options[0]['outcome']
    }

    render_data['twoParty'] = party_data_2pp
    
    print(f"{summary_results['votesCountedPercent']}% of Australia has voted.")
    
    return render_data


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
    
    # This uses the Coalition's combined primary vote ( NOT their 2pp and needs to be updated for new elections)

    if swing:
        coalition_summary['Swing'] = coalition_summary['Votes (%)'] - coalition_old_primary

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
            # '05-31-2022 07:09:33'
            item['unix'] = int(datetime.strptime(item['timestamp'], "%m-%d-%Y %H:%M:%S").timestamp())
            with_timestamp.append(item)
        else:
            without_timestamp.append(item)

    with_timestamp.sort(key=lambda x: x['unix'], reverse=True)
    return (with_timestamp + without_timestamp)[:15]

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
    parties = {item['partyCode'].lower(): item for item in googledoc['partyNames']}
    
    # Process data
    parties_table_data = create_table(latest_data['partyNationalResults'], googledoc['partyNames'], True)
    ticker = create_ticker_feed(googledoc)
    googledoc['parties'] = parties
    
    print(latest_data['nationalSwing'])
    
    # Process electorates

    electorates_data = googledoc['electorates']

    # Upload electorates data
    electorates_data_buffer = json.dumps(electorates_data).encode()
    if upload:
        upload_to_s3(f"{config['path']}/electorates.json", electorates_data_buffer)
    save_to_file("results/electorates.json", electorates_data_buffer)
    
    # Process and upload firewire data
    firewire = compile(googledoc['electorates'], googledoc['options'], parties, summary_results, latest_data['nationalSwing'])
    firewire_data_buffer = json.dumps(firewire).encode()
    if upload:
        upload_to_s3(f"{config['path']}/firewire.json", firewire_data_buffer)
    save_to_file("results/firewire.json", firewire_data_buffer)    

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
    if upload:
        upload_to_s3(f"{config['path']}/feed.json", feed_data_buffer)
    save_to_file("results/feed.json", feed_data_buffer)    

    # Upload last updated timestamp
    last_updated_buffer = json.dumps({'updated': updated}).encode()
    if upload:
        upload_to_s3(f"{config['path']}/lastUpdated.json", last_updated_buffer)
    save_to_file("results/lastUpdated.json", last_updated_buffer)    

    # Upload swing data
    swing_feed_buffer = json.dumps(swing).encode()
    if upload:
        upload_to_s3(f"{config['path']}/swing.json", swing_feed_buffer)
    save_to_file("results/swing.json", swing_feed_buffer)    
    
    timeRun = datetime.now() - startTime
    print(f"Finished in {timeRun}")

if __name__ == "__main__":
    main() 