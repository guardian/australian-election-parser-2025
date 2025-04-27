import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const config = {
  "title": "",
  "docData": "",
  "path": "embed/aus/2025/02/aus-election/results-data"
};

const googledocKey = "11WneZFp0CwnkDiBtQTTH_y-OjSzAlZR4c6rCPrQ_baA";

const url = `https://interactive.guim.co.uk/2022/05/aus-election/results-data`

function getAlignment(key) {
  switch (key) {
    case "alp":
    case "grn":
      // These keys return left alignment.
      return "left";
    case "lib":
    case "lnp":
    case "nat":
    case "ind":
    case "kap":
    case "ca":
      // These keys return right alignment.
      return "right";
    default:
      // Default alignment is right.
      return "right";
  }
}

function compile(electorates, parties, summaryResults, nationalSwing) {

    var tallies = electorates.reduce( (acc, electorate) => {
        var party = electorate.prediction.toLowerCase()
        var tally = acc[party] ? acc[party] + 1 : 1 ;
        return {
            ...acc,
            [party]: tally
        }
    }, {})

    var tallyMap = new Map(Object.entries(tallies))

    let partiesList = Array.from( tallyMap.keys() );

    var partyData = []

    partiesList.forEach((d) => {

        if (d) {

        var obj = {}

        obj.key = d

        obj.value = +tallyMap.get(d)

        obj.name = parties.get(d).partyName

        obj.shortName = parties.get(d).shortName

        obj.alignment = getAlignment(d)

        partyData.push(obj)

        }

    })

    //partyData = partyData.map(p => ({ ...p, value: +p.value }));

    var partyMap = new Map( partyData.map( (item) => [item.key, item]) )

    partyData.sort((a,b) => b.value - a.value)

    var listSize = Math.ceil( partyMap.size / 2 )

    var labSeats = (partyMap.get('alp')) ? partyMap.get('alp').value : 0 ;

    var labPercentage = ( labSeats / 151) * 100

    var coalitionSeats = Array.from(partyMap.values()).reduce((total, d) => {

      return (['lib', 'lnp', 'nat', 'clp'].indexOf(d.key) > -1) ? total + d.value : total

    }, 0);

    var coalitionPercentage = ( coalitionSeats / 151) * 100

    labSeats = (labSeats===0) ? '0' : labSeats ;

    coalitionSeats = (coalitionSeats===0) ? '0' : coalitionSeats ;

    var partyData2PP = [

      { name: 'Coalition', seats: coalitionSeats, percentage: coalitionPercentage, party: 'coal' },

      { name: 'Labor', seats: labSeats, percentage: labPercentage, party: 'alp' }

    ]

    //partyData.map((item) => item.value = item.value.toString())

    var renderData = {

        TOTAL_SEATS: 151,

        MAJORITY_SEATS: 76,

        partyData: partyData,

        resultCount: partiesList.length,

        partyListLeft: partyData.slice(0,listSize),

        header1: (partyData.length>0) ? 'seats' : "",

        partyListRight: partyData.slice(listSize),

        header2: (partyData.length>1) ? 'seats' : "",

        updated: new Date(),

        votesCountedPercent: summaryResults.votesCountedPercent,

        nationalSwing: nationalSwing,

        displaySwing: true

    };

    renderData.twoParty = partyData2PP

    console.log(`${summaryResults.votesCountedPercent}% of Australia has voted.`)

    return renderData

}


function selectElectorate(id, electorate, results, divisions, swing, parties) {

  const swingTime = true;

  const partyNamesMap = {
    "Australian Labor Party": "Labor",
    "Liberal National Party of Queensland": "LNP",
    "The Nationals": "National",
    "The Greens (VIC)": "Greens",
    "Pauline Hanson's One Nation": "One Nation",
    "Independent": "Independent",
    "United Australia Party": "UAP",
    "Katter's Australian Party (KAP)": "Katter Party",
    "Centre Alliance": "Centre Alliance"
  };

  function partyNames(party) {
    return partyNamesMap[party] || party;
  }


  console.log("Selected electorate:", electorate);
  // Get the results and division data for the given electorate
  const result = results.get(electorate);
  const aecResult = divisions.get(electorate);

  // Sort candidates based on total votes (descending)
  const candidates = [...aecResult.candidates].sort((a, b) => b.votesTotal - a.votesTotal);

  // Find the swing data for the electorate
  const candidateSwingData = swing.find(item => item.name === electorate)?.tcp;
  let swigInfo = { status: false };

  if (candidateSwingData?.length === 2 && swingTime) {
    // Ensure the candidate with the higher swing is first.
    if (candidateSwingData[0].swing < candidateSwingData[1].swing) {
      candidateSwingData.push(candidateSwingData.shift());
    }

    if (candidateSwingData[1].swing > 0) {
      swigInfo = { status: false };
    } else {
      const multiplier =
        candidateSwingData[0].swing < 15 ? 3.333 :
        candidateSwingData[0].swing < 25 ? 2 : 1;
      const label =
        candidateSwingData[0].swing < 15 ? 15 :
        candidateSwingData[0].swing < 25 ? 25 : 50;

      swigInfo = {
        status: true,
        text: `${candidateSwingData[0].swing}% swing to ${partyNames(candidateSwingData[0].party_long)}`,
        label,
        swingLeft: candidateSwingData[0].swing,
        swingRight: candidateSwingData[1].swing,
        swingPartyLeft: partyNames(candidateSwingData[0].party_long),
        swingPartyRight: partyNames(candidateSwingData[1].party_long),
        swingLeftBar: candidateSwingData[0].swing * multiplier,
        swingRightBar: candidateSwingData[1].swing,
        swingLeftShort: candidateSwingData[0].party_short.toLowerCase(),
        swingRightShort: candidateSwingData[1].party_short.toLowerCase()
      };
    }
  }

  // Process two-candidate preferred results if available.
  let hideTwoParty = false;
  let twoParty;
  let nameField;

  if (Array.isArray(aecResult.twoCandidatePreferred)) {
    twoParty = [...aecResult.twoCandidatePreferred].sort((a, b) => b.votesTotal - a.votesTotal);
    nameField = 'party_long';
  } else {
    hideTwoParty = true;
  }

  // Process prediction info.
  const prediction = result.prediction === '' ? '' : result.prediction.toLowerCase();
  const predictionName = result.prediction === ''
    ? ''
    : parties.get(prediction).partyName;

  // Compile all the info into a single object.
  const info = {
    display: true,
    id,
    electorate,
    candidates,
    hideTwoParty,
    twoParty,
    nameField,
    prediction,
    status: prediction !== '' ? true : false,
    predictionName,
    heldBy: result.incumbent.toLowerCase(),
    heldByName: parties.get(result.incumbent.toLowerCase()).partyName,
    percentageCounted: ((aecResult.votesCounted / aecResult.enrollment) * 100).toFixed(1),
    swigInfo
  };

  return info;
}


function getJson(feedURL) {
  return fetch(`${feedURL}`).then(r => {
    return r.json()
  })
}

const getLatestFeed = (latest) => {
  if (!Array.isArray(latest) || latest.length === 0) {
    throw new Error("getLatestFeed expects a non-empty array");
  }
  const numericFeeds = latest.map(Number);
  return Math.max(...numericFeeds);
}

const others = [
  "Australian Labor Party",
  "The Greens",
  "Independent",
  "Pauline Hanson's One Nation",
  "United Australia Party",
  "Katter's Australian Party (KAP)",
  "Centre Alliance"
];

const coalition = [
  "Liberal",
  "Liberal National Party of Queensland",
  "The Nationals",
  "Country Liberal Party (NT)"
];

function createTable(data, partyMap, swing) {
  // Build a lookup map using lowercased party codes.
  const pMap = new Map(
    partyMap.map(item => [item.partyCode.toLowerCase(), item])
  );

  // Build the initial table data.
  // Convert strings to numbers using Number(...).
  const tableData = data.map(item => {
    const base = {
      Party: item.partygroup_name,
      "Votes (%)": Number(item.votesPercent),
      "Total votes": Number(item.votesTotal),
      Short: item.coalition_short.toLowerCase()
    };
    return swing ? { ...base, Swing: Number(item.swing) } : base;
  });

  // Filter out the items that belong to the coalition.
  const coalitionItems = tableData.filter(item => contains(coalition, item.Party));

  // Build a summary object for the coalition.
  // (Subtract 41.44 from the swing if needed.)
  const coalitionSummary = {
    Party: "Coalition",
    "Votes (%)": sum(coalitionItems, "Votes (%)"),
    "Total votes": sum(coalitionItems, "Total votes"),
    ...(swing && { Swing: sum(coalitionItems, "Votes (%)") - 41.44 })
  };

  // Filter out the remaining parties.
  let result = tableData.filter(item => contains(others, item.Party));

  // Update party names from the lookup map if available.
  for (const item of result) {
    const partyInfo = pMap.get(item.Short);
    if (partyInfo) {
      item.Party = partyInfo.partyName;
    }
  }

  // Keep only the desired properties.
  result = result.map(item => ({
    Party: item.Party,
    "Votes (%)": item["Votes (%)"],
    "Total votes": item["Total votes"],
    ...(swing && { Swing: item.Swing })
  }));

  // Append the coalition summary.
  result.push(coalitionSummary);

  // Sort the results by "Total votes".
  result = sort(result, "Total votes");

  // Format numeric values to fixed 2 decimal places and convert them back to numbers.
  for (const record of result) {
    record["Votes (%)"] = Number(record["Votes (%)"].toFixed(2));
    if (record.Swing !== undefined) {
      record.Swing = Number(record.Swing.toFixed(2));
    }
  }

  return result;
}

function sort(arr, value, ranked=false) {
    let ordered = arr.sort((a, b) => (a[value] < b[value]) ? 1 : -1)
    if (ranked) {
      ordered.forEach( (item, index) => {
          item.rank = index + 1
      });
    }
    return ordered
}

function sum(arr, prop) {

    let total = 0
    for ( var i = 0, _len = arr.length; i < _len; i++ ) {
        total += arr[i][prop]
    }
    return total
}

function contains(a, b) {

    if (Array.isArray(b)) {
        return b.some(x => a.indexOf(x) > -1);
    }

    return a.indexOf(b) > -1;
}

function createTickerFeed(data) {
    // Filter out any entries with an empty prediction.
    const predictions = data.electorates.filter(d => d.prediction !== "");
  
    // Create a Map for quick lookup of party names by party code.
    const partyMap = new Map(data.partyNames.map(item => [item.partyCode, item]));
  
    // Update each prediction with additional properties.
    predictions.forEach(prediction => {
      prediction.announced = "Predicted"; // You can add a dynamic timestamp if needed.
      prediction.status = prediction.prediction === prediction.incumbent ? "hold" : "wins";
      prediction.label =
        prediction.prediction !== "IND"
          ? (partyMap.get(prediction.prediction)?.partyName ?? "")
          : prediction["prediction-name"];
    });
  
    // Partition predictions into those with a timestamp and those without.
    const { withTimestamp, withoutTimestamp } = predictions.reduce(
      (acc, item) => {
        if (item.timestamp !== "") {
          // Calculate Unix timestamp (in seconds).
          item.unix = Math.floor(new Date(item.timestamp).getTime() / 1000);
          acc.withTimestamp.push(item);
        } else {
          acc.withoutTimestamp.push(item);
        }
        return acc;
      },
      { withTimestamp: [], withoutTimestamp: [] }
    );
  
    // Sort items that have timestamps in descending order (most recent first).
    withTimestamp.sort((a, b) => b.unix - a.unix);
  
    // Return the combined array with timestamped items first.
    return [...withTimestamp, ...withoutTimestamp].slice(0, 15);;
  }


  const COALITION = ['lib', 'lnp', 'nat', 'clp']
  const SENATE = ['lib', 'nat', 'on', 'pending', 'other', 'alp', 'grn', 'ca']

  export function senateRender(data) {
    // Set up constants
    const key = 'senatefull';
    const partyField = 'party';
    const totalSeats = 76;
  
    // Filter out records with an empty party field
    const hasData = data[key].filter(d => d[partyField] !== "");
  
    // Tally each party's count (using a case-insensitive party key)
    const partyData = hasData.reduce((acc, curr) => {
      const partyKey = curr[partyField].toLowerCase();
      const existing = acc.find(item => item.key === partyKey);
      if (existing) {
        existing.value++;
      } else {
        acc.push({ key: partyKey, value: 1 });
      }
      return acc;
    }, []);
  
    // Enhance partyData with additional properties
    partyData.forEach(d => {
      const currentItems = hasData.filter(
        item => item[partyField].toLowerCase() === d.key && item.current === 'yes'
      );
      // Assume data.parties is a Map containing party information.
      const partyInfo = data.parties.get(d.key);
      d.name = partyInfo ? partyInfo.partyName : d.key;
      d.shortName = partyInfo ? partyInfo.shortName : d.key;
      d.current = currentItems.length;
      d.elected = d.value - currentItems.length;
    });
  
    // Create a Map for quick lookup of party data by key
    const partyMap = new Map(partyData.map(item => [item.key, item]));
  
    // Build senateData based on the global SENATE array
    const senateData = SENATE.map(d => {
      const party = partyMap.get(d);
      const seats = party ? party.value : 0;
      const current = party ? party.current : 0;
      const elected = party ? party.elected : 0;
      return {
        key: d,
        name: party ? party.name : d,
        shortName: party ? party.shortName : d,
        value: seats,
        current,
        elected,
        seats: seats > 0,
        currentSeats: current > 0,
        electedSeats: elected > 0,
        percentage: (seats / totalSeats) * 100,
        currentPercentage: seats ? (current / seats) * 100 : 0,
        electedPercentage: seats ? (elected / seats) * 100 : 0,
        notpending: d !== 'pending'
      };
    });
  
    // Create a Map from senateData for quick lookup by key
    const senateMap = new Map(senateData.map(item => [item.key, item]));
  
    // Define how we want to break down the numbers
    const breakdown = [
      { value: "value", seats: "seats", percentage: "percentage" },
      { value: "current", seats: "currentSeats", percentage: "currentPercentage" },
      { value: "elected", seats: "electedSeats", percentage: "electedPercentage" }
    ];
  
    // Calculate aggregate values for 'other' and adjust the 'lib' totals
    breakdown.forEach(field => {
      const other = senateMap.get('other');
      // Sum values for parties that are not explicitly in senateMap and not LNP/CLP
      other[field.value] = partyData
      .filter(d => !senateMap.has(d.key) && d.key !== 'lnp' && d.key !== 'clp')
      .reduce((sum, d) => sum + d[field.value], 0);
      other[field.seats] = other[field.value] > 0;
      other[field.percentage] =
        field.value === "value"
          ? (other[field.value] / totalSeats) * 100
          : (other[field.value] / other.value) * 100;
  
      // Add LNP and CLP numbers into the Liberal totals
      const lib = senateMap.get('lib');
      const lnpValue = partyMap.get('lnp') ? partyMap.get('lnp')[field.value] : 0;
      const clpValue = partyMap.get('clp') ? partyMap.get('clp')[field.value] : 0;
      lib[field.value] += lnpValue + clpValue;
      lib[field.seats] = lib[field.value] > 0;
      lib[field.percentage] =
        field.value === "value"
          ? (lib[field.value] / totalSeats) * 100
          : (lib[field.value] / lib.value) * 100;
    });
  
    // Calculate pending seats
    senateMap.get('pending').value = totalSeats - hasData.length;
  
    // Sort partyData in descending order by the number of seats won
    partyData.sort((a, b) => b.value - a.value);
    const listSize = Math.ceil(partyMap.size / 2);
  
    // Build the final render data object
    const renderData = {
      TOTAL_SEATS: totalSeats,
      MAJORITY_SEATS: Math.ceil(totalSeats / 2),
      partyData: Array.from(senateMap.values()),
      resultCount: hasData.length,
      partyListLeft: partyData.slice(0, listSize),
      partyListRight: partyData.slice(listSize)
    };
  
    return renderData;
  }

export async function uploadToS3(dest, buffer, ContentType = 'application/json') {
  const client = new S3Client({});
  const command = new PutObjectCommand({
    Bucket: "gdn-cdn",
    Key: dest,
    Body: buffer,
    ContentType: ContentType,
    ACL: 'public-read',
    CacheControl: "max-age=30"
  });

  try {
    await client.send(command);
    console.log(`https://interactive.guim.co.uk/${dest}`);
  } catch (err) {
    console.error(err);
  }
}

async function createSwingFeed(swingData, electorates) {
  
  const swingType = 'tpp'; // or 'tpp'
  
  const partyColors = {
    ALP: "#b51800", LNP: "#005689", LIB: "#005689", NAT: "#197caa",
    GRN: "#298422", Unknown: "#767676", NXT: "#e6711b", CA: "#e6711b",
    "": "#767676", KAP: "#ff9b0b", GVIC: "#298422", IND: "purple", Excluded: "#000"
  };
  
  const stateOrder = { NSW: 0, VIC: 1, QLD: 2, WA: 3, SA: 4, TAS: 5, NT: 6, ACT: 6 };
  const partyOrder = { ALP: 0, LNP: 1, LIB: 1, NAT: 1, GRN: 2, Unknown: 2, NXT: 2, CA: 2, Excluded: 3 };
  
  const electoratesMap = new Map(electorates.map(e => [e.electorate, e]));
  
  const processed = swingData.map(d => {
    const meta = electoratesMap.get(d.name) || { prediction: "Unknown", incumbent: "Unknown" };
    let value = null;
  
    if (swingType === 'tcp') {
      if (!d.tcp || !d.tcp[0] || !d.tcp[1]) return null;
      const short = d.tcp[0].swing > d.tcp[1].swing ? d.tcp[0].party_short : d.tcp[1].party_short;
      const val = Math.max(d.tcp[0].swing, d.tcp[1].swing);
      value = ["LNP", "LIB", "NAT"].includes(short) ? val : -Math.abs(val);
    } else {
      value = d.tppCoalition;
    }
  
    if (isNaN(value)) return null;
  
    return {
      name: d.name,
      state: d.state,
      value,
      size: Math.abs(value),
      prediction: meta.prediction,
      incumbent: meta.incumbent,
      stateOrder: stateOrder[d.state] ?? 6,
      partyOrder: partyOrder[meta.incumbent] ?? 2,
      partyColor: partyColors[meta.prediction] || '#999'
    };
  }).filter(Boolean);
  
  // Optional sort for initial view
  return processed.sort((a, b) => a.value - b.value);
  
}

;(async () => {

  // 

  // https://interactive.guim.co.uk/docsdata/11WneZFp0CwnkDiBtQTTH_y-OjSzAlZR4c6rCPrQ_baA.json

  const json = await fetch(`https://interactive.guim.co.uk/docsdata/${googledocKey}.json`).then(res => res.json());

  const googledoc = json.sheets

  const latest = await getJson(`${url}/recentResults.json`)

  const latestFeed = getLatestFeed(latest)

  const latestData = await getJson(`${url}/${latestFeed}.json`)

  const summaryResults = await getJson(`https://interactive.guim.co.uk/2022/05/aus-election/results-data/summaryResults.json`)

  console.log(`Latest feed: ${url}/${latestFeed}.json`)

  console.log(`Latest googledoc: https://interactive.guim.co.uk/docsdata/${googledocKey}.json`)

  const swing = await getJson(`${url}/${latestFeed}-swing.json`);

  console.log(`Latest swing: ${url}/${latestFeed}-swing.json`)

  const electoratesMap = new Map(googledoc.electorates.map(item => [item.electorate, item]));

  const places = googledoc.electorates.map( (item) => item.electorate)

  const divisions = new Map(latestData.divisions.map(item => [item.name, item]));

  const parties = new Map(googledoc.partyNames.map(item => [item.partyCode.toLowerCase(), item]));

  const partiesTableData = await createTable(latestData.partyNationalResults, googledoc.partyNames, true)

  const ticker = createTickerFeed(googledoc)

  googledoc.parties = parties

  const senateData = senateRender(googledoc);

  const electoratesData = googledoc.electorates

  console.log(latestData.nationalSwing)

  for await (const item of electoratesData) {
    
    let info = selectElectorate(item.id, item.electorate, electoratesMap, divisions, swing, parties)

    let byMargin = info.twoParty[0].swing

    /*

    How is the byMargin value calculated?

    */

    item.byMargin = byMargin

    let electorateInfo = Buffer.from(JSON.stringify(info));

    await uploadToS3(`${config.path}/electorates/${item.id}.json`, electorateInfo, 'application/json');

  }

  let electoratesDataBuffer = Buffer.from(JSON.stringify(electoratesData));

    // Upload JSON to S3 directly from buffer
  await uploadToS3(`${config.path}/electorates.json`, electoratesDataBuffer, 'application/json');


  const firewire = await compile(googledoc.electorates, parties, summaryResults, latestData.nationalSwing)

  let firewireDataBuffer = Buffer.from(JSON.stringify(firewire));

  await uploadToS3(`${config.path}/firewire.json`, firewireDataBuffer, 'application/json');

  const updated = new Date

  const feed = {

    updated : updated,

    ticker : ticker, 

    partiesTableData : partiesTableData,

    senate : { displaySenateData : true, senateData : senateData }
    
  }

  let feedDataBuffer = Buffer.from(JSON.stringify(feed));

  await uploadToS3(`${config.path}/feed.json`, feedDataBuffer, 'application/json');

  let lastUpdatedBuffer = Buffer.from(JSON.stringify({ updated : updated }));

  await uploadToS3(`${config.path}/lastUpdated.json`, lastUpdatedBuffer, 'application/json');

  //let swingFeed = await createSwingFeed(swing, googledoc.electorates)

  let swingFeedBuffer = Buffer.from(JSON.stringify(swing));

  await uploadToS3(`${config.path}/swing.json`, swingFeedBuffer, 'application/json');

  console.log("Cheque please")

})();