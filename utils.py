# Gets a json file from a url

def get_json(feed_url: str) -> Dict:
    response = requests.get(feed_url)
    return response.json()

# Gets a json file from a local path

def get_local_json(path: str):
    with open(path, 'r') as f:
         return json.load(f)

# Returns the highest timestamp from a list of timestamps

def get_latest_feed(latest: List[str]) -> int:
    # print(max(map(int, latest)))
    if not latest:
        raise ValueError("getLatestFeed expects a non-empty array")
    return max(map(int, latest))

# Uploads data to s3
import boto3

def upload_to_s3(dest: str, buffer: bytes, content_type: str = 'application/json'):

    if upload:
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

# Saves a file locally

def save_to_file(dest: str, buffer: bytes):    
    with open(dest, 'wb') as f:
        f.write(buffer)