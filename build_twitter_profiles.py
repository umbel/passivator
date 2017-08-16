import json
import re
import sys
import psycopg2
import argparse
import datetime


FOLLOWERS_COUNT_BUCKETS = [ 
    (100, "0 - 100"),
    (200, "100 - 200"),
    (300, "200 - 300"),
    (400, "300 - 400"),
    (500, "400 - 500"),
    (1000, "500 - 1,000"),
    (2000, "1,000 - 2,000"),
    (3000, "2,000 - 3,000"),
    (4000, "3,000 - 4,000"),
    (5000, "4,000 to 5,000"),
    (10000, "5,000 to 10,000"),
    (20000, "10,000 to 20,000"),
    (30000, "20,000 to 30,000"),
    (40000, "30,000 to 40,000"),
    (50000, "40,000 to 50,000"),
    (99999999999, "50,000+"),
]

DATETIME = datetime.datetime.now().isoformat()


try:
    conn = psycopg2.connect("dbname='passivator' user='root' host='passivator.cykflziimevg.us-east-1.rds.amazonaws.com' password='6%]Z6RshEmM}2TB6zvv8G6sM'")
except:
    print "I am unable to connect to the database"

cur = conn.cursor()
cur.execute("""SELECT * from cache_user""")
users = cur.fetchall()


def parse_location(location):
    if not location:
        return None
    #match = re.match(r'^\s*(\w+),*\s*(\w{2})\s*$', location)
    #match = re.match(r'^[A-Za-z]+,[ ]?[A-Za-z]+{2,}$', location)
    match = re.match("([\w\s]+),\s(\w+)", location)
    if match is None:
        return None
    city = match.group(1).title()
    state = match.group(2).upper()
    return (city, state)


def user_to_profile(user):
    """
    given a twitter user, return a profile import object
    profile, segments
    """
    profile = {
        'tags': [],
    }

    # user attributes
    profile['twitter_screen_name'] = user['screen_name']
    profile['twitter_user_id'] = user['id']
    
    for fcb in FOLLOWERS_COUNT_BUCKETS:
        if user['followers_count'] < fcb[0]:
            #profile['followers_count_bucket'] = fcb[1]
            profile['tags'].append({"tag": ['Twitter Followers', fcb[1]], 'datetime': DATETIME})
            break

    # handle location
    #"address": {"city": "Lubbock", "state": "Texa", "postal_code": "79401", "country": "US"}
    location = parse_location(user['location'])
    if location:
        #print "  PARSED:", location
        profile['address'] = {
            'city': location[0],
            'state': location[1],
        }

    if user['lang']:
        profile['tags'].append({"tag": ['Twitter Language', user['lang']], 'datetime': DATETIME})

    return profile


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parse an Epsilon data file.')
    parser.add_argument('--output-file', dest='output_file', action='store',
                        required=False,
                        help='path to the output data file')
    args = parser.parse_args()

    outfile = None
    if args.output_file:
        outfile = open(args.output_file, 'w')

    for user_id, user in users:
        #print user
        profile = user_to_profile(user)
       
        if outfile:
            outfile.write(json.dumps(profile) + '\n')
        else:
            print profile
