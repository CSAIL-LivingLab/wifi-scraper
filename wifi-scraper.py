import os
import requests
import json
from datetime import datetime

from datahub import DataHub
from secret import client_id, client_secret, username, password, ISTUSER, \
    ISTPASS

def scrape_data():
    # create a data directory to temporarily store the data
    tstamp = datetime.now()
    try:
        r = requests.get('https://nist-data.mit.edu/wireless/clients.cgi',
                         auth=(ISTUSER, ISTPASS))
        json_data = json.loads(r.content)
    except Exception as e:
        print e
        # log the exception into logs.txt
        pass
    return (json_data, tstamp)


def write_data_to_file(json_data, tstamp):
    current_directory = os.getcwd()
    if not os.path.exists(current_directory + '/data'):
        os.makedirs(current_directory + '/data')
    filename = current_directory + '/data/' + str(tstamp)
    with file(filename, 'w') as current_file:
        current_file.write(str(json_data))
    return filename


def prep_query(json_data, repo, table, tstamp):
    query = ("INSERT INTO %s.%s "
             "(tstamp, count, id, access_point, building_id, room_number) "
             "values " % (repo, table))
    # Do a messed up thing for keys to support the legacy ID column
    loc2id = {}
    for row in json_data:
        loc = row['accessPoint']
        if loc not in loc2id:
            loc2id[loc] = len(loc2id)
    # actually prepare the statement
    rows = [
        "(%s, %s, %s, '%s', '%s', '%s')"
        % (tstamp.strftime('%s'), row['clientCount'],
           loc2id[row['accessPoint']], row['accessPoint'], row['buildingID'],
           row['roomNumber'])
        for row in json_data]
    query += ', '.join(rows)
    return query


def make_query(query):
    dh = DataHub(client_id=client_id, client_secret=client_secret,
                 grant_type='password', username=username, password=password)
    res = dh.query(repo_base='al_carter', repo='test_repo', query=query)

    return res
