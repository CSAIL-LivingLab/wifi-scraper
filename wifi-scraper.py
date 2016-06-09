import time
import os
import requests
import json
from datetime import datetime

from datahub import DataHub
from secret import client_id, client_secret, username, password, ISTUSER, \
    ISTPASS


def scrape_data():
    tstamp = datetime.now()
    try:
        r = requests.get('https://nist-data.mit.edu/wireless/clients.cgi',
                         auth=(ISTUSER, ISTPASS))
        json_data = json.loads(r.content)
    except Exception as e:
        write_to_log_file(str(e) + "\n-----\n")
    return (json_data, tstamp)


def write_to_log_file(string):
    # create a directory for the errors, if necessary
    current_directory = os.getcwd()
    if not os.path.exists(current_directory + '/data'):
        os.makedirs(current_directory + '/data')
    filename = current_directory + '/data/logs.txt'

    # write error to the data file
    with file(filename, 'w') as current_file:
        current_file.write(str(string))

    return filename


def write_data_to_file(json_data, tstamp):
    # create a directory for the data, if necessary
    current_directory = os.getcwd()
    if not os.path.exists(current_directory + '/data'):
        os.makedirs(current_directory + '/data')
    filename = current_directory + '/data/' + str(tstamp)

    # write data to the data file
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
    res = dh.query(repo_base='livinglab', repo='wifi', query=query)

    return res


def scrape_and_insert_data():
    # actually do the thing
    print('scraping data')
    (json_data, tstamp) = scrape_data()

    print('writing data to file')
    write_data_to_file(json_data, tstamp)

    # Split into queries of 500 rows to avoid timeout

    inserted = 0
    num_rows = 500
    row_length = len(json_data)
    while inserted < row_length:
        data = None
        if inserted + num_rows <= row_length:
            data = json_data[inserted:inserted + num_rows]
            print('prepping query for %d to %d'
                  % (inserted, inserted + num_rows))
            inserted += num_rows
        else:
            data = json_data[inserted:row_length]
            print('prepping q for %d to %d'
                  % (inserted, row_length))
            inserted += row_length

        query = prep_query(data, 'wifi', 'wifi', tstamp)

        print('executing query')
        res = make_query(query)

    print('writing to log file')
    write_to_log_file(str(res) + "\n----\n")

    # wait for 5 minutes, and then scrape data again
    print('waiting for 5 minutes, then scraping again')
    time.sleep(5 * 60)
    scrape_and_insert_data()


scrape_and_insert_data()
