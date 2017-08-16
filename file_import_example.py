import collections
import csv
import datetime
import json
import requests


BATCH_SIZE = 5000
DRY_RUN = False
FILE_PATH = '/Users/benpapillon/bonnaroo/2016_purchases/Bonnaroo_Purchasers_TicketsToday_051116 (1).csv'
ADDRESS_HEADER_ATTRIBUTE_MAPPING = {  # key = Umbel attribute, value = import file header
    'street': 'Bill To Street1',
    'street2': 'Bill To Street2',
    'city': 'Bill To City',
    'state': 'Bill To State Prov',
    'postal_code': 'Bill To Postal Code',
    'country': 'Bill To Country',
}
IMPORT_NAME = '.'.join(FILE_PATH.split('/')[-1].split('.')[0:-1])
PROPERTY_ID = 592
SKIP_APPENDS = True
UMBEL_API_BASE = 'https://api.umbel.com'
UMBEL_API_TOKEN = '682f4171c46893ce8ad22e60f7c29923e45c4e7b'


def parse_records():
    rows_by_email = collections.defaultdict(list)
    with open(FILE_PATH) as file_handle:
        for row_dict in csv.DictReader(file_handle):  # assumes a header row is present
            clean_email = row_dict['Email Address'].lower().strip()
            rows_by_email[clean_email].append(row_dict)
    records = []
    for email, rows in rows_by_email.items():
        tags = set()
        record = {'email': email}
        for row in rows:
            tag = ['2016', row['Receipt Day Of Week Name'], row['Event'], row['Event Level']]  # more complicated tag determination logic may be required depending on the situation
            tags.add(json.dumps(tag))
            address = {}
            for attribute, header in ADDRESS_HEADER_ATTRIBUTE_MAPPING.items():
                if row.get(header):
                    address[attribute] = row.get(header)
            if len(address) > 0:
                record['address'] = address
            name_parts = map(lambda name: name.strip(), row['Will Call Name'].split(','))
            if len(name_parts) == 2:
                record['last_name'], record['first_name'] = name_parts
                record['full_name'] = ' '.join([name_parts[1], name_parts[0]])
        if len(tags) > 0:
            record['tags'] = [{'datetime': datetime.datetime.now().isoformat(), 'tag': json.loads(tag)} for tag in tags]
        records.append(record)
    return records


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield ndx, iterable[ndx: min(ndx + n, l)]


def create_job():
    if DRY_RUN:
        return
    response = requests.post(
        url='%s/v1/%s/jobs' % (UMBEL_API_BASE, PROPERTY_ID),
        data=json.dumps({
            'name': IMPORT_NAME,
            'skip_appends': SKIP_APPENDS,
        }),
        headers={
            'Authorization': 'Bearer %s' % UMBEL_API_TOKEN,
            'Content-Type': 'application/json',
        },
    )
    response.raise_for_status()
    return response.json()['id']


def do_import():
    if not UMBEL_API_TOKEN:
        raise Exception('Must provide Umbel API token')
    records = parse_records()
    job_id = create_job()
    for i, record_batch in batch(records, BATCH_SIZE):
        if DRY_RUN:
            for record in record_batch:
                print record
        else:
            response = requests.post(
                url='%s/v1/%s/import-data' % (UMBEL_API_BASE, PROPERTY_ID),
                data=json.dumps({
                    'import_job': job_id,
                    'name': '%s (%s)' % (IMPORT_NAME, i),
                    'records': record_batch,
                }),
                headers={
                    'Authorization': 'Bearer %s' % UMBEL_API_TOKEN,
                    'Content-Type': 'application/json',
                },
            )
            response.raise_for_status()
            print response.json()


if __name__ == '__main__':
    do_import()
