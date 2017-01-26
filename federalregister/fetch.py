#!/usr/bin/python

import pymysql
import pymysql.cursors
import requests

from dateutil import parser

db_host = "10.0.0.28"
db_user = "frtracker"
db_db = "federalregister"
db_pass = "CHANGEME"

fr_url = "https://www.federalregister.gov/api/v1/public-inspection-documents/current.json"

def gimme_datetime(fr_datetime):
    if fr_datetime is not None:
        return parser.parse(fr_datetime)
    return None

connection = pymysql.connect(host=db_host,
                             user=db_user,
                             password=db_pass,
                             db=db_db,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# Fetch current register documents
r = requests.get(fr_url)
r.raise_for_status()
data = r.json()
for doc in data['results']:
    with connection.cursor() as cursor:
        rc = 0
        if doc['publication_date'] is None:
            rc = cursor.execute("SELECT id FROM document WHERE id=%s AND publication_date IS NULL", (doc['document_number'],))
        else:
            rc = cursor.execute("SELECT id FROM document WHERE id=%s AND publication_date=%s", (doc['document_number'], doc['publication_date']))

        if rc == 0:
            filed_at = gimme_datetime(doc['filed_at'])
            pdf_updated_at = gimme_datetime(doc['pdf_updated_at'])

            cursor.execute("INSERT INTO document (id, editorial_note, excerpts, filed_at, filing_type, html_url, json_url, num_pages, pdf_file_name, pdf_file_size, pdf_updated_at, pdf_url, publication_date, raw_text_url, subject_1, subject_2, subject_3, title, toc_doc, toc_subject, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (doc['document_number'], doc['editorial_note'], doc['excerpts'], filed_at, doc['filing_type'], doc['html_url'], doc['json_url'], int(doc['num_pages']), doc['pdf_file_name'], int(doc['pdf_file_size']), pdf_updated_at, doc['pdf_url'], doc['publication_date'], doc['raw_text_url'], doc['subject_1'], doc['subject_2'], doc['subject_3'], doc['title'], doc['toc_doc'], doc['toc_subject'], doc['type']))

            for agency in doc['agencies']:
                if 'id' not in agency:
                    print "SKIP: ({0}) from {1}".format(doc['document_number'], agency['raw_name'])
                    continue
                agency_id = int(agency['id'])
                rc = cursor.execute("SELECT id FROM agency WHERE id=%s", (agency_id,))
                if rc == 0:
                    cursor.execute("INSERT INTO agency (id, slug, name, raw_name, url, json_url, created_at) VALUES (%s, %s, %s, %s, %s, %s, NOW())",
                                   (agency_id, agency['slug'], agency['name'], agency['raw_name'], agency['url'], agency['json_url']))
                cursor.execute("INSERT INTO document_agency (document_id, agency_id) VALUES (%s, %s)", (doc['document_number'], agency_id))

            for docket in doc['docket_numbers']:
                cursor.execute("INSERT INTO document_docket (document_id, docket_number) VALUES (%s, %s)", (doc['document_number'], docket))

    connection.commit()
