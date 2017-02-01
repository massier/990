import os
import json
import csv

def download_index(client):
    client.download_file('irs-form-990', 'index.json', '../Scratch/index.json')

def check_index_update(client):
    response = client.list_object_versions(Bucket='irs-form-990', KeyMarker='index.json', MaxKeys=1)
    print(response['Versions'][0]['LastModified'])


def upload_index(connection):
    index_stream = open('../Scratch/index.json', "r")
    index = json.load(index_stream)['AllFilings']

    ins_cursor = connection.cursor()

    ins_query = (
        "INSERT INTO form_index_temp "
        "(updated, ein, url, submittedOn, taxPeriod, dln, isElectronic, isAvailable, formType, objectId)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    counter = 0
    val_list = []
    for itm in index:
        counter += 1
        if counter < 2000000:
            continue
        val_list.append((
                itm.get('LastUpdated'), itm.get('EIN'), itm.get('URL'), itm.get('SubmittedOn'), itm.get('TaxPeriod'), itm.get('DLN'),
                itm.get('IsElectronic'), itm.get('IsAvailable'), itm.get('FormType'), itm.get('ObjectId')))


        if counter % 1000 == 0:
            ins_cursor.executemany(ins_query, val_list)
            if counter % 10000 == 0:
                connection.commit()
            val_list.clear()
            print(counter)
    ins_cursor.executemany(ins_query, val_list)
    connection.commit()


def digest_index_file():
    in_stream = open('../Scratch/index.json', "r")
    out_stream = open('../Scratch/index_digested.csv', "w", newline='')
    field_names = ['LastUpdated', 'EIN', 'URL', 'SubmittedOn', 'TaxPeriod', 'DLN', 'IsElectronic', 'IsAvailable', 'FormType', 'ObjectId']
    csv_writer = csv.DictWriter(out_stream, fieldnames=field_names, extrasaction='ignore')
    index = json.load(in_stream)['AllFilings']
    for itm in index:
        if itm['IsElectronic'] is not None:
            itm['IsElectronic'] = 1 if itm['IsElectronic'] else 0
        if itm['IsAvailable'] is not None:
            itm['IsAvailable'] = 1 if itm['IsAvailable'] else 0
        csv_writer.writerow(itm)

    in_stream.close()
    out_stream.close()
    print("File digest complete")


def upload_digested_file(connection):
    load_cursor = connection.cursor()
    load_query = (
        "LOAD DATA LOCAL INFILE '../Scratch/index_digested.csv' "
        "INTO TABLE irs.form_index_temp_2 "
    )

    load_cursor.execute(load_query)
    load_cursor.commit()
    print("Digest load complete")