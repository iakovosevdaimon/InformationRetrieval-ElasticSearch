#in order to use module xmltodict in cmd type command : pip install xmltodict
#in order to use module xmltodict in cmd type command : pip install elasticsearch
#in PyCharm install xmltodict File->Settings->Project Interpreter->push 'PLUS'(+) button -> search xmltodict->Install Package.Same for package elasticsearch
import xml.etree.ElementTree as ET
import xmltodict
import json
import os
import re
from elasticsearch import Elasticsearch


def find_name(f_name):
    index = len(f_name)
    extension = '.xml'
    for c in f_name[::-1]:
        if c == extension[len(extension) - 1]:
            if f_name[index - len(extension):index] == extension:
                return index - len(extension)

            index -= 1

    return -1


def process_xml_create_json():
    path = 'Parsed files'
    out_path = 'Json files'
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    allfiles = []
    for filename in os.listdir(path):
        if not filename.endswith('.xml'):
            continue
        fullname = os.path.join(path, filename)
        ind = find_name(filename)
        if ind > 0:
            name = filename[:ind]
            allfiles.append(name)
            tree = ET.parse(fullname)
            root = tree.getroot()
            attrib = {}
            c1 = '{'
            c2 = '}'
            x = root.tag.find(c1)
            y = root.tag.find(c2)
            fk = root.tag[x:y + 1]
            element = root.makeelement(fk + 'text', attrib)
            root.insert(2, element)
            data_text1 = re.sub(r"[^a-zA-Z0-9]+", ' ', root[4].text)
            data_text2 = re.sub(r"[^a-zA-Z0-9]+", ' ', root[3].text)
            data = data_text1 + " " + data_text2
            root[2].text = data
            root.remove(root[3])
            root.remove(root[3])
            #in order not to convert into json as @xmlns
            ET.register_namespace('', 'http://cordis.europa.eu')
            xml_to_string = ET.tostring(root, encoding='utf8').decode('utf8')
            json_string = xmltodict.parse(xml_to_string, attr_prefix='')
            name = name + ".json"
            name = os.path.join(out_path, name)
            with open(name, 'w+') as outfile:
                json.dump(json_string, outfile)


def process_xml():
    path = 'Parsed files'
    allfiles = []
    all_json = []
    k = 0
    for filename in os.listdir(path):
        if not filename.endswith('.xml'):
            continue
        fullname = os.path.join(path, filename)
        ind = find_name(filename)
        if ind > 0:
            name = filename[:ind]
            allfiles.append(name)
            tree = ET.parse(fullname)
            root = tree.getroot()
            attrib = {}
            c1 = '{'
            c2 = '}'
            x = root.tag.find(c1)
            y = root.tag.find(c2)
            fk = root.tag[x:y+1]
            element = root.makeelement(fk+'text', attrib)
            root.insert(2, element)
            data_text1 = re.sub(r"[^a-zA-Z0-9]+", ' ', root[4].text)
            data_text2 = re.sub(r"[^a-zA-Z0-9]+", ' ', root[3].text)
            data = data_text1 + " " + data_text2
            root[2].text = data
            root.remove(root[3])
            root.remove(root[3])
            ET.register_namespace('', 'http://cordis.europa.eu')
            xml_to_string = ET.tostring(root, encoding='utf8').decode('utf8')
            json_string = xmltodict.parse(xml_to_string, attr_prefix='')
            all_json.insert(k, json.dumps(json_string))
            k += 1
    return all_json


def index_creation(els, index_n):
    created = False
    # set index

    settings = {
        "settings": {
            "index": {
                "analysis": {
                    "analyzer": {
                        "default": {
                            "type": "english"
                        }
                    }
                }
            }
        }
    }

    if els.indices.exists(index_n):
        els.indices.delete(index=index_n, ignore=[400, 404])
    try:
        if not els.indices.exists(index_n):
            # Ignore 400 means to ignore "Index Already Exist" error.
            els.indices.create(index=index_n, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def load_collection_from_files(els, index_n):
    count = 1
    path = 'Json files'
    for filename in os.listdir(path):
        if not filename.endswith('.json'):
            continue
        fullname = os.path.join(path, filename)
        with open(fullname, 'r') as my_file:
            d = my_file.read()
            els.index(index=index_n, doc_type='project', id=count, body=json.loads(d))
        count += 1
    return count


def load_collection(els, index_n, j_data):
    count = 1
    for d in j_data:
        els.index(index=index_n, doc_type='project', id=count, body=json.loads(d))
        count += 1
    return count


def bulk_load_from_files(els, index_n):
    path = 'Json files'
    action = []
    count = 1
    for filename in os.listdir(path):
        if not filename.endswith('.json'):
            continue
        fullname = os.path.join(path, filename)
        with open(fullname, 'r') as myfile:
            d = myfile.read()
            a = {
                "_index": index_name,
                "_type": "project",
                "_id": count,
                "_source": d
            }
            action.append(a)
            count += 1
    from elasticsearch import helpers
    res = helpers.bulk(els, action, index=index_n)
    return res[0]


def bulk_load(els, index_n, j_data):
    action = []
    count = 1
    for d in j_data:
            a = {
                "_index": index_name,
                "_type": "project",
                "_id": count,
                "_source": d
            }
            action.append(a)
            count += 1
    from elasticsearch import helpers
    res = helpers.bulk(els, action, index=index_n)
    return res[0]


def query_processing():
    global qr
    filename = None
    path = 'Queries'
    not_found = True
    for filename in os.listdir(path):
        if filename == 'testingQueries.txt':
            not_found = False
            filename = 'testingQueries.txt'
            break
    lines = []
    if not not_found:
        fullname = os.path.join(path, filename)
        f = open(fullname, 'r', encoding='utf-8-sig')
        line = f.readline()
        cnt = 1
        rep = ["[", "u'", "\\n"]
        while line:
            for r in rep:
                qr = line.replace(r, "")
            data_query = re.sub(r"[^a-zA-Z0-9]+", ' ', qr)
            if cnt < 10:
                    prefix = 'Q0'+str(cnt)
            else:
                prefix = 'Q'+str(cnt)
            if data_query.startswith(prefix):
                data_query = data_query[len(prefix):]
            lines.insert(cnt, data_query)
            line = f.readline()
            cnt += 1
        f.close()
    else:
        print('Could not find file testingQueries.txt')
    return lines


def search(els, index_n):
    path = 'Results of queries'
    f_name = 'myResults.txt'
    fullname = os.path.join(path, f_name)
    fh = open(fullname, 'w+')
    queries = query_processing()
    count = 1
    for q in queries:
        query = {
            'from': 1, 'size': 20,
            'query': {
                'match': {
                    'project.text': {
                        'query': q
                    }
                }
            }
        }
        res = els.search(index=index_n, doc_type='project', body=query)
        for hit in res['hits']['hits']:
            if count < 10:
                line = 'Q0'+str(count)+" "+"Q0"+" "+hit['_source']['project']['rcn']+" "+"0"+" "+str(hit['_score'])+" "+"es"+"\n"
            else:
                line = 'Q10'+" "+"Q0"+" "+hit['_source']['project']['rcn']+" "+"0"+" "+str(hit['_score'])+" "+"es"+"\n"
            fh.write(line)
        count += 1
    fh.close()


#---------------------MAIN---------------------------#
collection = process_xml()
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
if es.ping():
    print('OK! Connected')
    index_name = 'collection'
    if index_creation(es, index_name):
        print('OK INDEX CREATION')
        # num_of_files = bulk_load_from_files(es, index_name, collection)
        #num_of_files = load_collection(es, index_name, collection)
        #num_of_files = num_of_files-1
        num_of_files = bulk_load(es, index_name, collection)
        if num_of_files == 18316:
            print('Collection stored correctly')
            search(es, index_name)
        else:
            print('Collection didn\'t stored correctly')
            print('-1')
else:
    print('It could not connect to elasticsearch!')
    print('-1')
