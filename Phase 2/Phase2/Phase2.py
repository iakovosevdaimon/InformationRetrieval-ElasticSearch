import os
import io
from elasticsearch import Elasticsearch
import re
from elasticsearch import helpers


def find_name(f_name):
    index = len(f_name)
    extension = '.txt'
    for c in f_name[::-1]:
        if c == extension[len(extension) - 1]:
            if f_name[index - len(extension):index] == extension:
                return index - len(extension)

            index -= 1

    return -1


def read_collection():
    path = 'Collection_1'
    allfiles = []
    all_txt = []
    k = 0
    for filename in os.listdir(path):
        if not filename.endswith('.txt'):
            continue
        fullname = os.path.join(path, filename)
        ind = find_name(filename)
        if ind > 0:
            name = filename[:ind]
            allfiles.append(name)
            with io.open(fullname, 'r', encoding='utf8') as f:
                text = []
                line = f.readline()
                cnt = 0
                rep = ["[", "u'", "\\n"]
                while line:
                    for r in rep:
                        line = line.replace('\n', '')
                        qr2 = line.replace(r, "")
                        line = re.sub(r"[^a-zA-Z0-9]+", ' ', qr2)
                    text.insert(cnt, line)
                    cnt += 1
                    line = f.readline()
            all_txt.insert(k, text)
            f.close()
            k += 1
    return all_txt, allfiles


def choose_files():
    texts, all_files = read_collection()
    q1 = []
    q2 = []
    q3 = []
    q4 = []
    q5 = []
    q6 = []
    q7 = []
    q8 = []
    q9 = []
    q10 = []
    for i in range(len(all_files)):
        if all_files[i] == '193378':
            q1 = texts[i]

        elif all_files[i] == '213164':
            q2 = texts[i]

        elif all_files[i] == '204146':
            q3 = texts[i]

        elif all_files[i] == '214253':
            q4 = texts[i]

        elif all_files[i] == '212490':
            q5 = texts[i]

        elif all_files[i] == '210133':
            q6 = texts[i]

        elif all_files[i] == '213097':
            q7 = texts[i]

        elif all_files[i] == '193715':
            q8 = texts[i]

        elif all_files[i] == '197346':
            q9 = texts[i]

        elif all_files[i] == '199879':
            q10 = texts[i]

    all_queries = []
    all_queries.insert(0, q1)
    all_queries.insert(1, q2)
    all_queries.insert(2, q3)
    all_queries.insert(3, q4)
    all_queries.insert(4, q5)
    all_queries.insert(5, q6)
    all_queries.insert(6, q7)
    all_queries.insert(7, q8)
    all_queries.insert(8, q9)
    all_queries.insert(9, q10)
    return all_queries


def take_percentage(q, percentage):
    q_30 = []
    per = percentage/100
    for y in range(10):
        t = int(round(len(q[y])*per))
        new_q = []
        for i in range(t):
            new_q.append(q[y][i])
        q_30.insert(y, new_q)
    return q_30


def search(els, index_n, queries, f_name):
    path = 'Results of queries'
    fullname = os.path.join(path, f_name)
    fh = open(fullname, 'w+')
    count = 1
    for q in queries:

        # match and query_string have the same results
        query = {
            'from': 1, 'size': 20,
            'query': {
                'bool': {
                    'should': [{'match': {'project.text': {'query': q[phrs]}}} for phrs in range(len(q))]
                }
            }
        }
        res = els.search(index=index_n, doc_type='project', body=query)
        for hit in res['hits']['hits']:
            if count < 10:
                line = 'Q0'+str(count)+" "+"Q0"+" "+hit['_source']['project']['rcn']+" "+"0"+" "+str(hit['_score'])+" "\
                       + "es"+"\n"
            else:
                line = 'Q10'+" "+"Q0"+" "+hit['_source']['project']['rcn']+" "+"0"+" "+str(hit['_score'])+" "+"es"+"\n"
            fh.write(line)
        count += 1
    fh.close()


def search_30(els, index_n, queries):
    f_name = 'myResults_30.txt'
    search(els, index_n, queries, f_name)


def search_60(els, index_n, queries):
    f_name = 'myResults_60.txt'
    search(els, index_n, queries, f_name)


def search_90(els, index_n, queries):
    f_name = 'myResults_90.txt'
    search(els, index_n, queries, f_name)


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


def search_mlt(els, index_n):
    queries = query_processing()
    # --------------------DEFAULT EXAMPLE-----------------------------------#
    path = 'Results of queries'
    f_name = 'myResults_mlt_1.txt'
    fullname = os.path.join(path, f_name)
    fh = open(fullname, 'w+')
    count = 1

    for q in queries:

        query = {
            'from': 1, 'size': 20,
            'query': {
                "more_like_this": {
                    "fields": ['project.text'],
                    "like": q
                }
            }
        }

        res = els.search(index=index_n, doc_type='project', body=query)
        for hit in res['hits']['hits']:
            if count < 10:
                line = 'Q0'+str(count)+" "+"Q0"+" "+hit['_source']['project']['rcn']+" "+"0"+" "+str(hit['_score'])+" "\
                       + "es" + "\n"
            else:
                line = 'Q10'+" "+"Q0"+" "+hit['_source']['project']['rcn']+" "+"0"+" "+str(hit['_score'])+" "+"es"+"\n"
            fh.write(line)
        count += 1
    fh.close()
    # -------------------------FIRST EXAMPLE------------------------------#
    path = 'Results of queries'
    f_name = 'myResults_mlt_2.txt'
    fullname = os.path.join(path, f_name)
    fh = open(fullname, 'w+')
    count = 1
    for q in queries:
        query = {
            'from': 1, 'size': 20,
            'query': {
                "more_like_this": {
                    "fields": ['project.text'],
                    "like": q,
                    "max_query_terms": 100,
                    "min_term_freq": 6,
                    "min_doc_freq": 6,
                    "max_doc_freq": 12121,
                    "minimum_should_match": "30%"
                }
            }
        }
        res = els.search(index=index_n, doc_type='project', body=query)
        for hit in res['hits']['hits']:
            if count < 10:
                line = 'Q0' + str(count) + " " + "Q0" + " " + hit['_source']['project']['rcn'] + " " + "0" + " " + str(
                    hit['_score']) + " " \
                       + "es" + "\n"
            else:
                line = 'Q10' + " " + "Q0" + " " + hit['_source']['project']['rcn'] + " " + "0" + " " + str(
                    hit['_score']) + " " + "es" + "\n"
            fh.write(line)
        count += 1
    fh.close()
    # ------------------------SECOND EXAMPLE-------------------------------#
    path = 'Results of queries'
    f_name = 'myResults_mlt_3.txt'
    fullname = os.path.join(path, f_name)
    fh = open(fullname, 'w+')
    count = 1
    for q in queries:
        query = {
            'from': 1, 'size': 20,
            'query': {
                "more_like_this": {
                    "fields": ['project.text'],
                    "like": q,
                    "max_query_terms": 15,
                    "min_term_freq": 5,
                    "min_doc_freq": 6,
                    "max_doc_freq": 1200,
                    "minimum_should_match": "30%"
                }
            }
        }
        res = els.search(index=index_n, doc_type='project', body=query)
        for hit in res['hits']['hits']:
            if count < 10:
                line = 'Q0' + str(count) + " " + "Q0" + " " + hit['_source']['project']['rcn'] + " " + "0" + " " + str(
                    hit['_score']) + " " \
                       + "es" + "\n"
            else:
                line = 'Q10' + " " + "Q0" + " " + hit['_source']['project']['rcn'] + " " + "0" + " " + str(
                    hit['_score']) + " " + "es" + "\n"
            fh.write(line)
        count += 1
    fh.close()
    # ---------------------THIRD EXAMPLE----------------------------------#
    path = 'Results of queries'
    f_name = 'myResults_mlt_4.txt'
    fullname = os.path.join(path, f_name)
    fh = open(fullname, 'w+')
    count = 1
    for q in queries:
        query = {
            'from': 1, 'size': 20,
            'query': {
                "more_like_this": {
                    "fields": ['project.text'],
                    "like": q,
                    "max_query_terms": 15,
                    "min_term_freq": 2,
                    "min_doc_freq": 1,
                    "max_doc_freq": 18316,
                    "minimum_should_match": "30%"
                }
            }
        }
        res = els.search(index=index_n, doc_type='project', body=query)
        for hit in res['hits']['hits']:
            if count < 10:
                line = 'Q0' + str(count) + " " + "Q0" + " " + hit['_source']['project']['rcn'] + " " + "0" + " " + str(
                    hit['_score']) + " " \
                       + "es" + "\n"
            else:
                line = 'Q10' + " " + "Q0" + " " + hit['_source']['project']['rcn'] + " " + "0" + " " + str(
                    hit['_score']) + " " + "es" + "\n"
            fh.write(line)
        count += 1
    fh.close()
    # -------------------------------------------------------#


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
        },
        "mappings": {
            "project": {
                "properties": {
                    "project": {
                        "properties": {
                            "xmlns": {
                                "type": "text"
                            },
                            "rcn": {
                                "type": "text"
                            },
                            "acronym": {
                                "type": "text"
                            },
                            "text": {
                                "type": "text",
                                "term_vector": "yes"
                            },
                            "identifier": {
                                "type": "text"
                            }
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


cf = choose_files()
ask_30 = take_percentage(cf, 30)
ask_60 = take_percentage(cf, 60)
ask_90 = take_percentage(cf, 90)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
if es.ping():
    print('OK! Connected')
    index_name = 'collection'
    search_30(es, index_name, ask_30)
    search_60(es, index_name, ask_60)
    search_90(es, index_name, ask_90)
    new_index_name = 'mlt_collection'
    if index_creation(es, new_index_name):
        print('OK INDEX CREATION')
        helpers.reindex(client=es, source_index=index_name, target_index=new_index_name)
        search_mlt(es, new_index_name)

else:
    print('It could not connect to elasticsearch!')
    print('-1')
