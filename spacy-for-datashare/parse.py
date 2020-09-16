import requests
import time
import uuid
import json
import re
import unicodedata
from urllib.parse import urlparse

import tqdm
import spacy
from spacy.pipeline import EntityRuler

from price_parser import Price # pip install price-parser


# Update Datashare's ElasticSearch index with Named Entities parsed by your Spacy model

# CommandLine$ python3 codebase/spaCy-for-Datashare/scripts/datashare_ner.py


LANGUAGE = 'DUTCH'
SPACY_MODEL = './data/spacy_model/nl-0.0.5/model-best' # or spacy default model like nl_core_news_sm
MAX_NR_OF_DOCS_TO_PROCESS = 999999999
PREPROCESS_TIKA_OUTPUT=True
SKIP_ALREADY_PARSED_DOCS = False
CLEAN_ENTITIES_BEFORE_UPDATE = True
ES_BASE_URL = 'http://10.0.2.2:9200/' # from_VMbox=10.0.2.2:9200 local=127.0.0.1:9200
ES_INDEX_NAME = 'local-datashare'
ES_URL_HEADERS = {"Content-Type": "application/json"}
ES_SEARCH_SIZE = 1000
PARSED_TAG = 'Parsed_by_spaCy'
ACCEPTED_SPACY_LABELS = ('PER', 'ORG', 'GPE', 'PER_C', 'ORG_C', 'NORP', 'LOC', 'EMAIL', 'URL', 'MONEY')




def preprocess_tika_output(content, content_type):
    # preprocess tika-output as good as possible, also use ftfy?

    # replace \r\n line-ends with \n
    content = re.sub(r'\r\n', r'\n', content) #.replace('"','\'').replace('  ',' ') # .replace('\n',' ').replace('\t',' ')
    
    if content_type == 'application/pdf':
        # preprocess broken lines from tika output

        # undo of subword-split-up for words which continue on the next line, e.g.: Maar wij bieden bijvoor-\nbeeld met een
        content = re.sub(r'([a-z])(-\n)([a-z])', r'\1\3', content) 
        
        # lineends_between_title_sent
        content = re.sub('(\n)([\w]{1,40}[ ]?)(\n{1,2})([ ]?[A-Z])', r'__TITLE_START__\2__TITLE_SENTENCE__\4', content) # dont change these : no dot at the end of previouw sentence, next sent starts with capital

        # lineends_between_sents
        content = re.sub('([\.?!][ ]?)(\n{1,2})([ ]?[A-Z])', r'\1__BETWEEN_SENTENCES__\3', content) # dont change these

        # lineends_with_tab
        content = re.sub('\n{1,2}\t', '__SECTION_TAB__', content) # dont change these : no dot at the end of previouw sentence, next sent starts with capital
        
        # threeplus_lineends 
        content = re.sub('[\n]{3,99}', '__SECTION_BREAK__', content)
        
        # lineends_mid_sentence
        '''if (len(re.findall('([\w][ ]?[,-]?[ ]?)(\n\n)([a-z])', content)) / len(re.findall('\n\n', content)) ) > 0.20:
            content = re.sub('([\w][ ]?[,-]?[ ]?)(\n\n)([\w])', r'\1__2REMOVED__\3', content) # remove lineends

        elif (len(re.findall('([\w][ ]?[,-]?[ ]?)(\n{1})([a-z])', content)) / len(re.findall('\n{1}', content)) ) > 0.20:
            content = re.sub('([\w][ ]?[,-]?[ ]?)(\n{1})([\w])', r'\1__1REMOVED__\3', content) # remove lineends
        else:
            content = re.sub('([\w][ ]?[,-]?[ ]?)(\n{1,2})([a-z])', r'\1__REMOVED__\3', content) # remove lineends
        '''

        # improve TITLESTART with surrounding lowercases
        content = re.sub('([a-z][0-9\-, ]{0,11}[ ]?)(__TITLE_START__)([a-z])', r'\1 \3', content) 
        
        # other_lineends 
        content = re.sub('\s+', ' ', content)
        content = re.sub('__BETWEEN_SENTENCES__', '\n', content)
        content = re.sub('__TITLE_START__', '\n\n', content)
        content = re.sub('__TITLE_SENTENCE__', '\n\n', content)
        content = re.sub('__SECTION_TAB__', '\n\t', content)
        content = re.sub('__SECTION_BREAK__', '\n\n\n\n', content)
        content = re.sub('\n[ ]+', '\n', content)

    return content


def run_es_command(http_method, url_command, json, headers=ES_URL_HEADERS):
    if http_method == 'POST':
        response = requests.post(url_command, json=json, headers=headers)
    elif http_method == 'PUT':
        response = requests.put(url_command, json=json, headers=headers)
    elif http_method == 'GET':
        response = requests.get(url_command, json=json, headers=headers)

    return response


def es_search_docs(es_base_url=ES_BASE_URL, skip=SKIP_ALREADY_PARSED_DOCS, parsed_tag=PARSED_TAG, language=LANGUAGE, size=ES_SEARCH_SIZE):
    # get all non-spacy-parsed documents 
    if skip:
        print(f'\033[92m\u2713 Skip already parsed docs\033[00m')
        search_url = f'{es_base_url}_search?q=language:{language}%20AND%20type:Document%20AND%20NOT%20tags:{parsed_tag}&size={size}'
    else:
        print(f'\033[92m\u2713 Also select already parsed docs\033[00m')
        search_url = f'{es_base_url}_search?q=language:{language}%20AND%20type:Document&size={size}'

    print(f'\033[92m\u2713 {search_url}\033[00m')
    response = run_es_command('GET', search_url, None)

    if response.status_code != 200:
        print(f'\u2717 Search Docs failed ({response.status_code})')
        print(response.json())

    return response.json(), language


def es_delete_ents(doc_id, es_base_url=ES_BASE_URL, index_name=ES_INDEX_NAME, clean_ents=CLEAN_ENTITIES_BEFORE_UPDATE):
    if clean_ents:
        # delete all ents for this doc from the ES index
        ner_delete_url = f'{es_base_url}{index_name}/doc/_delete_by_query'
        delete_json = {"query": {"bool": { "must": [
                        { "term": { "documentId": doc_id }},
                        { "term": { "type": "NamedEntity" }}
                        ] } } }
        response = run_es_command('POST', ner_delete_url, delete_json)

        if response.status_code != 200:
            print(f'\u2717 Deleting Named Entities failed ({response.status_code})')
            print(f'\u2717 {delete_json}')
            print(f'\u2717 {ner_delete_url}')
            print(response.json())


def es_bulk_index(bulk_dict, es_base_url=ES_BASE_URL, index_name=ES_INDEX_NAME):
    headers = {"Content-Type": "application/x-ndjson"}
    entities_bulk_data = '\n'.join(map(json.dumps, bulk_dict)) + '\n'
    entities_bulk_url = f'{es_base_url}{index_name}/_bulk'
    print(f'\u2713 {entities_bulk_url}')
    response = requests.post(entities_bulk_url, data=entities_bulk_data, headers=headers)
    if response.status_code == 200:
        print(f'\u2713 Bulk query ready: {round(len(bulk_dict)/2)} index operations realized')
    else:
        print(f'\u2717 Bulk query failed ({response.status_code})')
        print(response.json())


def es_refresh_index(es_base_url=ES_BASE_URL):
    refresh_all_url = f'{es_base_url}_refresh'
    response = run_es_command('POST', refresh_all_url, None)
    if response.status_code == 200:
        print(f'\u2713 ElasticSearch index refreshed')
    else:
        print(f'\u2717 Index Refresh failed ({response.status_code})')
        print(f'\u2717 {refresh_all_url}')
        print(response.json())
    

def get_documents(preprocess=PREPROCESS_TIKA_OUTPUT):
    response, language = es_search_docs()

    texts = []
    for doc in response['hits']['hits']:
        doc_id = doc['_id']
        doc_path = doc['_source']['path']
        doc_lang = doc['_source']['language']
        #print(doc_id, doc_path, doc_lang)

        content_type = doc['_source']['contentType']
        content = doc['_source']['content']
        if preprocess:
            content = preprocess_tika_output(content, content_type)

        texts.append([content, doc_id])
        
    print(f'\u2713 {len(texts)} {language} docs loaded from ElasticSearch')
    if len(texts) == 0:
        print(f'\u2717 Exit script')
        exit()
    if preprocess:
        print(f'\u2713 Content (TIKA output) is preprocessed')
    return texts


def load_spacy(spacy_model=SPACY_MODEL, accepted_labels=ACCEPTED_SPACY_LABELS):
    nlp = spacy.load(spacy_model, disable=['tagger','parser'])
    
    ruler = EntityRuler(nlp)
    patterns = [{"label": "URL", "pattern": [{"LIKE_URL": True}] },
                {"label": "EMAIL", "pattern": [{"LIKE_EMAIL": True}] },
                {"label": "MONEY", "pattern": [{"IS_CURRENCY": True}] },
                {"label": "MONEY", "pattern": [{"LOWER": "euro"}] },
                {"label": "MONEY", "pattern": [{"LOWER": "eur"}] },
                {"label": "MONEY", "pattern": [{"LOWER": "euros"}] },
                {"label": "MONEY", "pattern": [{"LOWER": "dollar"}] },
                {"label": "MONEY", "pattern": [{"LOWER": "dollars"}] },
                ]
    # https://github.com/gandersen101/spaczz/blob/master/src/spaczz/regex/_commonregex.py
    ruler.add_patterns(patterns)
    nlp.add_pipe(ruler)
    
    nlp_labels = str(list(nlp.pipe_labels.items())[0][1])
    ruler_labels = str(list(nlp.pipe_labels.items())[1][1])
    print(f'\u2713 spaCy model loaded: {spacy_model}')
    print(f'\u2713 NER-pipeline loaded with labels: {nlp_labels} | {ruler_labels}')
    print(f'\u2713 ES-index accepted labels: {accepted_labels}')
    return nlp


def get_entity(doc_id, doc, e, bulk_dict, language=LANGUAGE, index_name=ES_INDEX_NAME, accepted_labels=ACCEPTED_SPACY_LABELS):
    # only add accepted NER-labels
    if e.label_ not in accepted_labels:
        return bulk_dict
    
    # unidecode special chars to their simple version
    nfkd_form = unicodedata.normalize('NFKD', e.text) # Datashare doesnt use special characters like ọàbúròẹlẹ́wàâêî,ôûçÇ in ES
    mention_text = nfkd_form.encode('ASCII', 'ignore').decode()

    '''for l in e.text:
        ord_l = ord(l)
        if ord_l < 0 or ord_l > 256 or not ord_l:
            print(f'>>{e.text}<<')
            print(f'>>{mention_text}<<')
            print(f'{l} : {ord(l)}')
            try:
                print(unicodedata.name(l))
            except:
                print('UNKNOWN NAME')
            try:
                print(unicodedata.category(l))
            except:
                print('UNKNOWN CATEGORY')

    if e.text != mention_text and False:
        print(f'>>{mention_text}<<')
        try:
            print(unicodedata.name(e.text[0]))
        except:
            print('UNKNOWN NAME')
        try:
            print(unicodedata.category(e.text[0]))
        except:
            print('UNKNOWN CATEGORY')
    '''
    # determine the offset for the entity (position in text)
    if e.label_ in ('MONEY'):
        start_token = end_token = e.start
        try:
            token_id = e.start-1
            prev_token = doc[token_id]
            #print(token_id, prev_token, prev_token.like_num, prev_token.is_digit, prev_token.is_punct)
        except:
            pass
        if prev_token.like_num or prev_token.is_digit or prev_token.is_punct:
            start_token = token_id
            try:
                token_id = e.start-2
                prev_token = doc[token_id]
                #print(token_id, prev_token, prev_token.like_num, prev_token.is_digit, prev_token.is_punct)
            except:
                pass
            if prev_token.like_num or prev_token.is_digit or prev_token.is_punct:
                start_token = token_id

        #print(e.start, e.text)

        try:
            token_id = e.start+1
            next_token = doc[token_id]
            #print(token_id, next_token, next_token.like_num, next_token.is_digit, next_token.is_punct)
        except:
            pass
        if next_token.like_num or next_token.is_digit or next_token.is_punct:
            end_token = token_id
            try:
                token_id = e.start+2
                next_token = doc[token_id]
                #print(token_id, next_token, next_token.like_num, next_token.is_digit, next_token.is_punct)
            except:
                pass
            if next_token.like_num or next_token.is_digit or next_token.is_punct:
                end_token = token_id

        price_text = doc[start_token:end_token+1].text
        price = Price.fromstring(price_text)
        
        if price.currency and price.amount_text:
            currency_name = 'EUR' if price and price.currency.lower() in ('€', 'euro', 'eur', 'euros') else 'UNK'
            ent_begin = doc[start_token].idx
            mention_text = f'{currency_name} {price.amount_text}' # f'{price.currency} {price.amount_text}'
            mention_norm_text = f'{currency_name} {price.amount_float}'
        else:
            return bulk_dict

    elif e.label_ in ('URL'):
        parsed_uri = urlparse(mention_text) # urlsplit
        mention_norm_text = parsed_uri.netloc or parsed_uri.path
        mention_norm_text = mention_norm_text.lstrip('www.').split('/')[0]
        ent_begin = e.start_char + mention_text.find(mention_norm_text)
    else:
        ent_len_ws = len(mention_text) # length with whitespace
        ent_leading_space = ent_len_ws - len(mention_text.lstrip()) # a spacy entity can have leading/trailing spaces/tabs/newlines: adjust these for merging with the tokens
        ent_begin = e.start_char + ent_leading_space # adjust to get same start as non-leading whitespace for tokens
        mention_text = mention_text.lstrip().split('\n')[0].split('\t')[0].rstrip() # remove line-breaks or tabs or spacing
        mention_norm_text = mention_text

    # don't accept strings with only spacing/interpunction
    no_words = re.compile('^((?!\w).)*$')
    if no_words.match(mention_text):
        #print(f'---->{e.text}<----{mention_text}----')
        return bulk_dict

    # map spaCy-specific labels to Datashare-specific categories
    if e.label_ == 'PER':
        category = 'PERSON'
        if mention_norm_text[0].islower():
            mention_norm_text = mention_norm_text.title()
    elif e.label_ == 'ORG':
        category = 'ORGANIZATION'
    elif e.label_ == 'LOC':
        category = 'LOCATION'
    elif e.label_ == 'PER_C':
        category = 'PERSON_ROLE'
    elif e.label_ == 'ORG_C':
        category = 'ORGANIZATION_TYPE'
    else:
        category = e.label_ # NORP, GPE, URL, EMAIL, MONEY

    # unique entity id
    ent_id = str(uuid.uuid4())

    # create statement
    entity_create_json = { "create" : { "_index" : index_name, "_type" : "doc", "_id" : ent_id, "routing": doc_id} }

    # index-format for NamedEntity
    entity_json = {"offset": ent_begin,
            "extractor": "SPACY",
            "partsOfSpeech": None,
            "rootDocument": doc_id,
            "type": "NamedEntity",
            "mention": mention_text,
            "isHidden": False,
            "extractorLanguage": language,
            "mentionNorm": mention_norm_text,
            "documentId": doc_id,
            "id": ent_id,
            "join": {"parent": doc_id, "name": "NamedEntity"},
            "category": category
            }

    bulk_dict.append(entity_create_json) 
    bulk_dict.append(entity_json)
    return bulk_dict


def update_doc(doc_id, doc_text, bulk_dict, parsed_tag=PARSED_TAG, es_base_url=ES_BASE_URL, index_name=ES_INDEX_NAME):
    # update document with nerTags, Tags and Content (take the text-input to spacy to have a good match with entity-offsets)
    # nerTags: what's the purpose? should be an accepted value: [IXAPIPE, TEST, CORENLP, MITIE, GATENLP, OPENNLP, EMAIL] 
    update_doc_json = { "update" : {"_id" : doc_id, "_type" : "doc", "_index" : index_name} }
    
    doc_json = {"doc" : {
                    "nerTags" : ["CORENLP"],
                    "tags" : [parsed_tag],
                    "content" : doc_text,
                    "contentLength" : len(doc_text),
                    "status" : "DONE"
                } }

    bulk_dict.append(update_doc_json) 
    bulk_dict.append(doc_json)
    return bulk_dict


def parse_and_index_entities(max_docs=MAX_NR_OF_DOCS_TO_PROCESS):
    starttime =  time.time()

    # load documents
    texts = get_documents()

    # load spaCy model
    nlp = load_spacy()

    # parse documents
    bulk_dict = []
    for (doc, doc_id) in nlp.pipe(tqdm.tqdm(texts[:max_docs], desc='\u2713 Docs'), as_tuples=True, batch_size=50):
        # if required, delete Named Entities from Datashare-ES
        es_delete_ents(doc_id)

        # define Datashare-format for each Named Entity that spaCy found
        for e in list(doc.ents):
            bulk_dict = get_entity(doc_id, doc, e, bulk_dict)

        # update document with new (preprocessed) content and tag
        bulk_dict = update_doc(doc_id, doc.text, bulk_dict)

    # ES-bulk-query and refresh ES-index
    es_bulk_index(bulk_dict)
    es_refresh_index()
    print(f'\u2713 Session ready in {round(time.time() - starttime)} seconds') 


# GO!
parse_and_index_entities()


