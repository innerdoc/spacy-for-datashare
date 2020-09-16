![alt text](spacy-for-datashare.png "Let spaCy do the parsing of Named Entities for documents in the Datashare platform")


# spacy-for-datashare
Let spaCy do the parsing of Named Entities for documents in the Datashare platform.

- [spaCy](www.spacy.io) is a free, open-source library for advanced Natural Language Processing (NLP) in Python
- [Datashare](https://datashare.icij.org/) allows you to better analyze information, in all its forms. It is a free open-source software platform for Mac/Windows/Linux developed by the [International Consortium of Investigative Journalists](www.icij.org)


# Prerequisites
- [install Datashare](https://icij.gitbook.io/datashare/)
- upload documents to Datashare
- make your custom NER-filter visible in Datashare
  - add your plugins location `--pluginsDir "C:\Users\Name\AppData\Roaming\Datashare\plugins"` to `"C:\program files\Datashare-${VERSION}\datashareStandalone.bat"` 
  - register filter in index.js in plugins folder
```javascript
datashare.registerFilter({
  type: 'FilterNamedEntity',
  options: {
    name: 'EMAIL',
    key: 'byMentions',
    category: 'EMAIL',
    isSearchable: true
  }
})
```


# Settings
- SPACY_MODEL = './data/spacy_model/nl-0.0.5/model-best' # local model or spacy default models like nl_core_news_sm
- PREPROCESS_TIKA_OUTPUT=True
- SKIP_ALREADY_PARSED_DOCS = False
- CLEAN_ENTITIES_BEFORE_UPDATE = True
- ES_BASE_URL = 'http://10.0.2.2:9200/' # e.g. for VMbox=10.0.2.2:9200 , for local installation=127.0.0.1:9200
- ACCEPTED_SPACY_LABELS = ('PER', 'ORG', 'GPE', 'PER_C', 'ORG_C', 'NORP', 'LOC', 'EMAIL', 'URL', 'MONEY') # depends on spaCy model


# Steps taken by script
- Get documents from Datashare's ElasticSearch index
- Preprocess raw content (mostly raw TIKA output)
- Parse doc with spaCy
- Delete all old Named Entities that are already in the ES-index
- Get all Named Entities and merge them in Datashare's format
- Bulk index the document updates and new Named Entities to ES-index
- Refresh ES-index

