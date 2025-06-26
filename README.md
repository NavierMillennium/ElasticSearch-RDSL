# ElasticSearch RDSL


## Features
* Provides all the functionalities offered by official Python ElasticSearch Client
* Defining basic parent-child relationship(class definition) with automatic checking


## Examples

### __1. Index Model/Mapping definition:__

```python
from datetime import datetime, timezone
from elasticsearch_rdsl import (
    registry,
    AsyncRDSLDocument,
)
from elasticsearch.dsl import (
    Text,
    Keyword,
    Date,
    analyzer
)
custom_analyzer = analyzer('custom_analyzer',
    type='custom',
    tokenizer="standard",
    filter=["lowercase", "asciifolding"],
)

@registry.register_document
class Country(AsyncRDSLDocument):
    name_en = Text(
        fields={"keyword": {
            "type": "keyword",
            "ignore_above": 256
        }},
        analyzer=custom_analyzer
    )
    code = Keyword()
    created_at = Date(default_timezone='UTC')
    created_by = Keyword()
    updated_at = Date(default_timezone='UTC')
    updated_by = Keyword()

    class Index:
        name = 'index_countries'

    def save(self, **kwargs):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
    
        return super().save(**kwargs)
```
### __2. Elastic Index Managing:__

```python
from elasticsearch.dsl import async_connections
async_connections.create_connection(alias="default",
        hosts='elastic_url', # change to your elasticsearch url 
        basic_auth=('username', 'secret'), # change to your elasticsearch username and secret
        verify_certs=False,
        ssl_show_warn=False,
    )
await registry.index_delete(index='index_countries') # conditionally provide using = AsyncElasticsearch()
```
