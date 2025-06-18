import logging 
from exceptions import (
    ConfictIndexDefinition,
    IndexNotRegisteredError,
)
from collections import defaultdict
from elasticsearch.dsl.utils import AsyncUsingType 
from elasticsearch.dsl import AsyncDocument 

logger = logging.getLogger(__name__)

class AsyncDocumentRegistry():
    def __init__(self):
        self._indices:dict[str, AsyncDocument] = {}
        self._related_indices = defaultdict(set)

    def register_document(self, document: AsyncDocument) -> AsyncDocument:
        """
            Document class register decorator. For fast and easy to es init.
        """
        if not issubclass(document, AsyncDocument):
            raise TypeError(f"Expected input class to be subclass of AsyncDocument, got ({document.__name__})")

        index_meta = getattr(document, "Index")
  
        #Add model into internal register
        index_name = getattr(index_meta, 'name')

        if not index_name:
            logger.warning(f"For document class: {document.__name__} index name not indicated.")
        elif index_name == "*":
            logger.warning(f"For document class: {document.__name__} default index name provided: {index_name}.")

        if index_name not in self._indices:
            self._indices[index_name] = document
        else:
            raise ConfictIndexDefinition(f"Index: '{index_name}' already registered.")
        return document
    
    async def index_init(self, index: str, using: AsyncUsingType | None = None, **kwargs):
        index_ref:AsyncDocument = self._indices.get(index, False)
        if not index_ref:
            raise IndexNotRegisteredError(f"The indicated index: '{index} not registered")
        else:
            return await index_ref.init(using=using, **kwargs)
        
    async def index_delete(self, index: str, using: AsyncUsingType | None = None, **kwargs):
        index_ref:AsyncDocument = self._indices.get(index, False)
        if not index_ref:
            raise IndexNotRegisteredError(f"The indicated index: '{index} not registered")
        else:
            return await index_ref._index.delete(using=using, **kwargs) 

registry = AsyncDocumentRegistry()

