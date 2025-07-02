import logging 
from typing import (
    TypeVar,
    Type
)
from elasticsearch_rdsl.exceptions import (
    ConfictIndexDefinition,
    IndexNotRegisteredError,
    ErrDetail,
    IndexInitError
)
from collections import defaultdict
from elasticsearch.dsl.utils import AsyncUsingType 
from elasticsearch_rdsl import AsyncRDSLDocument 

logger = logging.getLogger(__name__)


T = TypeVar("T", bound=AsyncRDSLDocument)

class AsyncDocumentRegistry():
    def __init__(self):
        """
        """
        self._indices:dict[str, Type[T]] = {}
        self._related_indices = defaultdict(set)

    def register_document(self, document_class: Type[T]) -> Type[T]:
        """
            Document class register decorator. For fast and easy to es init.
        """
        if not issubclass(document_class, AsyncRDSLDocument):
            raise TypeError(f"Expected input class to be subclass of AsyncRDSLDocument, got ({document_class.__name__})")

        index_meta = getattr(document_class, "Index")
  
        # Add model into internal register
        index_name = getattr(index_meta, 'name')

        if not index_name:
            logger.warning(f"For document class: {document_class.__name__} index name not indicated.")
        elif index_name == "*":
            logger.warning(f"For document class: {document_class.__name__} default index name provided: {index_name}.")

        # Checking document class existance in the internal register 
        if index_name in self._indices:
            raise ConfictIndexDefinition(f"Index: '{index_name}' already registered.")
        
        self._indices[index_name] = document_class
        return document_class
    
    async def index_init(self, index: str, using: AsyncUsingType | None = None, **kwargs) -> None:
        """
        """
        index_ref:Type[T] = self._indices.get(index, False)
        if not index_ref:
            raise IndexNotRegisteredError(f"The indicated index: '{index} not registered")
        else:
            await index_ref.init(using=using, **kwargs)
        
    async def init_all(self, using: AsyncUsingType | None = None, fail_fast:bool = False, **kwargs) -> None:
        """
        """
        err_list = []

        for index_name, index_ref in self._indices.items():
            try:
                index_ref.init(using=using, **kwargs)
            except Exception as ex:
                if fail_fast:
                    raise ex
                else:
                    err_list.append(
                        ErrDetail(
                            index_name=index_name,
                            exception_type=type(ex),
                            detail=str(ex)
                        )
                    )
        if len(err_list) > 0:
            raise IndexInitError(detail=err_list)

    async def index_delete(self, index: str, using: AsyncUsingType | None = None, **kwargs) -> None:
        """
        """
        index_ref:Type[T] = self._indices.get(index, False)
        if not index_ref:
            raise IndexNotRegisteredError(f"The indicated index: '{index} not registered")
        else:
            await index_ref._index.delete(using=using, **kwargs) 

registry = AsyncDocumentRegistry()

