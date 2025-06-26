from __future__ import annotations
from elasticsearch import NotFoundError
from elasticsearch.dsl.utils import AsyncUsingType 
from elasticsearch.dsl import (
    AsyncDocument,
    Field,
    Keyword,
    AsyncSearch
)
from typing import (
    Type,
    Any
)
from elasticsearch_rdsl.exceptions import (
    IntegrityError
) 


class RelatedDocument:
    def __init__(
            self,
            doc_class: Type[AsyncRDSLDocument], 
            on_delete: str = "check",
            dsl_type: Field = Keyword(),
    ):
        """
        :arg doc_class: related class ref/name
        :arg on_delete: document delete strategies: 'check', 'cascade', 'set_null'
        :arg dsl_type: (default: 'elasticsearch.dsl.Keyword()') target field mapping in the elasticsearch database 
        """
        self.dsl_type = dsl_type
        self.doc_class = doc_class
        self.on_delete = on_delete


class BaseRelDocument():
    def __init__(
            self,
            doc_class: Type[AsyncRDSLDocument],
            field_name: str = "_id",
            on_delete: str = "check"
        ):
        """
        :arg child_doc_class: child document class in document relation
        :arg field_name: document field name related by foreign key
        :arg on_delete: document delete strategies: 'check', 'cascade', 'set_null'
        """
        self.doc_class = doc_class
        self.field_name = field_name
        self.on_delete = on_delete


class ParentRelDocument(BaseRelDocument): ...
class ChildRelDocument(BaseRelDocument): ...


class RDSLDocumentMeta(type(AsyncDocument)):
    """
    """
    _rel_parents: dict[str, ParentRelDocument] 
    _rel_childs: dict[AsyncRDSLDocument, ChildRelDocument]

    def __new__(mcls, classname, bases, namespace:dict):
        """
        """
        _rel_parents: dict[str, AsyncDocument] = {}
        _rel_childs: dict[str, ChildRelDocument] = {}

        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, RelatedDocument):
                # unbundle relationship definition and getting field type mapping
                _rel_parents[attr_name] = ParentRelDocument(
                    doc_class=attr_value.doc_class,
                    on_delete=attr_value.on_delete
                )
                namespace[attr_name] = attr_value.dsl_type

        cls = super().__new__(mcls, classname, bases, namespace)
        setattr(cls, "_rel_parents", _rel_parents)
        setattr(cls, "_rel_childs", _rel_childs)
        return cls
    
    def __init__(cls, classname: Any, bases: Any, namespace: Any, **kw: Any):
        """
        """
        for field_name, rel_doc_def in cls._rel_parents.items():
            rel_doc_def.doc_class._rel_childs[cls] = ChildRelDocument(
                doc_class=cls, 
                field_name=field_name,
                on_delete=rel_doc_def.on_delete
            )

        super().__init__(cls, classname, bases)

    def __hash__(cls):
        """
        """
        return hash(cls.__name__)
    
    def __eq__(cls, other):
        """
        """
        return cls.__class__ == other.__class__ and cls.__name__ == cls.__name__


class AsyncRDSLDocument(AsyncDocument, metaclass=RDSLDocumentMeta):
    """
    AsyncDocument class extended by relationship managing mechanism
    """
    @classmethod
    def get_rel_childs(cls) -> dict[AsyncRDSLDocument, ChildRelDocument]:
        """
        """
        return getattr(cls, '_rel_childs', {})
    
    @classmethod
    def get_rel_parents(cls) -> dict[str, ParentRelDocument]:
        """
        """
        return getattr(cls, '_rel_parents', {})
    
    async def delete(self, using=None, index=None, **kwargs):
        rel_childs = self.get_rel_childs()
        
        for rel_def in rel_childs.values():
            child_doc_class = rel_def.doc_class
            field_name = rel_def.field_name
            
            # Getting related document 
            s:AsyncSearch = child_doc_class.search().filter('term', **{field_name: self.meta.id})
            count = await s.count()
            
            if count > 0:
                if rel_def.on_delete == 'check':
                    raise IntegrityError(
                        f"Cannot delete {self.__class__.__name__} with ID {self.meta.id}. "
                        f"Found {count} related {child_doc_class.__name__} documents."
                    )
                elif rel_def.on_delete == 'cascade': # tasks: (optimalize time execution) rebuild to delete_by_query
                    async for hit in s.scan():
                        doc = await child_doc_class.get(id=hit.meta.id)
                        await doc.delete()
                elif rel_def.on_delete == 'set_null': # tasks: (optimalize time execution) rebuild to update_by_query
                    async for hit in s.scan():
                        doc = await child_doc_class.get(id=hit.meta.id)
                        setattr(doc, field_name, None)
                        await doc.update(**{field_name: None})

        await super().delete(using=using, index=index, **kwargs)

    async def save(
        self,
        using: AsyncUsingType | None = None,
        index: str | None = None,
        validate_relations: bool = True,
        validate: bool = True,
        skip_empty: bool = True,
        return_doc_meta: bool = False,
        **kwargs
    ) -> Any:
        if validate_relations:
            await self._validate_relationship()

        return await super().save(
            using=using,
            index=index,
            validate=validate,
            skip_empty=skip_empty,
            return_doc_meta=return_doc_meta,
            **kwargs)
        
    async def _validate_relationship(self) -> None:
        """
        Validate existance of the document indicated in the relationship definitons 
        """
        rel_parents = self.get_rel_parents()

        for attr_name, rel_def in rel_parents.items():
            attr_value = getattr(self, attr_name, None)

            if attr_value is None:
                raise ValueError(
                    f"Related document ID not provided, excpected type: 'str', input: {type(attr_value)}"
                    )
            elif isinstance(attr_value, (list, tuple)):
                if not AsyncRDSLDocument._rel_docs_exists(rel_def.doc_class, attr_value):
                    raise IntegrityError(
                        f"Related index '{rel_def.doc_class._index._name}' document with _id: '{attr_value}' not found"
                    )
            else:
                if not await AsyncRDSLDocument._rel_doc_exist(rel_def.doc_class, attr_value):
                    raise IntegrityError(
                        f"Related index '{rel_def.doc_class._index._name}' documents with _id's: '{attr_value}' not found"
                    )

    @staticmethod
    async def _rel_doc_exist(doc_class: Type[RDSLDocumentMeta], doc_id:str) -> bool:
        """
        Validate existance of the indicated document.

        :arg doc_class: RDSLDocumentMeta class object
        :arg doc_id: document _id 
        """
        try:
            await doc_class.get(id=doc_id)
        except NotFoundError:
            return False
        else:
            return True

    @staticmethod
    async def _rel_docs_exists(doc_class: Type[RDSLDocumentMeta], doc_ids:list[str]) -> bool:
        """
        Validate existance of the indicated list of documents.
        
        :arg doc_class: RDSLDocumentMeta class object
        :arg doc_ids: list of document _id 
        """
        s:AsyncSearch = await doc_class.search().filter("term", _id=doc_ids)
        return s.count() == len(list)


