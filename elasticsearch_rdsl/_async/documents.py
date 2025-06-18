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
from exceptions import (
    IntegrityError
) 



class RelatedDocument:
    def __init__(
            self,
            doc_class: Type[AsyncDocument], 
            field_name: str = "_id",
            on_delete: str = "check",
            es_dsl_type: Field = Keyword(),
    ):
        """
        :arg doc_class: related class ref/name
        :arg field_name: field name from related document with the 'foreign key'
        :arg on_delete: document delete strategies: 'check', 'cascade', 'set_null'
        :arg es_dsl_type: (default: 'elasticsearch.dsl.Keyword()') target field mapping in the elasticsearch database 
        """
        self.es_dsl_type = es_dsl_type
        self.doc_class = doc_class
        self.field_name = field_name
        self.on_delete = on_delete


class RelationshipMeta(type(AsyncDocument)):
    def __new__(mcls, name, bases, namespace):
        _relationships = {}
        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, RelatedDocument):
                # unbundle relationship definition and getting field type mapping
                _relationships[attr_name] = attr_value
                namespace[attr_name] = attr_value.es_dsl_type

        cls = super().__new__(mcls, name, bases, namespace)
        setattr(cls, "_relationships", _relationships)
        return cls


class RDSLDocument(AsyncDocument, metaclass=RelationshipMeta):
    """
    AsyncDocument class extended by relationship managing mechanism
    """
    @classmethod
    def get_relationship(cls) -> dict[str, RelatedDocument]:
        return getattr(cls, '_relationships', {})
    
    async def delete(self, using=None, index=None, **kwargs):
        #
        relationships = self.get_relationship()
        
        for _, rel_def in relationships.items():
            related_class = rel_def.doc_class
            field_name = rel_def.field_name
            
            # Wyszukaj dokumenty powiązane
            s:AsyncSearch = await related_class.search().filter('term', **{field_name: self.meta.id})
            count = s.count()
            
            if count > 0:
                if rel_def.on_delete == 'check':
                    raise IntegrityError(
                        f"Cannot delete {self.__class__.__name__} with ID {self.meta.id}. "
                        f"Found {count} related {related_class.__name__} documents."
                    )
                elif rel_def.on_delete == 'cascade':
                    for hit in s.scan():
                        doc:AsyncDocument = related_class.get(id=hit.meta.id)
                        doc.delete()
                elif rel_def.on_delete == 'set_null':
                    for hit in s.scan():
                        doc:AsyncDocument = related_class.get(id=hit.meta.id)
                        setattr(doc, field_name, None)
                        doc.save()

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
        """
        :arg validate_relations: 
        """
        if validate_relations:
            pass

        return await super().save(
            using=using,
            index=index,
            validate=validate,
            skip_empty=skip_empty,
            return_doc_meta=return_doc_meta,
            **kwargs)
        
    def _validate_relationship(self) -> bool:
        """
        Validate existance of the document indicated in the relationship definitons 
        """
        relationships = self.get_relationship()

        for attr_name, rel_def in relationships.items():
            attr_value = getattr(self, attr_name, None)

            if attr_value is not None:
                if isinstance(attr_value, (list, tuple)):
                    if not RDSLDocument._rel_docs_exists(rel_def.doc_class, attr_value):
                        raise IntegrityError(
                            f"Related index '{rel_def.doc_class._index._name}' document with _id: '{attr_value}' not found"
                        )
                else:
                    if not RDSLDocument._rel_doc_exist(rel_def.doc_class, attr_value):
                        raise IntegrityError(
                            f"Related index '{rel_def.doc_class._index._name}' documents with _id's: '{attr_value}' not found"
                        )

    @staticmethod
    async def _rel_doc_exist(doc_class: Type[AsyncDocument], doc_id:str) -> bool:
        """
        Validate existance of the indicated document.

        :arg doc_class: AsyncDocument class object
        :arg doc_id: document _id 
        """
        try:
            await doc_class.get(id=doc_id)
        except NotFoundError:
            return False
        else:
            return True

    @staticmethod
    async def _rel_docs_exists(doc_class: Type[AsyncDocument], doc_ids:list[str]) -> bool:
        """
        Validate existance of the indicated list of documents.
        
        :arg doc_class: AsyncDocument class object
        :arg doc_ids: list of document _id 
        """
        s:AsyncSearch = await doc_class.search().filter("term", _id=doc_ids)
        return s.count() == len(list)


