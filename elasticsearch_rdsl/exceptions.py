from typing import TypedDict

class ErrDetail(TypedDict):
    index_name: str 
    exception_type: str
    detail: str

class IndexInitError(Exception):
    """
        For re-raising exception from elasticsearch client module 
    """
    def __init__(self, detail:list[dict], msg:str = "Error during index initialization"):
        self.detail = detail 
        self.msg = msg
        super().__init__(self.msg, self.detail)


class ConfictIndexDefinition(Exception):
    """
        Index with the same name already defined 
    """
    pass


class IndexNotRegisteredError(Exception):
    """
        The indicated index not found in the register
    """
    pass


class IntegrityError(Exception):
    """
        Relationship constraints error
    """
    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(self.detail)