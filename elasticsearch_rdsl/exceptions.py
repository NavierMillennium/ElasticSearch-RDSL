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