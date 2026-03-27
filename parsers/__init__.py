from .supplier1 import Supplier1Parser
from .supplier2 import Supplier2Parser
from .supplier3 import Supplier3Parser
from .supplier4 import Supplier4Parser

def get_parser(supplier_key: str):
    mapping = {
        "supplier1": Supplier1Parser,
        "supplier2": Supplier2Parser,
        "supplier3": Supplier3Parser,
        "supplier4": Supplier4Parser,
    }
    return mapping[supplier_key]
