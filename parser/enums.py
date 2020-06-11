from enum import Enum, auto


class CMD(Enum):
    """
    命令ID
    """

    SEARCH = auto()
    REPLACE = auto()
    STATS = auto()


class SEARCH_KIND(Enum):
    """
    搜索语句中叶子节点表达式的种类
    """
    FULLTEXT = auto()
    CMP = auto()
    IN = auto()


class CMP_OP(Enum):
    """
    比较操作符
    """
    EQ = auto()
    NEQ = auto()
    LE = auto()
    LT = auto()
    GE = auto()
    GT = auto()


class LOGICAL_OP(Enum):
    """
    逻辑操作符
    """
    AND = auto()
    OR = auto()
    NOT = auto()
