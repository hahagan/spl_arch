from antlr4 import CommonTokenStream, ParseTreeWalker, InputStream

if __name__ is not None and "." in __name__:
    from .SPLParser import SPLParser
    from .SPLLexer import SPLLexer
    from .pipeliner import Pipeliner
else:
    from SPLParser import SPLParser
    from SPLLexer import SPLLexer
    from pipeliner import Pipeliner


class AntlrParser:
    def __init__(self):
        self._pipliner = Pipeliner()
        self._walker = ParseTreeWalker()

    def validate(self):
        raise NotImplementedError

    def parse(self, cmd):
        lexer = SPLLexer(InputStream(cmd))
        token_stream = CommonTokenStream(lexer)
        parser = SPLParser(token_stream)
        tree = parser.pipeline()
        self._pipliner.clear()
        self._walker.walk(self._pipliner, tree)
        return self._pipliner.cmds


if __name__ == "__main__":
    parser = AntlrParser()
    for c in parser.parse("search test | stats max(f1) as 最大值 by f2"):
        print(c)
