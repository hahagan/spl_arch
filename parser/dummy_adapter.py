from spl_arch.command.replace_command import ReplaceCommand
from spl_arch.command.search_command import SearchCommand
from spl_arch.command.stats_command import StatsCommand
from spl_arch.parser.enums import CMD, SEARCH_KIND


class DummyAdapter:
    """将解析后的命令强制裁剪为临时演示命令
    """
    def _fetch_first_leaf_value(self, node):
        if node is None:
            return None
        if node.is_leaf:
            if node.kind == SEARCH_KIND.FULLTEXT:
                return node.text
            elif node.kind == SEARCH_KIND.CMP:
                return node.value
            else:
                return None
        else:
            return (self._fetch_first_leaf_value(node.left) or
                    self._fetch_first_leaf_value(node.right))

    def convert(self, parsed_command):
        cid = parsed_command.cid
        if cid == CMD.SEARCH:
            # 取任意叶子节点的一个字符串值 value，
            # 抛弃解析后的Search语义
            # 强制转为FieldCmpSearch(field='repo', op=<CMP_OP.EQ>, value=value)
            value = self._fetch_first_leaf_value(parsed_command.logical_tree)
            return SearchCommand('search', 'streaming', value)

        elif cid == CMD.REPLACE:
            # 只取第一个 with_op, in_field
            old, new = parsed_command.with_ops[0]
            in_field = None
            if parsed_command.in_fields:
                in_field = parsed_command.in_fields[0]
            return ReplaceCommand("replace", "streaming", old, new, in_field)

        elif cid == CMD.STATS:
            # 只取第一个 stats_agg_term, by_field
            term = parsed_command.stats_agg_terms[0]
            func = term.func
            func_f = term.func_field
            as_f = term.as_field
            by_f = None
            if parsed_command.by_fields:
                by_f = parsed_command.by_fields[0]
            return StatsCommand('stats', 'non_streaming', func, func_f, as_f, by_f)
        else:
            raise ValueError('Unrecognized parsed command')
