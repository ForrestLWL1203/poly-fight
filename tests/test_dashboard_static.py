from html.parser import HTMLParser
from pathlib import Path
import unittest


class _Node:
    def __init__(self, tag, attrs, line):
        self.tag = tag
        self.attrs = {name: value for name, value in attrs}
        self.line = line
        self.children = []


class _TemplateParser(HTMLParser):
    _VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.root = _Node("root", {}, 0)
        self._stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, attrs, self.getpos()[0])
        self._stack[-1].children.append(node)
        if tag not in self._VOID_TAGS:
            self._stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self._stack[-1].children.append(_Node(tag, attrs, self.getpos()[0]))

    def handle_endtag(self, tag):
        for index in range(len(self._stack) - 1, 0, -1):
            if self._stack[index].tag == tag:
                del self._stack[index:]
                return


def _walk(node):
    yield node
    for child in node.children:
        yield from _walk(child)


class DashboardStaticTests(unittest.TestCase):
    def test_vue_else_branches_have_adjacent_conditionals(self):
        template_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "index.html"
        parser = _TemplateParser()
        parser.feed(template_path.read_text())

        failures = []
        for parent in _walk(parser.root):
            previous = None
            for child in parent.children:
                has_else = "v-else" in child.attrs or "v-else-if" in child.attrs
                if has_else and not (previous and ("v-if" in previous.attrs or "v-else-if" in previous.attrs)):
                    failures.append(f"line {child.line}: <{child.tag}> has v-else without adjacent v-if")
                previous = child

        self.assertEqual([], failures)

    def test_decimal_input_patterns_accept_integer_values(self):
        template_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "index.html"
        parser = _TemplateParser()
        parser.feed(template_path.read_text())

        failures = []
        for node in _walk(parser.root):
            pattern = node.attrs.get("pattern")
            if pattern and "0-9" in pattern:
                if "\\\\." in pattern:
                    failures.append(f"line {node.line}: pattern has a double-escaped decimal point")
                self.assertRegex("2000", rf"^(?:{pattern})$")

        self.assertEqual([], failures)


if __name__ == "__main__":
    unittest.main()
