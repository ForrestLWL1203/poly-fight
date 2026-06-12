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


def _classes(node):
    return set((node.attrs.get("class") or "").split())


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

    def test_wallet_quarantine_action_lives_at_row_end(self):
        template_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "index.html"
        parser = _TemplateParser()
        template_text = template_path.read_text()
        parser.feed(template_text)

        favorite_cells = [node for node in _walk(parser.root) if "favorite-cell" in _classes(node)]
        end_action_cells = [node for node in _walk(parser.root) if "wallet-end-action-cell" in _classes(node)]

        self.assertNotIn("⛔", template_text)
        self.assertTrue(favorite_cells)
        self.assertTrue(end_action_cells)
        for cell in favorite_cells:
            nested_classes = set()
            for child in _walk(cell):
                nested_classes.update(_classes(child))
            self.assertNotIn("wallet-quarantine-btn", nested_classes)
            self.assertNotIn("wallet-unquarantine-btn", nested_classes)

        end_action_classes = set()
        for cell in end_action_cells:
            for child in _walk(cell):
                end_action_classes.update(_classes(child))
        self.assertIn("wallet-quarantine-btn", end_action_classes)
        self.assertIn("wallet-quarantine-bar", end_action_classes)
        self.assertIn("wallet-unquarantine-btn", end_action_classes)

    def test_runner_stake_ratio_input_has_no_placeholder(self):
        template_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "index.html"
        parser = _TemplateParser()
        parser.feed(template_path.read_text())

        ratio_inputs = [
            node
            for node in _walk(parser.root)
            if node.tag == "input" and node.attrs.get("v-model") == "runnerStakeRatioInput"
        ]

        self.assertEqual(1, len(ratio_inputs))
        self.assertNotIn("placeholder", ratio_inputs[0].attrs)

    def test_follow_detail_slippage_uses_our_price_minus_wallet_price(self):
        root = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static"
        template = (root / "index.html").read_text()
        app = (root / "app.js").read_text()

        self.assertIn("<td>{{ price(legWalletEntryPrice(leg)) }}</td>", template)
        self.assertIn("<td>{{ price(signalAverageEntry(signal)) }}</td>", template)
        self.assertNotIn("<td>{{ price(leg.wallet_fill_price || leg.wallet_avg_price) }}</td>", template)

        start = app.index("legSlippageValue(leg)")
        end = app.index("legSlippageText(leg)", start)
        body = app[start:end]
        self.assertIn("const walletEntry = this.legWalletEntryPrice(leg);", body)
        self.assertIn("const ourEntry = Number(leg.our_entry_price);", body)
        self.assertIn("return ourEntry - walletEntry;", body)

    def test_follow_and_event_lists_refresh_without_loading_overlay(self):
        root = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static"
        template = (root / "index.html").read_text()
        styles = (root / "styles.css").read_text()

        self.assertNotIn("panel-loading-mask", template)
        self.assertNotIn(".panel-loading-mask", styles)
        self.assertNotIn(".panel-loading-card", styles)

    def test_overview_shows_total_account_equity(self):
        template_path = Path(__file__).resolve().parents[1] / "poly_fight" / "dashboard" / "static" / "index.html"
        template = template_path.read_text()

        self.assertIn("总体资金", template)
        self.assertIn("可动用余额 + 当前持仓花费成本", template)
        self.assertIn("money(overview.account_total_equity_usdc)", template)

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
