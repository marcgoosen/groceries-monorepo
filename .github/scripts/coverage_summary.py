"""Render the Kover XML reports as a markdown coverage table."""

import glob
import sys
import xml.etree.ElementTree as ET

MARKER = "<!-- coverage-report -->"
REPORTS = "**/build/reports/kover/report.xml"


def counters(node):
    return {
        c.get("type"): (int(c.get("covered")), int(c.get("missed")))
        for c in node.findall("counter")
    }


def percentage(covered, missed):
    total = covered + missed
    return 100.0 * covered / total if total else 100.0


def cell(counts, kind):
    if kind not in counts:
        return "—"
    covered, missed = counts[kind]
    return f"{percentage(covered, missed):.1f}% ({covered}/{covered + missed})"


def module_of(path):
    return "/".join(path.split("/")[:2])


def main():
    reports = sorted(glob.glob(REPORTS, recursive=True))
    if not reports:
        print(f"{MARKER}\n## Coverage\n\nNo Kover reports were produced.")
        return 0

    rows = []
    totals = {"LINE": [0, 0], "BRANCH": [0, 0]}
    for report in reports:
        counts = counters(ET.parse(report).getroot())
        rows.append((module_of(report), counts))
        for kind in totals:
            if kind in counts:
                covered, missed = counts[kind]
                totals[kind][0] += covered
                totals[kind][1] += missed

    lines = [
        MARKER,
        "## Coverage",
        "",
        "| Module | Line | Branch |",
        "| --- | --- | --- |",
    ]
    for module, counts in rows:
        lines.append(f"| `{module}` | {cell(counts, 'LINE')} | {cell(counts, 'BRANCH')} |")
    lines.append(
        f"| **Total** | **{cell(totals, 'LINE')}** | **{cell(totals, 'BRANCH')}** |"
    )
    lines += ["", "Line coverage is gated at 60% per module by `koverVerify`."]
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
