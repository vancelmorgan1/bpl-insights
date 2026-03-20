#!/usr/bin/env python3
"""
BPL Insights — Dashboard Update Script
=======================================
Usage:
    python update_dashboard.py --adobe "path/to/adobe_export.csv"
    python update_dashboard.py --adobe "path/to/adobe_export.csv" --assets "path/to/x_snc_accel_asset.xlsx"

What it does:
  1. Parses your new Adobe Analytics CSV export (any date range)
  2. Optionally re-ingests a fresh Now Create asset table Excel export
  3. Rebuilds index.html with updated data and date labels
  4. You push index.html to GitHub — done

Requirements:
    pip install pandas openpyxl

Getting your Adobe CSV:
  - Open Adobe Analytics Workspace → your BPL report
  - Change the date range to your desired window
  - Top right → Export → CSV
  - Use that file as --adobe argument

Getting your Now Create Excel:
  - In Now Create, go to Assets → Export (x_snc_accel_asset table)
  - Use that file as --assets argument (optional — skips if not provided)
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict

# ── PARSE ADOBE CSV ──────────────────────────────────────────────────────────

def parse_adobe_csv(path):
    """Parse the Adobe Analytics CSV export. Returns (period_label, asset_downloads, sp_exports, recs, sp_clicks)."""
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    # Extract date range from header
    period_label = "Unknown period"
    for line in lines[:10]:
        m = re.search(r'Date:\s*(.+)', line)
        if m:
            period_label = m.group(1).strip().strip('"')
            break

    def parse_section(lines, start, end):
        result = {}
        for line in lines[start:end]:
            line = line.strip().strip('"')
            if not line or line.startswith('#') or line.startswith(','):
                continue
            parts = line.rsplit(',', 1)
            if len(parts) == 2:
                try:
                    result[parts[0].strip().strip('"')] = int(parts[1].strip().replace(',', ''))
                except ValueError:
                    pass
        return result

    # Find section boundaries
    section_starts = {}
    for i, line in enumerate(lines):
        if '# Asset - Download' in line:
            section_starts['downloads'] = i + 2
        elif '# Freeform table' in line and 'downloads' in section_starts:
            section_starts['downloads_end'] = i
        elif '# Recommendations Clicked' in line:
            section_starts['recs'] = i + 2
        elif '# Success Pack Clicks' in line:
            section_starts['sp_clicks'] = i + 2
        elif '# Success Pack Export' in line:
            section_starts['sp_exports'] = i + 2

    # Parse asset downloads
    raw_dl = parse_section(lines,
        section_starts.get('downloads', 11),
        section_starts.get('downloads_end', 63))

    asset_downloads = {}
    for k, v in raw_dl.items():
        m = re.match(r'bpl (?:authenticated|public):asset:(.+):(?:download|sign in to download)', k)
        if m:
            asset_downloads[m.group(1).strip()] = v

    # Parse exports
    raw_exp = parse_section(lines, section_starts.get('sp_exports', 256), len(lines))
    sp_exports = []
    for k, v in sorted(raw_exp.items(), key=lambda x: -x[1]):
        m = re.match(r'bpl authenticated:success pack:(.+):(export .+)', k)
        if m:
            sp_exports.append({'name': m.group(1).strip().title() + ' — ' + m.group(2).strip().title(), 'exports': v})

    # Parse recs
    raw_recs = parse_section(lines, section_starts.get('recs', 142), section_starts.get('sp_clicks', 192))
    recs = []
    for k, v in sorted(raw_recs.items(), key=lambda x: -x[1]):
        m = re.match(r'bpl authenticated:recommendations:(success pack|asset):(.+)', k)
        if m:
            recs.append({'name': m.group(2).strip().title(), 'type': m.group(1).strip(), 'clicks': v})

    # Parse sp clicks
    raw_spc = parse_section(lines, section_starts.get('sp_clicks', 192), section_starts.get('sp_exports', 256))
    sp_clicks = [{'name': k.replace('success pack:', '').strip().title(), 'clicks': v}
                 for k, v in sorted(raw_spc.items(), key=lambda x: -x[1])
                 if k.startswith('success pack:')]

    print(f"  Adobe period: {period_label}")
    print(f"  Asset download rows: {len(asset_downloads)}")
    print(f"  SP export rows: {len(sp_exports)}")
    print(f"  Rec rows: {len(recs)}")

    return period_label, asset_downloads, sp_exports, recs, sp_clicks


# ── PARSE NOW CREATE ASSET EXCEL ─────────────────────────────────────────────

KNOWN_SUITES = [
    'Governance, Risk, and Compliance', 'Environmental, Social, and Governance',
    'IT Service Management', 'IT Operations Management', 'IT Asset Management',
    'Customer Service Management', 'Employee Service Management', 'HR Service Delivery',
    'Strategic Portfolio Management', 'ServiceNow AI Platform', 'Now Assist', 'Now Intelligence',
    'Security Operations', 'Field Service Management', 'Telecommunications Service Management',
    'Automation Engine', 'App Engine', 'Financial Services', 'Health and Safety',
    'Healthcare and Life Sciences', 'Operational Technology', 'Public Sector Digital Services',
    'Source-to-Pay Operations', 'Accounts Payable Operations', 'Supplier Lifecycle Operations',
    'Supply Chain Management', 'Enterprise Architecture',
]
KNOWN_SUITES_S = sorted(KNOWN_SUITES, key=len, reverse=True)

SUITE_TO_BUCKET = {
    'IT Service Management': 'ITSM', 'IT Operations Management': 'CMDB/CSDM',
    'IT Asset Management': 'HAM/SAM', 'Customer Service Management': 'CSM',
    'Employee Service Management': 'ESM', 'HR Service Delivery': 'HRSD',
    'Strategic Portfolio Management': 'SPM/PPM',
    'Governance, Risk, and Compliance': 'GRC/IRM',
    'Environmental, Social, and Governance': 'GRC/IRM',
    'ServiceNow AI Platform': 'Now Assist/AI', 'Now Assist': 'Now Assist/AI',
    'Now Intelligence': 'Now Assist/AI', 'Security Operations': 'SecOps',
    'Field Service Management': 'FSM', 'Telecommunications Service Management': 'Telecom',
    'Automation Engine': 'Platform', 'App Engine': 'Platform',
    'Enterprise Architecture': 'Platform', 'Financial Services': 'Financial Services',
    'Health and Safety': 'ESM', 'Healthcare and Life Sciences': 'Industry',
    'Operational Technology': 'Industry', 'Public Sector Digital Services': 'Industry',
    'Source-to-Pay Operations': 'Financial Services',
    'Accounts Payable Operations': 'Financial Services',
    'Supplier Lifecycle Operations': 'Financial Services',
    'Supply Chain Management': 'Financial Services',
}

def parse_suites(val):
    if not val or str(val).strip() == 'nan':
        return []
    remaining = str(val).strip()
    result = []
    while remaining:
        matched = False
        for suite in KNOWN_SUITES_S:
            if remaining.startswith(suite):
                result.append(suite)
                remaining = remaining[len(suite):].lstrip(', ')
                matched = True
                break
        if not matched:
            result.append(remaining)
            break
    return result

def parse_assets_excel(path):
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas required. Run: pip install pandas openpyxl")
        sys.exit(1)

    df = pd.read_excel(path)
    print(f"  Now Create rows: {len(df)}")

    assets = []
    for _, row in df.iterrows():
        suites = parse_suites(row.get('Parent Product Suites', ''))
        buckets = list(dict.fromkeys([SUITE_TO_BUCKET.get(s, 'Other') for s in suites]))
        primary = buckets[0] if buckets else 'Platform'
        assets.append({
            'id': str(row.get('Number', '')),
            'n': str(row.get('Name', '')),
            'b': primary,
            't': str(row.get('Asset Type', '')) if str(row.get('Asset Type', '')) != 'nan' else '',
            's': suites[0] if suites else '',
            'ldl': int(row['Total downloads']) if pd.notna(row.get('Total downloads')) else 0,
            'lv': int(row['Total views']) if pd.notna(row.get('Total views')) else 0,
            'pdl': 0,   # will be filled from Adobe
            'adobe': False,
        })
    return assets


# ── MATCH ADOBE → ASSETS ────────────────────────────────────────────────────

def normalize(s):
    return re.sub(r'[^a-z0-9]', ' ', str(s).lower()).strip()

def match_assets_to_adobe(assets, adobe_dl):
    adobe_lookup = {normalize(k): v for k, v in adobe_dl.items()}
    matched = 0
    for a in assets:
        norm = normalize(a['n'])
        val = adobe_lookup.get(norm)
        if val is None:
            for k, v in adobe_lookup.items():
                if norm in k or k in norm:
                    val = v
                    break
        if val is not None:
            a['pdl'] = val
            a['adobe'] = True
            matched += 1
        else:
            a['pdl'] = 0
            a['adobe'] = False
    print(f"  Matched {matched}/{len(assets)} assets to Adobe export")
    return assets


# ── BUILD BENCHMARKS ─────────────────────────────────────────────────────────

def build_benchmarks(assets):
    all_dls = sorted(a['ldl'] for a in assets)
    n = len(all_dls)
    global_stats = {
        'avg': round(sum(all_dls) / n) if n else 0,
        'median': all_dls[n // 2] if n else 0,
        'p75': all_dls[int(n * 0.75)] if n else 0,
        'p90': all_dls[int(n * 0.90)] if n else 0,
        'total': n,
    }
    bucket_map = defaultdict(list)
    for a in assets:
        bucket_map[a['b']].append(a)
    bucket_stats = {}
    for b, items in bucket_map.items():
        dls = sorted(x['ldl'] for x in items)
        ni = len(dls)
        bucket_stats[b] = {
            'count': ni,
            'total_dl': sum(dls),
            'avg_dl': round(sum(dls) / ni) if ni else 0,
            'median_dl': dls[ni // 2] if ni else 0,
            'p75_dl': dls[int(ni * 0.75)] if ni else 0,
            'period_dl': sum(x['pdl'] for x in items),
            'adobe_count': sum(1 for x in items if x['adobe']),
        }
    type_map = defaultdict(list)
    for a in assets:
        if a['t']:
            type_map[a['t']].append(a['ldl'])
    type_avgs = {t: {'avg': round(sum(dls) / len(dls)), 'count': len(dls)}
                 for t, dls in type_map.items()}
    return {'global': global_stats, 'by_bucket': bucket_stats, 'by_type': type_avgs}


# ── INJECT INTO index.html ───────────────────────────────────────────────────

def inject_into_html(assets, bench, period_label, total_period_dl):
    html_path = os.path.join(os.path.dirname(__file__), 'index.html')
    if not os.path.exists(html_path):
        print(f"ERROR: index.html not found at {html_path}")
        sys.exit(1)

    content = open(html_path, 'r', encoding='utf-8').read()

    # Replace ASSETS block
    assets_json = json.dumps(assets, separators=(',', ':'))
    start_marker = '/* __ASSETS_START__ */\nconst ASSETS = '
    end_marker = ';\n/* __ASSETS_END__ */'
    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)
    if start_idx == -1 or end_idx == -1:
        print("ERROR: Could not find ASSETS markers in index.html")
        sys.exit(1)
    content = content[:start_idx] + start_marker + assets_json + end_marker + content[end_idx + len(end_marker):]

    # Replace BENCH block
    bench_json = json.dumps(bench, separators=(',', ':'))
    start_marker2 = '/* __BENCH_START__ */\nconst BENCH = '
    end_marker2 = ';\n/* __BENCH_END__ */'
    start_idx2 = content.find(start_marker2)
    end_idx2 = content.find(end_marker2)
    if start_idx2 == -1 or end_idx2 == -1:
        print("ERROR: Could not find BENCH markers in index.html")
        sys.exit(1)
    content = content[:start_idx2] + start_marker2 + bench_json + end_marker2 + content[end_idx2 + len(end_marker2):]

    # Update period badge
    content = re.sub(
        r'<!-- __PERIOD_LABEL__ -->',
        period_label.replace('–', '&ndash;').replace('—', '&mdash;'),
        content
    )

    # Update all occurrences of the old Adobe period label in text
    content = re.sub(
        r'Oct 1, 2025\s*[–-]\s*Mar 20, 2026',
        period_label,
        content
    )
    content = re.sub(
        r'Oct 1, 2025\s*&ndash;\s*Mar 20, 2026',
        period_label.replace('–', '&ndash;'),
        content
    )
    content = re.sub(
        r'Oct\s*&ndash;\s*Mar',
        period_label[:period_label.find(',')].split()[0] + '&ndash;' + period_label.split('–')[-1].strip().split()[0] if '–' in period_label else 'Period',
        content
    )

    # Update total period downloads stat tile
    content = re.sub(
        r'(<div class="stat-lbl">Adobe Downloads)[^<]*(</div>\s*<div class="stat-val">)[^<]*(</div>)',
        lambda m: f'{m.group(1)} ({period_label}){m.group(2)}{total_period_dl:,}{m.group(3)}',
        content
    )

    # Update adobe_count in note
    total_adobe = sum(1 for a in assets if a['adobe'])
    content = re.sub(
        r'Period download data \(Oct[^)]*\) covers \d+ assets',
        f'Period download data ({period_label}) covers {total_adobe} assets',
        content
    )
    content = re.sub(
        r'Adobe Analytics \(Oct[^)]*\)',
        f'Adobe Analytics ({period_label})',
        content
    )

    # Update Now Create snapshot date if it appears
    # (leave it — it reflects the actual Now Create export date)

    # Stamp refresh date
    from datetime import datetime
    stamp = f"Data refreshed {datetime.now().strftime('%b %d, %Y')}"
    content = re.sub(r'Data refreshed \w+ \d+, \d+', stamp, content)
    content = content.replace('<!-- __REFRESH_DATE__ -->', stamp)

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"  index.html updated ({len(content):,} chars)")


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Update BPL Insights dashboard')
    parser.add_argument('--adobe', required=True,
                        help='Path to Adobe Analytics CSV export')
    parser.add_argument('--assets', required=False,
                        help='Path to Now Create asset table Excel export (optional — uses existing data if omitted)')
    args = parser.parse_args()

    if not os.path.exists(args.adobe):
        print(f"ERROR: Adobe CSV not found: {args.adobe}")
        sys.exit(1)

    print("\n── Step 1: Parsing Adobe Analytics CSV ──")
    period_label, adobe_dl, sp_exports, recs, sp_clicks = parse_adobe_csv(args.adobe)

    if args.assets:
        if not os.path.exists(args.assets):
            print(f"ERROR: Asset Excel not found: {args.assets}")
            sys.exit(1)
        print("\n── Step 2: Parsing Now Create asset table ──")
        assets = parse_assets_excel(args.assets)
    else:
        print("\n── Step 2: Using existing asset data from index.html ──")
        html_path = os.path.join(os.path.dirname(__file__), 'index.html')
        content = open(html_path, 'r', encoding='utf-8').read()
        m = re.search(r'/\* __ASSETS_START__ \*/\nconst ASSETS = (\[.*?\]);', content, re.DOTALL)
        if not m:
            print("ERROR: Could not find ASSETS block in index.html. Run with --assets to provide a fresh export.")
            sys.exit(1)
        assets = json.loads(m.group(1))
        # Reset adobe flags
        for a in assets:
            a['pdl'] = 0
            a['adobe'] = False
        print(f"  Loaded {len(assets)} assets from existing index.html")

    print("\n── Step 3: Matching assets to Adobe download data ──")
    assets = match_assets_to_adobe(assets, adobe_dl)
    total_period_dl = sum(a['pdl'] for a in assets)
    print(f"  Total period downloads: {total_period_dl:,}")

    print("\n── Step 4: Building benchmarks ──")
    bench = build_benchmarks(assets)
    print(f"  Benchmarks built for {len(bench['by_bucket'])} product areas")

    print("\n── Step 5: Updating index.html ──")
    inject_into_html(assets, bench, period_label, total_period_dl)

    print(f"""
── Done ──────────────────────────────────────────
  Period:          {period_label}
  Assets:          {len(assets)}
  Adobe matched:   {sum(1 for a in assets if a['adobe'])}
  Period downloads:{total_period_dl:,}

  Next step: git add index.html && git commit -m "Update dashboard: {period_label}" && git push
──────────────────────────────────────────────────
""")


if __name__ == '__main__':
    main()
