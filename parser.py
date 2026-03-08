import os
import json
import urllib.request
import re
from datetime import datetime
import xml.etree.ElementTree as ET

M3U_LIVE_URL = os.environ.get("M3U_LIVE_URL", "")
M3U_FILM_URL = os.environ.get("M3U_FILM_URL", "")
M3U_SERIES_URL = os.environ.get("M3U_SERIES_URL", "")
EPG_URL = os.environ.get("EPG_URL", "")

DATA_DIR = "data"

def download_file(url, filename):
    print(f"Downloading {url} to {filename}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response, open(filename, 'wb') as out_file:
        data = response.read()
        out_file.write(data)

def parse_m3u(filename):
    print(f"Parsing {filename}...")
    group_regex = re.compile(r'group-title="([^"]*)"')
    tvg_id_regex = re.compile(r'tvg-id="([^"]*)"')
    logo_regex = re.compile(r'tvg-logo="([^"]*)"')
    
    channels = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        name = ""
        group = ""
        tvg_id = ""
        logo = ""
        
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith("#EXTINF:"):
                m_group = group_regex.search(line)
                group = m_group.group(1) if m_group else "Uncategorized"
                
                m_id = tvg_id_regex.search(line)
                tvg_id = m_id.group(1) if m_id else ""
                
                m_logo = logo_regex.search(line)
                logo = m_logo.group(1) if m_logo else ""
                
                parts = line.split(",", 1)
                name = parts[1].strip() if len(parts) > 1 else ""
                
            elif not line.startswith("#"):
                url = line
                if url and name:
                    channels.append({
                        "name": name,
                        "group": group,
                        "tvg_id": tvg_id,
                        "logo": logo,
                        "url": url
                    })
                name, group, tvg_id, logo = "", "", "", ""
                
    return channels

def generate_jsons(channels, subfolder):
    print(f"Generating JSON blocks for {subfolder}...")
    out_dir = os.path.join(DATA_DIR, subfolder)
    os.makedirs(out_dir, exist_ok=True)
    
    # 1. Get unique ordered groups
    groups = []
    seen = set()
    for c in channels:
        if c["group"] not in seen:
            groups.append(c["group"])
            seen.add(c["group"])
            
    with open(os.path.join(out_dir, "categories.json"), "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False)
        
    # 2. Split by category
    by_category = {}
    for c in channels:
        by_category.setdefault(c["group"], []).append(c)
        
    for group, items in by_category.items():
        safe_name = "".join(x if x.isalnum() else "_" for x in group)
        with open(os.path.join(out_dir, f"cat_{safe_name}.json"), "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)
            
    # 3. Save a combined slim searchable database
    search_db = [{"n": c["name"], "g": c["group"], "l": c["logo"], "u": c["url"], "t": c["tvg_id"]} for c in channels]
    with open(os.path.join(out_dir, "channels.json"), "w", encoding="utf-8") as f:
        json.dump(search_db, f, separators=(',', ':'), ensure_ascii=False)
        
    print(f"{subfolder} JSON chunks generated.")

def parse_epg():
    print("Parsing EPG XMLTV...")
    if not EPG_URL:
        print("No EPG_URL provided.")
        return
        
    epg_file = "epg.xml"
    download_file(EPG_URL, epg_file)
    
    epg_dir = os.path.join(DATA_DIR, "epg")
    os.makedirs(epg_dir, exist_ok=True)
    
    context = ET.iterparse(epg_file, events=('end',))
    programs = {}
    
    now = datetime.utcnow().timestamp()
    window_start = now - (2 * 3600)  # 2 hours ago
    window_end = now + (48 * 3600)   # 48 hours ahead
    
    for event, elem in context:
        if elem.tag == 'programme':
            channel = elem.get('channel', '')
            start_str = elem.get('start', '')
            stop_str = elem.get('stop', '')
            
            title_elem = elem.find('title')
            title = title_elem.text if title_elem is not None else ""
            
            try:
                start_ts = datetime.strptime(start_str.split(' ')[0], '%Y%m%d%H%M%S').timestamp()
                stop_ts = datetime.strptime(stop_str.split(' ')[0], '%Y%m%d%H%M%S').timestamp()
            except:
                start_ts, stop_ts = 0, 0
                
            if start_ts <= window_end and stop_ts >= window_start:
                programs.setdefault(channel, []).append({
                    "title": title,
                    "start": int(start_ts),
                    "stop": int(stop_ts)
                })
                
            elem.clear()
            
    for channel, schedule in programs.items():
        safe_channel = "".join(x if x.isalnum() else "_" for x in channel)
        with open(os.path.join(epg_dir, f"{safe_channel}.json"), "w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False)
            
    print("EPG chunks generated.")

def process_playlist(url, name):
    if url:
        filename = f"{name}.m3u"
        download_file(url, filename)
        channels = parse_m3u(filename)
        generate_jsons(channels, name)
    else:
        print(f"Skipping {name}, no URL provided.")

def main():
    if not (M3U_LIVE_URL or M3U_FILM_URL or M3U_SERIES_URL):
        print("ERROR: No M3U URLs provided in GitHub Actions secrets.")
        return
        
    process_playlist(M3U_LIVE_URL, "live")
    process_playlist(M3U_FILM_URL, "film")
    process_playlist(M3U_SERIES_URL, "series")
    
    if EPG_URL:
        parse_epg()

if __name__ == "__main__":
    main()
