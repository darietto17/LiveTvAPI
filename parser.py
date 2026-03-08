import os
import json
import urllib.request
import urllib.parse
import re
import time
from datetime import datetime
import xml.etree.ElementTree as ET

M3U_LIVE_URL = os.environ.get("M3U_LIVE_URL", "")
M3U_FILM_URL = os.environ.get("M3U_FILM_URL", "")
M3U_SERIES_URL = os.environ.get("M3U_SERIES_URL", "")
EPG_URL = os.environ.get("EPG_URL", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

DATA_DIR = "data"

def download_file(url, filename):
    print(f"[*] STEP: Downloading {filename} from {url[:50]}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as response, open(filename, 'wb') as out_file:
        chunk_size = 1024 * 1024
        downloaded = 0
        while True:
            chunk = response.read(chunk_size)
            if not chunk: break
            out_file.write(chunk)
            downloaded += len(chunk)
            if downloaded % (10 * 1024 * 1024) == 0:
                print(f"  ... {downloaded // (1024*1024)}MB downloaded")

def optimize_logo(url):
    if not url: return ""
    if "wsrv.nl" in url: return url
    encoded = urllib.parse.quote(url, safe='')
    return f"https://wsrv.nl/?url={encoded}&w=300&output=webp"

def load_tmdb_cache():
    cache_path = os.path.join(DATA_DIR, "tmdb_cache.json")
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_tmdb_cache(cache):
    cache_path = os.path.join(DATA_DIR, "tmdb_cache.json")
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)

def enrich_channels_with_tmdb(channels, is_series):
    cache = load_tmdb_cache()
    new_adds = 0
    print(f"Enriching {len(channels)} items with TMDB (is_series={is_series})...")
    
    for c in channels:
        title = c["name"]
        clean_title = re.sub(r'\(.*?\)|\[.*?\]', '', title)
        clean_title = re.sub(r'(1080p|720p|4K|FHD|HD|x264|H264|HEVC|ITA|ENG|Multi)', '', clean_title, flags=re.IGNORECASE)
        clean_title = clean_title.strip()
        
        if not clean_title:
            continue
            
        if clean_title in cache:
            c["tmdb"] = cache[clean_title]
            continue
            
        if not TMDB_API_KEY:
            continue
            
        query = urllib.parse.quote(clean_title)
        search_type = "tv" if is_series else "movie"
        url = f"https://api.themoviedb.org/3/search/{search_type}?api_key={TMDB_API_KEY}&query={query}&language=it-IT"
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as response:
                dat = json.loads(response.read().decode('utf-8'))
                
            res = dat.get("results", [])
            if res:
                first = res[0]
                year = first.get("release_date", "") if not is_series else first.get("first_air_date", "")
                tmdb_data = {
                    "overview": first.get("overview", ""),
                    "rating": first.get("vote_average", 0),
                    "year": year[:4] if year else "",
                    "poster": f"https://image.tmdb.org/t/p/w500{first['poster_path']}" if first.get("poster_path") else "",
                    "backdrop": f"https://image.tmdb.org/t/p/w780{first['backdrop_path']}" if first.get("backdrop_path") else ""
                }
                cache[clean_title] = tmdb_data
                c["tmdb"] = tmdb_data
            else:
                cache[clean_title] = None
                c["tmdb"] = None
                
            new_adds += 1
            if new_adds % 50 == 0:
                print(f"  ... enriched {new_adds} new items.")
                save_tmdb_cache(cache)
                
            time.sleep(0.05)
        except Exception as e:
            print(f"TMDB Error for {clean_title}: {e}")
            
    if new_adds > 0:
        save_tmdb_cache(cache)
    print(f"  [OK] TMDB Enrichment complete. New items added: {new_adds}")

def parse_m3u(filename, use_proxy=False):
    print(f"[*] STEP: Parsing {filename} (Proxy={use_proxy})...")
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
                    final_url = url
                    if use_proxy and "vixsrc.to" not in url:
                        encoded = urllib.parse.quote(url, safe='')
                        final_url = f"https://eproxy.rrinformatica.cloud/proxy/manifest.m3u8?url={encoded}"

                    channels.append({
                        "name": name,
                        "group": group,
                        "tvg_id": tvg_id,
                        "logo": optimize_logo(logo),
                        "url": final_url
                    })
                name, group, tvg_id, logo = "", "", "", ""
                
    print(f"  [OK] Found {len(channels)} valid entries in {filename}.")
    return channels

def generate_jsons(channels, subfolder):
    print(f"[*] STEP: Generating JSON files for '{subfolder}'...")
    out_dir = os.path.join(DATA_DIR, subfolder)
    os.makedirs(out_dir, exist_ok=True)
    
    groups = []
    seen = set()
    for c in channels:
        if c["group"] not in seen:
            groups.append(c["group"])
            seen.add(c["group"])
            
    with open(os.path.join(out_dir, "categories.json"), "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False)
        
    by_category = {}
    for c in channels:
        by_category.setdefault(c["group"], []).append(c)
        
    for group, items in by_category.items():
        safe_name = "".join(x if x.isalnum() else "_" for x in group)
        with open(os.path.join(out_dir, f"cat_{safe_name}.json"), "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)
    
    print(f"  ... Created {len(by_category)} category files for {subfolder}.")
            
    # Search DB: ultra light indexing
    search_db = []
    for c in channels:
        item = {"n": c["name"], "g": c["group"], "l": c["logo"], "u": c["url"], "t": c["tvg_id"]}
        if "tmdb" in c and c["tmdb"]:
            item["p"] = c["tmdb"].get("poster", "")
            item["b"] = c["tmdb"].get("backdrop", "")
            item["r"] = c["tmdb"].get("rating", 0)
        search_db.append(item)
        
    with open(os.path.join(out_dir, "channels.json"), "w", encoding="utf-8") as f:
        json.dump(search_db, f, separators=(',', ':'), ensure_ascii=False)
        
    print(f"{subfolder} JSON chunks generated.")

def parse_epg():
    print("[*] STEP: Beginning EPG Parsing...")
    if not EPG_URL:
        print("No EPG_URL provided.")
        return
        
    epg_file = "epg.xml"
    download_file(EPG_URL, epg_file)
    
    epg_dir = os.path.join(DATA_DIR, "epg")
    os.makedirs(epg_dir, exist_ok=True)
    
    context = ET.iterparse(epg_file, events=('end',))
    programs = {}
    epg_now = {}
    
    now = datetime.utcnow().timestamp()
    window_start = now - (2 * 3600)
    window_end = now + (48 * 3600)
    
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
                prog = {
                    "title": title,
                    "start": int(start_ts),
                    "stop": int(stop_ts)
                }
                programs.setdefault(channel, []).append(prog)
                
                if start_ts <= now and stop_ts >= now:
                    epg_now[channel] = prog
                    
            elem.clear()
            
    for channel, schedule in programs.items():
        safe_channel = "".join(x if x.isalnum() else "_" for x in channel)
        with open(os.path.join(epg_dir, f"{safe_channel}.json"), "w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False)
            
    with open(os.path.join(epg_dir, "epg_now.json"), "w", encoding="utf-8") as f:
        json.dump(epg_now, f, ensure_ascii=False)
            
    print(f"  [OK] EPG processing finished. {len(epg_now)} channels currently on-air.")
    print("EPG chunks and epg_now.json generated.")

def process_playlist(url, name):
    if url:
        filename = f"{name}.m3u"
        download_file(url, filename)
        
        use_proxy = name in ["film", "series"]
        channels = parse_m3u(filename, use_proxy=use_proxy)
        
        if name in ["film", "series"] and TMDB_API_KEY:
            enrich_channels_with_tmdb(channels, is_series=(name=="series"))
            
        generate_jsons(channels, name)
    else:
        print(f"Skipping {name}, no URL provided.")

def main():
    start_time = time.time()
    print(f"--- LiveTvAPI Parser Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    os.makedirs(DATA_DIR, exist_ok=True)
    if not (M3U_LIVE_URL or M3U_FILM_URL or M3U_SERIES_URL):
        print("ERROR: No M3U URLs provided in GitHub Actions secrets.")
        return
        
    process_playlist(M3U_LIVE_URL, "live")
    process_playlist(M3U_FILM_URL, "film")
    process_playlist(M3U_SERIES_URL, "series")
    
    if EPG_URL:
        parse_epg()
    
    end_time = time.time()
    print(f"\n[COMPLETE] All tasks finished in {end_time - start_time:.2f} seconds.")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
