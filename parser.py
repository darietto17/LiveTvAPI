import os
import json
import requests
import re
import time
from datetime import datetime
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import gzip

# Global lock for thread-safe cache access
cache_lock = threading.Lock()

M3U_LIVE_URL = os.environ.get("M3U_LIVE_URL", "")
M3U_FILM_URL = os.environ.get("M3U_FILM_URL", "")
M3U_SERIES_URL = os.environ.get("M3U_SERIES_URL", "")
EPG_URL = os.environ.get("EPG_URL", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
PROXY_URL = "https://script.google.com/macros/s/AKfycbybNHpTwVofPgSEg2I433cDmHbB7Nl1azrA5Xtt1OWPSaeXJkoRZl3pU0LFSiof49U_/exec"
NOMEREPO = os.environ.get("NOMEREPO", "LiveTvAPI") # Default or from env

# Global session for connection pooling
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

def load_rules():
    rules_path = "user_rules.json"
    if os.path.exists(rules_path):
        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[!] Error loading user_rules.json: {e}")
    return {"items": {}, "order": [], "groupsOrder": []}

def apply_rules(channels, rules):
    # 1. Apply item-level overrides (name, group, logo, hidden)
    mapped = []
    items_rules = rules.get("items", {})
    
    for ch in channels:
        orig_name = ch["name"]
        rule = items_rules.get(orig_name)
        if rule:
            if rule.get("hidden"):
                continue
            ch["name"] = rule.get("name", ch["name"])
            ch["group"] = rule.get("group", ch["group"])
            ch["logo"] = rule.get("logo", ch["logo"])
            # Note: url override is usually not needed here as links are dynamic, 
            # but we keep the name matching consistent.
        mapped.append(ch)
    
    # 2. Sort channels based on global 'order' if present
    order = rules.get("order", [])
    if order:
        order_map = {name: i for i, name in enumerate(order)}
        # We sort by originalName (which is stored in 'name' at this point before override if we want consistency, 
        # but the rule usually refers to the original name part from M3U).
        # To be safe, we should have kept 'originalName' in the dict.
        # Let's adjust parse_m3u to include it.
        mapped.sort(key=lambda x: order_map.get(x.get("originalName", x["name"]), 999999))
    
    return mapped

def download_file(url, filename):
    print(f"[*] STEP: Downloading {filename} from {url[:50]}...")
    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(filename, 'wb') as f:
                downloaded = 0
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if downloaded % (10 * 1024 * 1024) == 0:
                            print(f"  ... {downloaded // (1024*1024)}MB downloaded")
        print(f"  [OK] Finished downloading {filename}")
    except Exception as e:
        print(f"[!] Error downloading {filename}: {e}")
        raise

def optimize_logo(url):
    if not url: return ""
    if "wsrv.nl" in url: return url
    encoded = requests.utils.quote(url)
    return f"https://wsrv.nl/?url={encoded}&w=300&output=webp"

def load_tmdb_cache():
    cache_path = os.path.join(DATA_DIR, "tmdb_cache.json")
    with cache_lock:
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"[!] Error loading cache: {e}")
                return {}
    return {}

def save_tmdb_cache(cache):
    cache_path = os.path.join(DATA_DIR, "tmdb_cache.json")
    with cache_lock:
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False)
        except Exception as e:
            print(f"[!] Error saving cache: {e}")

def fetch_tmdb_info(c, is_series, cache):
    title = c["name"]
    clean_title = re.sub(r'\(.*?\)|\[.*?\]', '', title)
    clean_title = re.sub(r'(1080p|720p|4K|FHD|HD|x264|H264|HEVC|ITA|ENG|Multi)', '', clean_title, flags=re.IGNORECASE).strip()
    
    if not clean_title or not TMDB_API_KEY: return None, None
    if clean_title in cache: return clean_title, cache[clean_title]
    
    query = requests.utils.quote(clean_title)
    search_type = "tv" if is_series else "movie"
    url = f"https://api.themoviedb.org/3/search/{search_type}?api_key={TMDB_API_KEY}&query={query}&language=it-IT"
    
    try:
        r = session.get(url, timeout=10)
        dat = r.json()
        res = dat.get("results", [])
        if res:
            first = res[0]
            year_key = "first_air_date" if is_series else "release_date"
            year = first.get(year_key, "")
            tmdb_data = {
                "overview": first.get("overview", ""),
                "rating": first.get("vote_average", 0),
                "year": year[:4] if year else "",
                "poster": f"https://image.tmdb.org/t/p/w500{first['poster_path']}" if first.get("poster_path") else "",
                "backdrop": f"https://image.tmdb.org/t/p/w780{first['backdrop_path']}" if first.get("backdrop_path") else ""
            }
            return clean_title, tmdb_data
        return clean_title, None
    except Exception as e:
        print(f"TMDB Error for {clean_title}: {e}")
        return clean_title, None

def enrich_channels_with_tmdb(channels, is_series):
    cache = load_tmdb_cache()
    print(f"Enriching {len(channels)} items with TMDB (is_series={is_series}) using Threads...")
    
    to_fetch = [c for c in channels if re.sub(r'\(.*?\)|\[.*?\]', '', c["name"]).strip() not in cache]
    
    # Cap TMDB fetches per run to prevent timeout
    FETCH_LIMIT = 500
    if len(to_fetch) > FETCH_LIMIT:
        print(f"  [!] Limiting TMDB fetches to {FETCH_LIMIT} for faster execution.")
        to_fetch = to_fetch[:FETCH_LIMIT]
        
    new_adds = 0

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_tmdb_info, c, is_series, cache): c for c in to_fetch}
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    title, data = res
                    if data:
                        with cache_lock:
                            cache[title] = data
                        new_adds += 1
                        if new_adds % 50 == 0:
                            print(f"  ... enriched {new_adds} items")
                            save_tmdb_cache(cache)
            except Exception as e:
                print(f"  [!] Error processing TMDB result: {e}")

    # Assign from cache
    for c in channels:
        clean = re.sub(r'\(.*?\)|\[.*?\]', '', c["name"]).strip()
        c["tmdb"] = cache.get(clean)

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
        lines = f.readlines()
        
    name = ""
    group = ""
    tvg_id = ""
    logo = ""
    
    new_lines = []
    
    for line in lines:
        clean_line = line.strip()
        if not clean_line:
            new_lines.append(line)
            continue
        
        if clean_line.startswith("#EXTINF:"):
            m_group = group_regex.search(clean_line)
            group = m_group.group(1) if m_group else "Uncategorized"
            
            m_id = tvg_id_regex.search(clean_line)
            tvg_id = m_id.group(1) if m_id else ""
            
            m_logo = logo_regex.search(clean_line)
            logo = m_logo.group(1) if m_logo else ""
            
            parts = clean_line.split(",", 1)
            name = parts[1].strip() if len(parts) > 1 else ""
            new_lines.append(line)
            
        elif not clean_line.startswith("#"):
            url = clean_line
            if url and name:
                final_url = url
                if use_proxy:
                    PROXY_PREFIX = "https://eproxy.rrinformatica.cloud/proxy/manifest.m3u8?url="
                    if not url.startswith(PROXY_PREFIX):
                        encoded = requests.utils.quote(url, safe='')
                        final_url = f"{PROXY_PREFIX}{encoded}"
                    else:
                        final_url = url
                
                # Append the potentially proxied URL instead of the original one
                new_lines.append(final_url + "\n")

                channels.append({
                    "originalName": name,
                    "name": name,
                    "group": group,
                    "tvg_id": tvg_id,
                    "logo": optimize_logo(logo),
                    "url": final_url
                })
            name, group, tvg_id, logo = "", "", "", ""
        else:
             # Altri commenti M3U come #EXTM3U
             new_lines.append(line)
             
    # Rewrite the M3U file if we proxied any URLs
    if use_proxy:
        with open(filename, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
            
    print(f"  [OK] Found {len(channels)} valid entries in {filename}.")
    return channels

def generate_jsons(channels, subfolder, rules):
    print(f"[*] STEP: Generating JSON files for '{subfolder}'...")
    out_dir = os.path.join(DATA_DIR, subfolder)
    os.makedirs(out_dir, exist_ok=True)
    
    # 1. Determine groups and respect groupsOrder from rules
    groups_raw = []
    seen = set()
    for c in channels:
        if c["group"] not in seen:
            groups_raw.append(c["group"])
            seen.add(c["group"])
            
    # Apply groupsOrder
    groups_order = rules.get("groupsOrder", [])
    if groups_order:
        # Filter groups that exist in current channels and maintain order, 
        # then append any new groups not in the order list.
        ordered = [g for g in groups_order if g in groups_raw]
        remaining = [g for g in groups_raw if g not in ordered]
        groups = ordered + remaining
    else:
        groups = groups_raw

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
        
    # Also save as .gz to bypass Google Apps Script 50MB limit
    with gzip.open(os.path.join(out_dir, "channels.json.gz"), "wt", encoding="utf-8") as f:
        json.dump(search_db, f, separators=(',', ':'), ensure_ascii=False)
        
    print(f"{subfolder} JSON chunks generated.")

def parse_epg():
    print("[*] STEP: Beginning EPG Parsing...")
    if not EPG_URL:
        print("No EPG_URL provided.")
        return
        
    epg_file = "epg.xml"
    # Note: download_file uses session.get(url). If EPG_URL is a private raw github link, 
    # it needs a token or be accessed via proxy.
    # Assuming EPG_URL is provided correctly with token if needed or points to proxy.
    download_file(EPG_URL, epg_file)
    
    epg_dir = os.path.join(DATA_DIR, "epg")
    os.makedirs(epg_dir, exist_ok=True)
    
    context = ET.iterparse(epg_file, events=('start', 'end',))
    # root element to clear children
    _, root = next(context)
    
    programs = {}
    epg_now = {}
    
    now = datetime.utcnow().timestamp()
    window_start = now - (2 * 3600)
    window_end = now + (48 * 3600)
    
    for event, elem in context:
        if event == 'end' and elem.tag == 'programme':
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
            
            # Memory safety: clear the element after processing
            elem.clear()
            # Also clear children from the root to prevent memory accumulation
            root.clear()
        elif event == 'end':
            # Clear other elements we don't need (like 'channel', 'displayName' etc) 
            # as long as they are not the root
            if elem != root:
                elem.clear()
                root.clear()
            
    for channel, schedule in programs.items():
        safe_channel = "".join(x if x.isalnum() else "_" for x in channel)
        with open(os.path.join(epg_dir, f"{safe_channel}.json"), "w", encoding="utf-8") as f:
            json.dump(schedule, f, ensure_ascii=False)
            
    with open(os.path.join(epg_dir, "epg_now.json"), "w", encoding="utf-8") as f:
        json.dump(epg_now, f, ensure_ascii=False)
            
    print(f"  [OK] EPG processing finished. {len(epg_now)} channels currently on-air.")
    print("EPG chunks and epg_now.json generated.")

def process_playlist(url, name, rules):
    print(f"[*] START Process Playlist: {name}")
    if url:
        filename = f"{name}.m3u"
        download_file(url, filename)
        
        use_proxy = True # Abilita il proxy per tutti i link, inclusi i canali live
        channels = parse_m3u(filename, use_proxy=use_proxy)
        
        # Apply the manual rules (renaming, hiding, ordering)
        channels = apply_rules(channels, rules)

        # 1. First Pass: Generate JSONs with proxied URLs immediately
        # This ensures we have updated links even if TMDB enrichment fails or is slow
        generate_jsons(channels, name, rules)
        
        if name in ["film", "series"] and TMDB_API_KEY:
            print(f"[*] STEP: Starting TMDB enrichment for {name}...")
            enrich_channels_with_tmdb(channels, is_series=(name=="series"))
            # 2. Second Pass: Update JSONs with TMDB metadata
            print(f"[*] STEP: Updating JSONs with metadata for {name}...")
            generate_jsons(channels, name, rules)
            
        print(f"[*] END Process Playlist: {name}")
    else:
        print(f"Skipping {name}, no URL provided.")

def main():
    start_time = time.time()
    print(f"--- LiveTvAPI Parallel Parser Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    # Load rules once
    rules = load_rules()
    
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"Rules loaded: {len(rules.get('items', {}))} items, {len(rules.get('order', []))} order hints")
    print(f"M3U_LIVE_URL preset: {'YES' if M3U_LIVE_URL else 'NO'}")
    print(f"M3U_FILM_URL preset: {'YES' if M3U_FILM_URL else 'NO'}")
    print(f"M3U_SERIES_URL preset: {'YES' if M3U_SERIES_URL else 'NO'}")
    print(f"EPG_URL preset: {'YES' if EPG_URL else 'NO'}")
    print(f"TMDB_API_KEY preset: {'YES' if TMDB_API_KEY else 'NO'}")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    tasks = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        if M3U_LIVE_URL:
            tasks.append(executor.submit(process_playlist, M3U_LIVE_URL, "live", rules))
        if M3U_FILM_URL:
            tasks.append(executor.submit(process_playlist, M3U_FILM_URL, "film", rules))
        if M3U_SERIES_URL:
            tasks.append(executor.submit(process_playlist, M3U_SERIES_URL, "series", rules))
        if EPG_URL:
            tasks.append(executor.submit(parse_epg))

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                print(f"[!] Critical Error in task: {e}")
    
    end_time = time.time()
    print(f"\n[COMPLETE] All parallel tasks finished in {end_time - start_time:.2f} seconds.")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
