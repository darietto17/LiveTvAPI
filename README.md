# LiveTv Parallel Parser & Rule Engine

Il motore Python core dell'ecosistema LiveTvPremium. Questo componente si occupa di processare playlist M3U nel cloud tramite GitHub Actions e trasformarle in database JSON ottimizzati per l'app Android.

## 🛠️ Funzionalità

- **Parallel Processing**: Scarica e processa contemporaneamente flussi Live, Film e Serie TV utilizzando un `ThreadPoolExecutor`.
- **Intelligent Rule Engine**: Legge il file `user_rules.json` (generato dal Channel Manager Web) per:
  - Rinomire i canali.
  - Spostare canali tra categorie.
  - Nascondere canali disattivati.
  - Applicare un ordinamento personalizzato globale.
- **TMDB Enrichment**: Interroga le API di The Movie Database per ottenere metadati, locandine e trame per i contenuti VOD.
- **EPG Generator**: Scarica e processa i dati della Guida TV (XMLTV), suddividendoli in "chunks" per un caricamento veloce nell'app Android.
- **Proxy Injection**: Gestisce l'iniezione automatica di prefissi proxy per i link che ne hanno bisogno.

## ⚙️ Configurazione (GitHub Secrets)

Per il corretto funzionamento nelle GitHub Actions, assicurati di aver impostato:
- `M3U_LIVE_URL`: URL della playlist live.
- `M3U_FILM_URL`: URL della playlist film.
- `M3U_SERIES_URL`: URL della playlist serie.
- `TMDB_API_KEY`: La tua chiave API TMDB.
- `EPG_URL`: URL per i dati della Guida TV.

## 📦 Struttura Dati
I file generati vengono salvati nella cartella `/data`, che funge da database statico per l'applicazione Android.
- `categories.json`: Elenco ordinato delle categorie.
- `channels.json`: Database completo dei canali con metadati.
- `epg_now.json`: Programmi attualmente in onda per ogni canale.
