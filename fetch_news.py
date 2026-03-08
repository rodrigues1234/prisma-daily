"""
Prisma — fetch_news.py  (v4 — user-categorized)
══════════════════════════════════════════════════
Mudanças v4:
  - Gemini NÃO categoriza. Só enriquece (tradução, resumo, why, impact).
  - Categorização feita no HTML pelo utilizador via source → categoria.
  - 1 chamada enriquecimento + 1 chamada resumo = 2/run (sem rate limit).
  - Espera 65s se rate limit (era 35s, insuficiente).
  - 8 artigos por feed, máximo 80 total.
  - JSON devolve lista plana de artigos + stocks + resumo.
  - Portfolio calculado localmente sem Gemini.
"""

import os, json, re, time, hashlib, sys, signal
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
import yfinance as yf
from google import genai
from google.genai import types

def _timeout_handler(sig, frame):
    print("\n⚠️  Timeout global atingido.")
    sys.exit(0)
signal.signal(signal.SIGALRM, _timeout_handler)
signal.alarm(720)  # 12 min: 3 batches × worst-case + resumo

MODEL_FALLBACKS = ["gemini-2.0-flash","gemini-2.0-flash-lite","gemini-1.5-flash"]

_NOW     = datetime.now(timezone.utc)
TODAY    = _NOW.strftime("%Y-%m-%d")
RUN_SLOT = _NOW.strftime("%Hh")
RUN_ID   = f"{TODAY}-{RUN_SLOT}"
NOW_ISO  = _NOW.isoformat()
DATA     = Path("data")
DATA.mkdir(exist_ok=True)

MESES_PT = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
            "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
DIA_PT = f"{_NOW.day} de {MESES_PT[_NOW.month-1]} de {_NOW.year}"

FEED_TIMEOUT   = 8
MAX_WORKERS    = 12
ARTS_PER_FEED  = 8
MAX_ARTS_TOTAL = 80
MAX_TITLE_LEN  = 120

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; PrismaBot/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
})

RSS = [
    ("BBC News",           "https://feeds.bbci.co.uk/news/rss.xml"),
    ("BBC News",           "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("BBC News",           "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("The Guardian",       "https://www.theguardian.com/world/rss"),
    ("The Guardian",       "https://www.theguardian.com/business/economics/rss"),
    ("The Guardian",       "https://www.theguardian.com/business/rss"),
    ("The Guardian",       "https://www.theguardian.com/environment/rss"),
    ("The Guardian",       "https://www.theguardian.com/money/work-and-careers/rss"),
    ("The Guardian",       "https://www.theguardian.com/lifeandstyle/rss"),
    ("The Guardian",       "https://www.theguardian.com/science/rss"),
    ("Observador",         "https://feeds.feedburner.com/observador"),
    ("Eco",                "https://eco.sapo.pt/feed/"),
    ("Jornal de Negócios", "https://www.jornaldenegocios.pt/rss"),
    ("TechCrunch",         "https://techcrunch.com/feed/"),
    ("The Verge",          "https://www.theverge.com/rss/index.xml"),
    ("Ars Technica",       "https://feeds.arstechnica.com/arstechnica/index"),
]

SOURCE_NAMES = sorted(set(name for name, _ in RSS))

STOCKS = ["AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOGL"]
FOREX  = ["EURUSD=X","BTC-USD","GC=F"]
LABELS = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","TSLA":"Tesla",
    "AMZN":"Amazon","META":"Meta","GOOGL":"Google",
    "EURUSD=X":"EUR/USD","BTC-USD":"Bitcoin","GC=F":"Gold",
}


def fetch_one_feed(canonical_name, url):
    try:
        resp = SESSION.get(url, timeout=FEED_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        items = []
        for e in feed.entries[:ARTS_PER_FEED]:
            title = e.get("title","").strip()[:MAX_TITLE_LEN]
            if not title: continue
            items.append({"title":title,"url":e.get("link",""),"source":canonical_name})
        return items
    except Exception as ex:
        print(f"  aviso feed ({url[:50]}): {ex}")
        return []


def collect_all():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fmap = {ex.submit(fetch_one_feed, name, url): (name,url) for name,url in RSS}
        raw = []
        for f in as_completed(fmap):
            try: raw.extend(f.result())
            except: pass

    seen_urls, seen_titles, unique = set(), set(), []
    for it in raw:
        uk = hashlib.md5(it["url"].encode()).hexdigest()
        tk = hashlib.md5(it["title"].lower().encode()).hexdigest()
        if uk not in seen_urls and tk not in seen_titles:
            seen_urls.add(uk); seen_titles.add(tk); unique.append(it)

    print(f"  {len(unique)} artigos únicos de {len(RSS)} feeds")
    return unique[:MAX_ARTS_TOTAL]


def get_client():
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


def call_gemini(client, prompt, max_tokens):
    for attempt in range(2):
        for mdl in MODEL_FALLBACKS:
            try:
                r = client.models.generate_content(
                    model=mdl, contents=prompt,
                    config=types.GenerateContentConfig(temperature=0.2, max_output_tokens=max_tokens)
                )
                if mdl != MODEL_FALLBACKS[0]: print(f"  modelo fallback: {mdl}")
                return r.text
            except Exception as ex:
                msg = str(ex)
                if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                    print(f"  rate limit ({mdl}, tentativa {attempt+1}/2), aguarda 65s...")
                    time.sleep(65); break
                elif "404" in msg or "not found" in msg.lower():
                    print(f"  modelo {mdl} indisponível, próximo..."); continue
                else:
                    print(f"  erro Gemini: {ex}"); return None
    print("  Gemini não respondeu após 2 tentativas.")
    return None


def parse_json(text):
    if not text: return None
    text = re.sub(r"^```(?:json)?\s*|\s*```$","",text.strip(),flags=re.MULTILINE).strip()
    try: return json.loads(text)
    except json.JSONDecodeError:
        for pattern in [r'\[.*\]',r'\{.*\}']:
            m = re.search(pattern,text,re.DOTALL)
            if m:
                try: return json.loads(m.group())
                except: pass
    return None


BATCH_SIZE = 30  # ~4500 tokens input + ~5000 output por batch — dentro do limite seguro

def _enrich_batch(client, batch: list, id_offset: int) -> dict:
    """
    Enriquece um batch de artigos. Devolve dict {id -> enriched_item}.
    id_offset: o primeiro artigo deste batch tem id = id_offset + 1.
    """
    arts_txt = "\n".join([
        f"[{id_offset+i+1}] {a['title']} | {a['source']} | {a['url']}"
        for i,a in enumerate(batch)
    ])
    prompt = f"""És um curador de notícias para um executivo português. Data: {DIA_PT}.

Para cada artigo preenche TODOS os campos abaixo. Sê conciso.

- "id": número do artigo (inteiro, tal como no input)
- "title": TRADUZ SEMPRE para português de Portugal. Se já for português, mantém igual.
- "translated": true se traduziste, false se já era português
- "summary": 2 frases directas em português de Portugal
- "why": 1 frase — porque é relevante para um executivo português
- "impact": 1 frase — impacto concreto nos próximos dias/semanas
- "related": 1 frase — ligação a outro tema ou notícia
- "impact_level": exactamente "alto", "médio" ou "baixo"
- "breaking": true APENAS se for notícia urgente de impacto imediato (ex: conflito, crise financeira, catástrofe, decisão política inesperada). false nos restantes.
- "recomendacao": true APENAS para 1 artigo por batch — o mais interessante para leitura/reflexão pessoal (ensaio, livro, podcast, ciência, lifestyle). false nos restantes.

Responde APENAS com JSON válido — array com {len(batch)} objectos:
[{{"id":{id_offset+1},"title":"...","translated":true,"summary":"...","why":"...","impact":"...","related":"...","impact_level":"médio","breaking":false,"recomendacao":false}}]

Artigos:
{arts_txt}"""

    print(f"  batch [{id_offset+1}–{id_offset+len(batch)}]: {len(prompt)} chars, ~{len(prompt)//4} tokens")
    text = call_gemini(client, prompt, max_tokens=6000)
    data = parse_json(text)
    if not isinstance(data, list):
        print(f"  aviso: batch [{id_offset+1}–{id_offset+len(batch)}] falhou")
        return {}
    return {item.get("id"): item for item in data if isinstance(item, dict)}


def enrich_articles(client, articles):
    if not articles:
        return []

    all_enriched: dict = {}
    batches = [articles[i:i+BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]
    print(f"  {len(articles)} artigos em {len(batches)} batch(es) de {BATCH_SIZE}")

    for b_idx, batch in enumerate(batches):
        if b_idx > 0:
            time.sleep(12)  # margem entre batches — evita rate limit
        offset = b_idx * BATCH_SIZE
        enriched = _enrich_batch(client, batch, offset)
        all_enriched.update(enriched)

    result = []
    for i, a in enumerate(articles):
        e = all_enriched.get(i + 1, {})
        result.append({
            "title":        e.get("title") or a["title"],
            "url":          a["url"],
            "source":       a["source"],
            "translated":   e.get("translated", False),
            "summary":      e.get("summary", ""),
            "why":          e.get("why", ""),
            "impact":       e.get("impact", ""),
            "related":      e.get("related", ""),
            "impact_level": (e.get("impact_level") or "médio").replace("medio","médio"),
            "breaking":     bool(e.get("breaking", False)),
            "recomendacao": bool(e.get("recomendacao", False)),
            "date":         NOW_ISO,
        })
    return result


def gen_summary(client, articles, stocks):
    top = [a for a in articles if a.get("why")][:8] or articles[:8]
    arts_txt = "\n".join([f"- {a['title']} ({a['source']})" for a in top])
    stocks_txt = "\n".join([f"- {s['label']}: {s['price']} ({'+' if s['up'] else ''}{s['change_pct']:.2f}%)" for s in stocks[:7]])

    prompt = f"""Notícias de {DIA_PT}:
{arts_txt or "Sem notícias."}

Cotações:
{stocks_txt or "Sem cotações."}

Responde APENAS com JSON válido:
{{"headline":"Briefing de {DIA_PT}","items":[
  {{"text":"frase directa pt-PT","color":"#EF4444","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}},
  {{"text":"frase directa pt-PT","color":"#F59E0B","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}},
  {{"text":"frase directa pt-PT","color":"#3B82F6","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}},
  {{"text":"frase directa pt-PT","color":"#10B981","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}}
]}}"""

    text = call_gemini(client, prompt, max_tokens=600)
    data = parse_json(text)
    default = {"headline": f"Briefing de {DIA_PT}", "items": []}
    if not isinstance(data, dict): return default
    return {"headline": data.get("headline", default["headline"]), "items": data.get("items",[])}


def gen_portfolio(stocks):
    if not stocks:
        return {"total_change_pct":0,"sentiment":"neutro","what_happened":"Sem dados.","what_to_do":"Manter posições.","risks":"Volatilidade de mercado.","tips":[{"risk":"baixo","suggestion":"ETF de índice"},{"risk":"médio","suggestion":"Acções com dividendo"},{"risk":"alto","suggestion":"Sem recomendação"}]}
    avg = sum(s["change_pct"] for s in stocks) / len(stocks)
    sentiment = "positivo" if avg>0.3 else "negativo" if avg<-0.3 else "neutro"
    up   = [s["label"] for s in stocks if s["up"]]
    down = [s["label"] for s in stocks if not s["up"]]
    what = ("Mercado em alta: " if avg>0 else "Mercado em baixa: ")
    what += (", ".join(up[:3])+" sobem" if up else "")
    what += ("; "+", ".join(down[:3])+" descem." if down else ".")
    return {
        "total_change_pct": round(avg,2), "sentiment": sentiment,
        "what_happened": what,
        "what_to_do": "Rever exposição a activos voláteis." if abs(avg)>1 else "Manter posições actuais.",
        "risks": "Alta volatilidade." if abs(avg)>1.5 else "Volatilidade normal.",
        "tips":[{"risk":"baixo","suggestion":"ETF de índice diversificado"},{"risk":"médio","suggestion":"Acções de qualidade com dividendo"},{"risk":"alto","suggestion":"Sem recomendação especulativa"}]
    }


def fetch_stocks():
    result = []
    tickers = STOCKS + FOREX
    try:
        data  = yf.download(tickers, period="5d", interval="1d", progress=False, auto_adjust=True)
        close = data["Close"]
        for tk in tickers:
            try:
                if tk not in close.columns: continue
                prices = close[tk].dropna()
                if len(prices) < 1: continue
                p_now  = float(prices.iloc[-1])
                p_prev = float(prices.iloc[-2]) if len(prices)>=2 else p_now
                pct    = ((p_now-p_prev)/p_prev*100) if p_prev else 0
                result.append({"symbol":tk,"label":LABELS.get(tk,tk),"price":round(p_now,2),"change_pct":round(pct,2),"up":pct>=0})
            except Exception as ex:
                print(f"  aviso {tk}: {ex}")
    except Exception as ex:
        print(f"  aviso yfinance: {ex}")
    return result


def atomic_write(path, content):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def main():
    t0 = time.time()
    print(f"\n{'='*54}")
    print(f"  Prisma v4 — {RUN_ID}")
    print(f"  Feeds: {len(RSS)} | Max artigos: {MAX_ARTS_TOTAL} | Gemini calls: 2")
    print(f"  Timeout: 12 min")
    print(f"{'='*54}\n")

    client = get_client()

    print(f"[RSS] A recolher {len(RSS)} feeds...")
    articles = collect_all()
    print(f"  {len(articles)} artigos em {time.time()-t0:.1f}s\n")

    print("[stocks] A buscar cotações...")
    stocks_list = fetch_stocks()
    print(f"  {len(stocks_list)} instrumentos\n")

    print(f"[Gemini 1/2] A enriquecer {len(articles)} artigos...")
    articles_enriched = enrich_articles(client, articles)
    enriched_count = sum(1 for a in articles_enriched if a.get("summary"))
    print(f"  {enriched_count}/{len(articles_enriched)} com detalhe\n")

    time.sleep(10)

    print("[Gemini 2/2] Resumo do dia...")
    summary = gen_summary(client, articles_enriched, stocks_list)
    print(f"  {len(summary.get('items',[]))} pontos\n")

    pa = gen_portfolio(stocks_list)

    output = {
        "updated_at":         NOW_ISO,
        "run_id":             RUN_ID,
        "version":            4,
        "sources":            SOURCE_NAMES,
        "articles":           articles_enriched,
        "summary":            summary,
        "stocks":             stocks_list,
        "portfolio_analysis": pa,
    }
    payload = json.dumps(output, ensure_ascii=False, indent=2)

    run_file = DATA / f"{RUN_ID}.json"
    atomic_write(run_file, payload)
    atomic_write(DATA / "latest.json", payload)
    print(f"  Guardado: {run_file}")

    idx_path = DATA / "index.json"
    existing = {"runs": []}
    if idx_path.exists():
        try: existing = json.loads(idx_path.read_text())
        except: existing = {"runs": []}
    runs = [r for r in existing.get("runs",[]) if r.get("id")!=RUN_ID]
    runs.insert(0,{"id":RUN_ID,"date":TODAY,"slot":RUN_SLOT,"ts":NOW_ISO})
    existing["runs"] = runs[:180]
    atomic_write(idx_path, json.dumps(existing, ensure_ascii=False, indent=2))

    elapsed = time.time()-t0
    print(f"\n{'='*54}")
    print(f"  Concluído em {elapsed:.1f}s")
    print(f"  {len(articles_enriched)} artigos | {len(SOURCE_NAMES)} fontes")
    print(f"  Gemini calls: {len(batches)+1} / 1500 disponíveis hoje")
    print(f"{'='*54}\n")


if __name__ == "__main__":
    main()
