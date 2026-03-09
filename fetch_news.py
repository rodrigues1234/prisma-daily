"""
Prisma — fetch_news.py  v6
══════════════════════════════════════════════════════════
Stack:
  - Mistral AI (mistral-small-latest) — pago, rate limits generosos
  - 18 fontes, max 3 artigos/fonte = até 54 artigos, limita a 40 para Mistral
  - 1 chamada Mistral: tradução + categorização + enriquecimento
  - 1 chamada Mistral: resumo do dia
  - Fallback por fonte se Mistral falhar (nunca crasha)
  - Exit code 0 sempre
"""

import os, json, re, time, hashlib, sys, signal
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
import yfinance as yf
from mistralai import Mistral

# ─── TIMEOUT ──────────────────────────────────────────────────────────
def _timeout_handler(sig, frame):
    print("\n⚠️  Timeout — a guardar estado actual...")
    sys.exit(0)
signal.signal(signal.SIGALRM, _timeout_handler)
signal.alarm(300)  # 5 min — Mistral é rápido

# ─── CONFIG ───────────────────────────────────────────────────────────
MODEL          = "mistral-small-latest"
MAX_ARTICLES   = 40    # enviados ao Mistral
MAX_PER_SOURCE = 3     # 18 fontes × 3 = 54 → limita a 40

_NOW     = datetime.now(timezone.utc)
TODAY    = _NOW.strftime("%Y-%m-%d")
RUN_SLOT = _NOW.strftime("%Hh")
RUN_ID   = f"{TODAY}-{RUN_SLOT}"
NOW_ISO  = _NOW.isoformat()
DATA     = Path("data")
DATA.mkdir(exist_ok=True)

MESES_PT = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
            "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
DIA_PT   = f"{_NOW.day} de {MESES_PT[_NOW.month-1]} de {_NOW.year}"
HORA_PT  = _NOW.strftime("%H:%M")

FEED_TIMEOUT = 8
MAX_WORKERS  = 12

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; PrismaBot/1.0)",
    "Accept"    : "application/rss+xml, application/xml, text/xml, */*",
})

# ─── CATEGORIAS ───────────────────────────────────────────────────────
CATS = ["breaking","portugal","mundo","economia","portfolio",
        "tecnologia","carreira","recomendacao"]

SOURCE_TO_CAT = {
    "Observador":           "portugal",
    "Eco":                  "portugal",
    "Jornal de Negócios":   "economia",
    "Dinheiro Vivo":        "economia",
    "RTP Notícias":         "portugal",
    "Público":              "portugal",
    "The Guardian":         "mundo",
    "BBC News":             "breaking",
    "SAPO Tek":             "tecnologia",
    "Bloomberg":            "economia",
    "The Economist":        "economia",
    "TechCrunch":           "tecnologia",
    "The Verge":            "tecnologia",
    "Wired":                "tecnologia",
    "Seeking Alpha":        "portfolio",
    "HBR":                  "carreira",
    "MIT Sloan Review":     "carreira",
    "Fast Company":         "carreira",
}

# ─── 18 FONTES ────────────────────────────────────────────────────────
# URLs testados em runs reais + melhores alternativas conhecidas
RSS = [
    # Portugal (6)
    ("Observador",          "https://feeds.feedburner.com/observador"),
    ("Eco",                 "https://eco.sapo.pt/feed/"),
    ("Jornal de Negócios",  "https://www.jornaldenegocios.pt/rss"),
    ("Dinheiro Vivo",       "https://www.dinheirovivo.pt/feed/"),
    ("RTP Notícias",        "https://www.rtp.pt/noticias/rss/rtp-noticias"),
    ("Público",             "https://www.publico.pt/api/feeds/rss"),
    # Mundo & Breaking (2)
    ("The Guardian",        "https://www.theguardian.com/world/rss"),
    ("BBC News",            "https://feeds.bbci.co.uk/news/rss.xml"),
    # Economia & Portfolio (4)
    ("Bloomberg",           "https://feeds.bloomberg.com/markets/news.rss"),
    ("The Economist",       "https://www.economist.com/finance-and-economics/rss.xml"),
    ("Seeking Alpha",       "https://seekingalpha.com/feed.xml"),
    ("SAPO Tek",            "https://tek.sapo.pt/rss"),
    # Tecnologia (3)
    ("TechCrunch",          "https://techcrunch.com/feed/"),
    ("The Verge",           "https://www.theverge.com/rss/index.xml"),
    ("Wired",               "https://www.wired.com/feed/rss"),
    # Carreira & Recomendação (3)
    ("HBR",                 "https://feeds.feedburner.com/harvardbusiness"),
    ("MIT Sloan Review",    "https://sloanreview.mit.edu/feed/"),
    ("Fast Company",        "https://www.fastcompany.com/latest/rss"),
]

SOURCE_NAMES = sorted(set(name for name, _ in RSS))

STOCKS_DEFAULT = ["AAPL","MSFT","NVDA","TSLA","AMZN"]
FOREX_DEFAULT  = ["EURUSD=X","BTC-USD","GC=F"]
LABELS = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","TSLA":"Tesla",
    "AMZN":"Amazon","META":"Meta","GOOGL":"Google","AVGO":"Broadcom",
    "ASML":"ASML","EURUSD=X":"EUR/USD","BTC-USD":"Bitcoin","GC=F":"Ouro",
}


# ─── RSS ──────────────────────────────────────────────────────────────
def fetch_one_feed(name: str, url: str) -> list[dict]:
    try:
        resp = SESSION.get(url, timeout=FEED_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        items = []
        for e in feed.entries[:12]:
            title = e.get("title","").strip()[:200]
            link  = e.get("link","")
            if not title or not link:
                continue
            ts = 0
            for tf in ("published_parsed","updated_parsed"):
                if e.get(tf):
                    try: ts = time.mktime(e[tf])
                    except Exception: pass
                    break
            items.append({"title":title,"url":link,"source":name,"_ts":ts})
        return items
    except Exception as ex:
        print(f"  aviso feed {name}: {type(ex).__name__}: {str(ex)[:80]}")
        return []


def collect_and_filter() -> list[dict]:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_one_feed, n, u): n for n, u in RSS}
        raw = []
        for f in as_completed(futures):
            try: raw.extend(f.result())
            except Exception: pass

    seen_urls, seen_titles, unique = set(), set(), []
    for a in raw:
        uk = hashlib.md5(a["url"].encode()).hexdigest()
        tk = hashlib.md5(a["title"].lower().encode()).hexdigest()
        if uk not in seen_urls and tk not in seen_titles:
            seen_urls.add(uk); seen_titles.add(tk)
            unique.append(a)

    by_source: dict[str, list] = {}
    for a in unique:
        by_source.setdefault(a["source"],[]).append(a)

    selected = []
    for src, arts in by_source.items():
        arts.sort(key=lambda x: x["_ts"], reverse=True)
        selected.extend(arts[:MAX_PER_SOURCE])

    for a in selected: a.pop("_ts", None)

    # Ordena por fonte para output consistente, limita a MAX_ARTICLES
    selected = selected[:MAX_ARTICLES]
    feeds_ok = len(by_source)
    print(f"  {len(unique)} artigos únicos de {feeds_ok}/{len(RSS)} feeds → {len(selected)} seleccionados")
    return selected


# ─── MISTRAL ──────────────────────────────────────────────────────────
def get_client() -> Mistral:
    return Mistral(api_key=os.environ["MISTRAL_API_KEY"])


def call_mistral(client: Mistral, prompt: str, max_tokens: int, label: str = "") -> str | None:
    try:
        resp = client.chat.complete(
            model=MODEL,
            messages=[{"role":"user","content":prompt}],
            max_tokens=max_tokens,
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as ex:
        print(f"  {label} erro Mistral: {ex}")
        return None


def parse_json_safe(text: str):
    if not text: return None
    text = re.sub(r"^```(?:json)?\s*|\s*```$","",text.strip(),flags=re.MULTILINE).strip()
    try: return json.loads(text)
    except json.JSONDecodeError:
        for pat in [r'\[.*\]',r'\{.*\}']:
            m = re.search(pat, text, re.DOTALL)
            if m:
                try: return json.loads(m.group())
                except Exception: pass
    return None


# ─── ENRIQUECIMENTO + CATEGORIZAÇÃO ──────────────────────────────────
def enrich_and_categorize(client: Mistral, articles: list) -> tuple[list, str]:
    if not articles:
        return [], "ok"

    cats_desc = (
        "breaking     = notícia urgente de impacto imediato (conflito, crise, catástrofe, decisão política inesperada)\n"
        "portugal     = notícia sobre Portugal ou assuntos portugueses\n"
        "mundo        = geopolítica, diplomacia, eventos internacionais\n"
        "economia     = economia, finanças, mercados, empresas\n"
        "portfolio    = acções, investimentos, mercados financeiros, crypto\n"
        "tecnologia   = tecnologia, IA, startups, inovação\n"
        "carreira     = liderança, gestão, futuro do trabalho, carreira profissional\n"
        "recomendacao = 1 artigo por run — leitura/reflexão de valor pessoal ou profissional"
    )

    arts_txt = "\n".join([
        f"[{i+1}] {a['title']} | {a['source']}"
        for i, a in enumerate(articles)
    ])

    prompt = f"""És um curador editorial para um executivo português. Data: {DIA_PT}, {HORA_PT} UTC.

Categorias disponíveis:
{cats_desc}

Para cada artigo devolve um objecto JSON. Sê conciso e directo.

Campos obrigatórios:
- "id": número inteiro (igual ao do input)
- "title": título em português de Portugal. TRADUZ SEMPRE se não for português. Se já for português, mantém exactamente.
- "translated": true se traduziste, false se já era português
- "category": uma das 8 categorias (string exacta, minúsculas)
- "summary": 2 frases em português de Portugal
- "why": 1 frase — relevância concreta para executivo português
- "impact": 1 frase — impacto prático nos próximos dias ou semanas
- "impact_level": "alto", "médio" ou "baixo"
- "breaking": true APENAS se urgente e impacto imediato; false nos restantes
- "recomendacao": true para NO MÁXIMO 1 artigo — o mais valioso para leitura ou reflexão; false nos restantes

Responde APENAS com array JSON válido, sem texto antes ou depois, sem markdown:
[{{"id":1,"title":"...","translated":true,"category":"mundo","summary":"...","why":"...","impact":"...","impact_level":"médio","breaking":false,"recomendacao":false}}]

Artigos:
{arts_txt}"""

    print(f"  prompt: {len(prompt)} chars (~{len(prompt)//4} tokens estimados)")
    t0 = time.time()
    text = call_mistral(client, prompt, max_tokens=8000, label="[enrich]")
    print(f"  Mistral respondeu em {time.time()-t0:.1f}s")
    data = parse_json_safe(text)

    if not isinstance(data, list):
        print("  enriquecimento falhou — categorização automática por fonte")
        return [{
            "title":        a["title"],
            "url":          a["url"],
            "source":       a["source"],
            "translated":   False,
            "category":     SOURCE_TO_CAT.get(a["source"],"mundo"),
            "summary":      "",
            "why":          "",
            "impact":       "",
            "impact_level": "médio",
            "breaking":     False,
            "recomendacao": False,
            "date":         NOW_ISO,
        } for a in articles], "failed"

    enriched_map = {item.get("id"): item for item in data if isinstance(item,dict)}
    result = []
    for i, a in enumerate(articles):
        e = enriched_map.get(i+1, {})
        cat = e.get("category","mundo")
        if cat not in CATS:
            cat = SOURCE_TO_CAT.get(a["source"],"mundo")
        result.append({
            "title":        e.get("title") or a["title"],
            "url":          a["url"],
            "source":       a["source"],
            "translated":   bool(e.get("translated", False)),
            "category":     cat,
            "summary":      e.get("summary",""),
            "why":          e.get("why",""),
            "impact":       e.get("impact",""),
            "impact_level": (e.get("impact_level") or "médio").replace("medio","médio"),
            "breaking":     bool(e.get("breaking", False)),
            "recomendacao": bool(e.get("recomendacao", False)),
            "date":         NOW_ISO,
        })

    enriched_count = sum(1 for a in result if a.get("summary"))
    print(f"  {enriched_count}/{len(result)} artigos com detalhe completo")
    return result, "ok"


# ─── RESUMO DO DIA ────────────────────────────────────────────────────
def gen_summary(client: Mistral, articles: list, stocks: list) -> tuple[dict, str]:
    top = [a for a in articles if a.get("why")][:8] or articles[:8]
    arts_txt   = "\n".join([f"- {a['title']} ({a['source']})" for a in top])
    stocks_txt = "\n".join([
        f"- {s['label']}: {s['price']} ({'+' if s['up'] else ''}{s['change_pct']:.2f}%)"
        for s in stocks[:6]
    ]) or "Sem dados"

    prompt = f"""Notícias de {DIA_PT}:
{arts_txt}

Mercados:
{stocks_txt}

Gera um briefing executivo com exactamente 4 pontos de destaque em português de Portugal.
Cada ponto deve ser uma frase directa, informativa e útil para um executivo.

Responde APENAS com JSON válido:
{{"headline":"Briefing de {DIA_PT}","items":[
  {{"text":"frase directa","color":"#EF4444","source":"nome da fonte","time":"{HORA_PT}","url":""}},
  {{"text":"frase directa","color":"#F59E0B","source":"nome da fonte","time":"{HORA_PT}","url":""}},
  {{"text":"frase directa","color":"#3B82F6","source":"nome da fonte","time":"{HORA_PT}","url":""}},
  {{"text":"frase directa","color":"#10B981","source":"nome da fonte","time":"{HORA_PT}","url":""}}
]}}"""

    t0 = time.time()
    text = call_mistral(client, prompt, max_tokens=600, label="[resumo]")
    print(f"  Mistral resumo em {time.time()-t0:.1f}s")
    data = parse_json_safe(text)
    default = {"headline": f"Briefing de {DIA_PT}", "items": []}
    if not isinstance(data, dict):
        print("  resumo falhou")
        return default, "failed"
    return {
        "headline": data.get("headline", default["headline"]),
        "items":    data.get("items", [])
    }, "ok"


# ─── PORTFOLIO ────────────────────────────────────────────────────────
def gen_portfolio(stocks: list) -> dict:
    if not stocks:
        return {"total_change_pct":0,"sentiment":"neutro",
                "what_happened":"Sem dados de mercado.",
                "what_to_do":"Manter posições actuais.",
                "risks":"Volatilidade de mercado.",
                "tips":[{"risk":"baixo","suggestion":"ETF de índice diversificado"},
                        {"risk":"médio","suggestion":"Acções de qualidade com dividendo"},
                        {"risk":"alto","suggestion":"Sem recomendação especulativa"}]}
    avg  = sum(s["change_pct"] for s in stocks) / len(stocks)
    sent = "positivo" if avg > 0.3 else "negativo" if avg < -0.3 else "neutro"
    up   = [s["label"] for s in stocks if s["up"]]
    down = [s["label"] for s in stocks if not s["up"]]
    what = "Mercado em alta: " if avg > 0 else "Mercado em baixa: "
    what += (", ".join(up[:3]) + " sobem" if up else "")
    what += ("; " + ", ".join(down[:3]) + " descem." if down else ".")
    return {
        "total_change_pct": round(avg,2),
        "sentiment":        sent,
        "what_happened":    what,
        "what_to_do":       "Rever exposição a activos voláteis." if abs(avg)>1 else "Manter posições.",
        "risks":            "Alta volatilidade." if abs(avg)>1.5 else "Volatilidade normal.",
        "tips":[
            {"risk":"baixo","suggestion":"ETF de índice diversificado"},
            {"risk":"médio","suggestion":"Acções de qualidade com dividendo"},
            {"risk":"alto","suggestion":"Sem recomendação especulativa"},
        ]
    }


# ─── STOCKS ───────────────────────────────────────────────────────────
def fetch_stocks() -> list[dict]:
    result = []
    tickers = STOCKS_DEFAULT + FOREX_DEFAULT
    try:
        data  = yf.download(tickers, period="5d", interval="1d",
                            progress=False, auto_adjust=True)
        close = data["Close"]
        for tk in tickers:
            try:
                if tk not in close.columns: continue
                prices = close[tk].dropna()
                if len(prices) < 1: continue
                p_now  = float(prices.iloc[-1])
                p_prev = float(prices.iloc[-2]) if len(prices) >= 2 else p_now
                pct    = ((p_now - p_prev) / p_prev * 100) if p_prev else 0
                result.append({
                    "symbol":     tk,
                    "label":      LABELS.get(tk, tk),
                    "price":      round(p_now,2),
                    "change_pct": round(pct,2),
                    "up":         pct >= 0,
                })
            except Exception as ex:
                print(f"  aviso stock {tk}: {ex}")
    except Exception as ex:
        print(f"  aviso yfinance: {ex}")
    return result


# ─── ATOMIC WRITE ─────────────────────────────────────────────────────
def atomic_write(path: Path, content: str):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ─── MAIN ─────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"\n{'='*54}")
    print(f"  Prisma v6 — {RUN_ID}")
    print(f"  Fontes: {len(RSS)} | Max artigos: {MAX_ARTICLES} | Mistral calls: 2")
    print(f"  Runs/dia: 4 (07h 12h 18h 22h UTC)")
    print(f"{'='*54}\n")

    client = get_client()

    # 1. RSS
    print(f"[RSS] A recolher {len(RSS)} feeds...")
    articles = collect_and_filter()
    print(f"  pronto em {time.time()-t0:.1f}s\n")

    # 2. Stocks
    print("[stocks] A buscar cotações...")
    stocks_list = fetch_stocks()
    print(f"  {len(stocks_list)} instrumentos\n")

    # 3. Mistral — enriquecimento + categorização
    print(f"[Mistral 1/2] A enriquecer e categorizar {len(articles)} artigos...")
    articles_enriched, enrich_status = enrich_and_categorize(client, articles)

    # 4. Mistral — resumo
    print("\n[Mistral 2/2] A gerar resumo do dia...")
    summary, summary_status = gen_summary(client, articles_enriched, stocks_list)
    print(f"  {len(summary.get('items',[]))} pontos\n")

    # 5. Portfolio
    pa = gen_portfolio(stocks_list)

    # 6. Alerta se Mistral falhou
    gemini_status = "ok"   # campo mantido por compatibilidade HTML
    gemini_alert  = None
    if enrich_status == "failed" and summary_status == "failed":
        gemini_status = "failed"
        gemini_alert  = "O serviço de IA não respondeu neste run. Os artigos são mostrados sem tradução ou análise, categorizados automaticamente por fonte."
    elif enrich_status == "failed":
        gemini_status = "partial"
        gemini_alert  = "A análise detalhada não está disponível neste run. Os títulos podem estar em inglês. O resumo foi gerado."
    elif summary_status == "failed":
        gemini_status = "partial"
        gemini_alert  = "O resumo do dia não foi gerado neste run. Os artigos têm análise completa."

    # 7. Agrupa por categoria
    categories: dict[str, list] = {c: [] for c in CATS}
    for a in articles_enriched:
        cat = a.get("category","mundo")
        if a.get("breaking"):     cat = "breaking"
        elif a.get("recomendacao"): cat = "recomendacao"
        if cat not in categories: cat = "mundo"
        categories[cat].append(a)

    # 8. Output
    output = {
        "updated_at":         NOW_ISO,
        "run_id":             RUN_ID,
        "version":            6,
        "sources":            SOURCE_NAMES,
        "gemini_status":      gemini_status,
        "gemini_alert":       gemini_alert,
        "articles":           articles_enriched,
        "categories":         categories,
        "summary":            summary,
        "stocks":             stocks_list,
        "portfolio_analysis": pa,
    }

    payload = json.dumps(output, ensure_ascii=False, indent=2)
    atomic_write(DATA / f"{RUN_ID}.json", payload)
    atomic_write(DATA / "latest.json",    payload)
    print(f"  Guardado: data/{RUN_ID}.json")

    # 9. Index
    idx_path = DATA / "index.json"
    existing = {"runs": []}
    if idx_path.exists():
        try: existing = json.loads(idx_path.read_text())
        except Exception: pass
    runs = [r for r in existing.get("runs",[]) if r.get("id") != RUN_ID]
    runs.insert(0, {"id":RUN_ID,"date":TODAY,"slot":RUN_SLOT,
                    "ts":NOW_ISO,"ai_status":gemini_status})
    existing["runs"] = runs[:180]
    atomic_write(idx_path, json.dumps(existing, ensure_ascii=False, indent=2))

    elapsed = time.time() - t0
    print(f"\n{'='*54}")
    print(f"  Concluído em {elapsed:.1f}s")
    print(f"  {len(articles_enriched)} artigos | AI: {gemini_status}")
    if gemini_alert:
        print(f"  ⚠️  {gemini_alert}")
    print(f"{'='*54}\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
