"""
Prisma — fetch_news.py  v5
══════════════════════════════════════════════════════════
Arquitectura:
  - 40 artigos pré-seleccionados localmente (max 2 por fonte, mais recentes)
  - 1 chamada Gemini: tradução + categorização + enriquecimento completo
  - 1 chamada Gemini: resumo do dia (4 pontos)
  - Total: 2 chamadas por run × 4 runs/dia = 8 chamadas/dia
  - Se Gemini falhar: guarda artigos sem detalhe + gemini_status = "failed"
  - Exit code 0 sempre (nunca crasha o workflow)
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

# ─── TIMEOUT ──────────────────────────────────────────────────────────
def _timeout_handler(sig, frame):
    print("\n⚠️  Timeout atingido — a guardar estado actual...")
    sys.exit(0)   # exit 0 para não falhar o workflow
signal.signal(signal.SIGALRM, _timeout_handler)
signal.alarm(480)  # 8 minutos

# ─── CONFIG ───────────────────────────────────────────────────────────
MODEL          = "gemini-2.0-flash"
MODEL_FALLBACK = "gemini-1.5-flash"
MAX_ARTICLES   = 40    # artigos enviados ao Gemini
MAX_PER_SOURCE = 2     # máximo por fonte (filtro local)

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

# ─── CATEGORIAS VÁLIDAS ───────────────────────────────────────────────
CATS = ["breaking","portugal","mundo","economia","portfolio",
        "tecnologia","carreira","recomendacao"]

# ─── 16 FONTES FIXAS ──────────────────────────────────────────────────
# 6 portuguesas + 10 internacionais cobrindo todos os temas
RSS = [
    # Portugal (6)
    ("Observador",          "https://feeds.feedburner.com/observador"),
    ("Eco",                 "https://eco.sapo.pt/feed/"),
    ("Jornal de Negócios",  "https://www.jornaldenegocios.pt/rss"),
    ("Público",             "https://www.publico.pt/rss"),
    ("Expresso",            "https://feeds.feedburner.com/expresso-online"),
    ("RTP Notícias",        "https://www.rtp.pt/noticias/rss/rtp-noticias"),
    # Internacional — Mundo & Breaking (4)
    ("BBC News",            "https://feeds.bbci.co.uk/news/rss.xml"),
    ("Reuters",             "https://feeds.reuters.com/reuters/topNews"),
    ("The Guardian",        "https://www.theguardian.com/world/rss"),
    ("AP News",             "https://rsshub.app/apnews/topics/apf-topnews"),
    # Economia & Mercados (2)
    ("Financial Times",     "https://www.ft.com/rss/home"),
    ("Bloomberg",           "https://feeds.bloomberg.com/markets/news.rss"),
    # Tecnologia & IA (3)
    ("TechCrunch",          "https://techcrunch.com/feed/"),
    ("The Verge",           "https://www.theverge.com/rss/index.xml"),
    ("Ars Technica",        "https://feeds.arstechnica.com/arstechnica/index"),
    # Carreira & Recomendação (1)
    ("Harvard Business Review", "https://hbr.org/rss"),
]

SOURCE_NAMES = sorted(set(name for name, _ in RSS))

STOCKS = ["AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOGL"]
FOREX  = ["EURUSD=X","BTC-USD","GC=F"]
LABELS = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","TSLA":"Tesla",
    "AMZN":"Amazon","META":"Meta","GOOGL":"Google",
    "EURUSD=X":"EUR/USD","BTC-USD":"Bitcoin","GC=F":"Ouro",
}


# ─── RSS ──────────────────────────────────────────────────────────────
def fetch_one_feed(name: str, url: str) -> list[dict]:
    try:
        resp = SESSION.get(url, timeout=FEED_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        items = []
        for e in feed.entries[:10]:
            title = e.get("title","").strip()[:150]
            link  = e.get("link","")
            if not title or not link:
                continue
            # Timestamp para ordenação
            ts = None
            for tf in ("published_parsed","updated_parsed"):
                if e.get(tf):
                    try:
                        ts = time.mktime(e[tf])
                    except Exception:
                        pass
                    break
            items.append({"title": title, "url": link,
                           "source": name, "_ts": ts or 0})
        return items
    except Exception as ex:
        print(f"  aviso feed {name}: {ex}")
        return []


def collect_and_filter() -> list[dict]:
    """Recolhe feeds, deduplica, selecciona max MAX_PER_SOURCE por fonte mais recente."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_one_feed, name, url): name for name, url in RSS}
        raw = []
        for f in as_completed(futures):
            try:
                raw.extend(f.result())
            except Exception:
                pass

    # Deduplica por URL e título
    seen_urls, seen_titles, unique = set(), set(), []
    for a in raw:
        uk = hashlib.md5(a["url"].encode()).hexdigest()
        tk = hashlib.md5(a["title"].lower().encode()).hexdigest()
        if uk not in seen_urls and tk not in seen_titles:
            seen_urls.add(uk); seen_titles.add(tk)
            unique.append(a)

    # Ordena por recência dentro de cada fonte, pega max MAX_PER_SOURCE
    by_source: dict[str, list] = {}
    for a in unique:
        by_source.setdefault(a["source"], []).append(a)

    selected = []
    for src, arts in by_source.items():
        arts.sort(key=lambda x: x["_ts"], reverse=True)
        selected.extend(arts[:MAX_PER_SOURCE])

    # Remove campo interno _ts
    for a in selected:
        a.pop("_ts", None)

    # Limita total e baralha para não ter sempre as mesmas fontes no topo
    selected = selected[:MAX_ARTICLES]
    print(f"  {len(unique)} artigos únicos → {len(selected)} seleccionados (max {MAX_PER_SOURCE}/fonte)")
    return selected


# ─── GEMINI ───────────────────────────────────────────────────────────
def get_client() -> genai.Client:
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


def call_gemini(client, prompt: str, max_tokens: int,
                label: str = "") -> str | None:
    """Tenta MODEL, depois MODEL_FALLBACK. Rate limit: espera 70s e tenta 1x mais."""
    for attempt in range(2):
        for mdl in [MODEL, MODEL_FALLBACK]:
            try:
                r = client.models.generate_content(
                    model=mdl, contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.2,
                        max_output_tokens=max_tokens,
                    )
                )
                if mdl != MODEL:
                    print(f"  {label} fallback para {mdl}")
                return r.text
            except Exception as ex:
                msg = str(ex)
                if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                    if attempt == 0:
                        print(f"  {label} rate limit ({mdl}), aguarda 70s...")
                        time.sleep(70)
                        break   # tenta o próximo attempt
                    else:
                        print(f"  {label} rate limit persistente — a desistir")
                        return None
                elif "404" in msg or "not found" in msg.lower():
                    print(f"  {label} modelo {mdl} indisponível")
                    continue
                else:
                    print(f"  {label} erro: {ex}")
                    return None
    return None


def parse_json_safe(text: str):
    if not text:
        return None
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(),
                  flags=re.MULTILINE).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        for pat in [r'\[.*\]', r'\{.*\}']:
            m = re.search(pat, text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group())
                except Exception:
                    pass
    return None


# ─── ENRIQUECIMENTO + CATEGORIZAÇÃO (1 chamada) ───────────────────────
def enrich_and_categorize(client, articles: list) -> tuple[list, str]:
    """
    Devolve (lista_enriquecida, status).
    status: "ok" | "failed"
    """
    if not articles:
        return [], "ok"

    cats_desc = (
        "breaking   = notícia urgente de impacto imediato (conflito, crise, catástrofe, decisão política inesperada)\n"
        "portugal   = notícia sobre Portugal ou assuntos portugueses\n"
        "mundo      = geopolítica, diplomacia, eventos internacionais\n"
        "economia   = economia, finanças, mercados, empresas\n"
        "portfolio  = acções, investimentos, mercados financeiros, crypto\n"
        "tecnologia = tecnologia, IA, startups, inovação\n"
        "carreira   = liderança, gestão, futuro do trabalho, carreira\n"
        "recomendacao = 1 artigo por run — leitura/podcast/reflexão de valor pessoal ou profissional"
    )

    arts_txt = "\n".join([
        f"[{i+1}] {a['title']} | {a['source']}"
        for i, a in enumerate(articles)
    ])

    prompt = f"""És um curador editorial para um executivo português. Data: {DIA_PT}, {HORA_PT} UTC.

Categorias disponíveis:
{cats_desc}

Para cada artigo responde com JSON. Sê conciso e directo.

Campos obrigatórios:
- "id": número inteiro
- "title": título em português de Portugal (traduz SEMPRE se estiver noutra língua; mantém se já for PT)
- "translated": true se traduziste, false se já era português
- "category": uma das 8 categorias acima (string exacta)
- "summary": 2 frases em português de Portugal
- "why": 1 frase — relevância para executivo português
- "impact": 1 frase — impacto prático nos próximos dias
- "impact_level": "alto", "médio" ou "baixo"
- "breaking": true APENAS se for urgente e de impacto imediato; false nos restantes
- "recomendacao": true para NO MÁXIMO 1 artigo — o mais valioso para leitura/reflexão pessoal; false nos restantes

Responde APENAS com array JSON válido, sem texto antes ou depois:
[{{"id":1,"title":"...","translated":true,"category":"mundo","summary":"...","why":"...","impact":"...","impact_level":"médio","breaking":false,"recomendacao":false}}]

Artigos:
{arts_txt}"""

    print(f"  prompt enriquecimento: {len(prompt)} chars, ~{len(prompt)//4} tokens estimados")
    text = call_gemini(client, prompt, max_tokens=8000, label="[enrich]")
    data = parse_json_safe(text)

    if not isinstance(data, list):
        print("  enriquecimento falhou — artigos sem detalhe")
        fallback = [{
            "title":        a["title"],
            "url":          a["url"],
            "source":       a["source"],
            "translated":   False,
            "category":     "mundo",
            "summary":      "",
            "why":          "",
            "impact":       "",
            "impact_level": "médio",
            "breaking":     False,
            "recomendacao": False,
            "date":         NOW_ISO,
        } for a in articles]
        return fallback, "failed"

    enriched_map = {item.get("id"): item for item in data if isinstance(item, dict)}
    result = []
    for i, a in enumerate(articles):
        e = enriched_map.get(i + 1, {})
        cat = e.get("category", "mundo")
        if cat not in CATS:
            cat = "mundo"
        result.append({
            "title":        e.get("title") or a["title"],
            "url":          a["url"],
            "source":       a["source"],
            "translated":   bool(e.get("translated", False)),
            "category":     cat,
            "summary":      e.get("summary", ""),
            "why":          e.get("why", ""),
            "impact":       e.get("impact", ""),
            "impact_level": (e.get("impact_level") or "médio").replace("medio","médio"),
            "breaking":     bool(e.get("breaking", False)),
            "recomendacao": bool(e.get("recomendacao", False)),
            "date":         NOW_ISO,
        })

    enriched_count = sum(1 for a in result if a.get("summary"))
    print(f"  {enriched_count}/{len(result)} artigos com detalhe")
    return result, "ok"


# ─── RESUMO DO DIA (1 chamada) ────────────────────────────────────────
def gen_summary(client, articles: list, stocks: list) -> tuple[dict, str]:
    top = [a for a in articles if a.get("why")][:8] or articles[:8]
    arts_txt  = "\n".join([f"- {a['title']} ({a['source']})" for a in top])
    stocks_txt = "\n".join([
        f"- {s['label']}: {s['price']} ({'+' if s['up'] else ''}{s['change_pct']:.2f}%)"
        for s in stocks[:7]
    ]) or "Sem dados"

    prompt = f"""Notícias de {DIA_PT}:
{arts_txt}

Cotações:
{stocks_txt}

Gera um briefing executivo com 4 pontos de destaque em português de Portugal.
Responde APENAS com JSON válido:
{{"headline":"Briefing de {DIA_PT}","items":[
  {{"text":"frase directa em português","color":"#EF4444","source":"fonte","time":"{HORA_PT}","url":"url_ou_vazio"}},
  {{"text":"frase directa em português","color":"#F59E0B","source":"fonte","time":"{HORA_PT}","url":""}},
  {{"text":"frase directa em português","color":"#3B82F6","source":"fonte","time":"{HORA_PT}","url":""}},
  {{"text":"frase directa em português","color":"#10B981","source":"fonte","time":"{HORA_PT}","url":""}}
]}}"""

    text = call_gemini(client, prompt, max_tokens=800, label="[resumo]")
    data = parse_json_safe(text)
    default = {"headline": f"Briefing de {DIA_PT}", "items": []}
    if not isinstance(data, dict):
        print("  resumo falhou")
        return default, "failed"
    return {
        "headline": data.get("headline", default["headline"]),
        "items":    data.get("items", []),
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
    what = ("Mercado em alta: " if avg > 0 else "Mercado em baixa: ")
    what += (", ".join(up[:3]) + " sobem" if up else "")
    what += ("; " + ", ".join(down[:3]) + " descem." if down else ".")
    return {
        "total_change_pct": round(avg, 2),
        "sentiment":        sent,
        "what_happened":    what,
        "what_to_do":       "Rever exposição a activos voláteis." if abs(avg) > 1 else "Manter posições.",
        "risks":            "Alta volatilidade." if abs(avg) > 1.5 else "Volatilidade normal.",
        "tips": [
            {"risk":"baixo",  "suggestion":"ETF de índice diversificado"},
            {"risk":"médio",  "suggestion":"Acções de qualidade com dividendo"},
            {"risk":"alto",   "suggestion":"Sem recomendação especulativa"},
        ]
    }


# ─── STOCKS ───────────────────────────────────────────────────────────
def fetch_stocks() -> list[dict]:
    result = []
    tickers = STOCKS + FOREX
    try:
        data  = yf.download(tickers, period="5d", interval="1d",
                            progress=False, auto_adjust=True)
        close = data["Close"]
        for tk in tickers:
            try:
                if tk not in close.columns:
                    continue
                prices = close[tk].dropna()
                if len(prices) < 1:
                    continue
                p_now  = float(prices.iloc[-1])
                p_prev = float(prices.iloc[-2]) if len(prices) >= 2 else p_now
                pct    = ((p_now - p_prev) / p_prev * 100) if p_prev else 0
                result.append({
                    "symbol":     tk,
                    "label":      LABELS.get(tk, tk),
                    "price":      round(p_now, 2),
                    "change_pct": round(pct, 2),
                    "up":         pct >= 0,
                })
            except Exception as ex:
                print(f"  aviso {tk}: {ex}")
    except Exception as ex:
        print(f"  aviso yfinance: {ex}")
    return result


# ─── UTILIDADES ───────────────────────────────────────────────────────
def atomic_write(path: Path, content: str):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ─── MAIN ─────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"\n{'='*54}")
    print(f"  Prisma v5 — {RUN_ID}")
    print(f"  Feeds: {len(RSS)} | Max artigos: {MAX_ARTICLES} | Gemini calls: 2")
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

    # 3. Gemini — enriquecimento + categorização
    print(f"[Gemini 1/2] A enriquecer e categorizar {len(articles)} artigos...")
    articles_enriched, enrich_status = enrich_and_categorize(client, articles)

    # Pausa entre chamadas
    time.sleep(8)

    # 4. Gemini — resumo
    print("[Gemini 2/2] A gerar resumo do dia...")
    summary, summary_status = gen_summary(client, articles_enriched, stocks_list)
    print(f"  {len(summary.get('items',[]))} pontos\n")

    # 5. Portfolio
    pa = gen_portfolio(stocks_list)

    # 6. Alerta Gemini
    gemini_status = "ok"
    gemini_alert  = None
    if enrich_status == "failed" and summary_status == "failed":
        gemini_status = "failed"
        gemini_alert  = "O serviço de IA atingiu o limite de pedidos. Os artigos são mostrados sem tradução ou análise. Tenta actualizar manualmente mais tarde."
    elif enrich_status == "failed":
        gemini_status = "partial"
        gemini_alert  = "A análise detalhada dos artigos não está disponível neste run. Os títulos podem estar em inglês. O resumo foi gerado."
    elif summary_status == "failed":
        gemini_status = "partial"
        gemini_alert  = "O resumo do dia não foi gerado neste run. Os artigos têm análise completa."

    # 7. Agrupa artigos por categoria (para compatibilidade com HTML)
    categories: dict[str, list] = {c: [] for c in CATS}
    for a in articles_enriched:
        cat = a.get("category","mundo")
        # Flag breaking sobrepõe categoria
        if a.get("breaking"):
            cat = "breaking"
        elif a.get("recomendacao"):
            cat = "recomendacao"
        if cat not in categories:
            cat = "mundo"
        categories[cat].append(a)

    # 8. Output
    output = {
        "updated_at":         NOW_ISO,
        "run_id":             RUN_ID,
        "version":            5,
        "sources":            SOURCE_NAMES,
        "gemini_status":      gemini_status,
        "gemini_alert":       gemini_alert,
        "articles":           articles_enriched,
        "categories":         categories,        # pré-agrupado para o HTML
        "summary":            summary,
        "stocks":             stocks_list,
        "portfolio_analysis": pa,
    }

    payload = json.dumps(output, ensure_ascii=False, indent=2)
    run_file = DATA / f"{RUN_ID}.json"
    atomic_write(run_file, payload)
    atomic_write(DATA / "latest.json", payload)
    print(f"  Guardado: {run_file}")

    # 9. Index
    idx_path = DATA / "index.json"
    existing = {"runs": []}
    if idx_path.exists():
        try:
            existing = json.loads(idx_path.read_text())
        except Exception:
            existing = {"runs": []}
    runs = [r for r in existing.get("runs", []) if r.get("id") != RUN_ID]
    runs.insert(0, {"id": RUN_ID, "date": TODAY, "slot": RUN_SLOT, "ts": NOW_ISO,
                    "gemini_status": gemini_status})
    existing["runs"] = runs[:180]
    atomic_write(idx_path, json.dumps(existing, ensure_ascii=False, indent=2))

    elapsed = time.time() - t0
    print(f"\n{'='*54}")
    print(f"  Concluído em {elapsed:.1f}s")
    print(f"  {len(articles_enriched)} artigos | gemini: {gemini_status}")
    if gemini_alert:
        print(f"  ⚠️  {gemini_alert}")
    print(f"{'='*54}\n")
    sys.exit(0)   # sempre 0


if __name__ == "__main__":
    main()
