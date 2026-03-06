"""
Prisma — fetch_news.py
Corre 1x/dia via GitHub Actions (07:00 UTC).
Usa: feedparser + google-generativeai (Gemini Flash, gratis) + yfinance
Gera: data/YYYY-MM-DD.json  +  data/latest.json  +  data/index.json
"""

import os, json, re, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import yfinance as yf
import google.generativeai as genai

# ─── CONFIG ──────────────────────────────────────────────────────────
genai.configure(api_key=os.environ["GEMINI_API_KEY"])
MODEL   = genai.GenerativeModel("gemini-1.5-flash")
TODAY   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
NOW_ISO = datetime.now(timezone.utc).isoformat()
DATA    = Path("data")
DATA.mkdir(exist_ok=True)

# ─── RSS POR CATEGORIA ────────────────────────────────────────────────
RSS = {
    "breaking": [
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://feeds.reuters.com/reuters/topNews",
    ],
    "geo": [
        "https://feeds.reuters.com/Reuters/worldNews",
        "https://www.theguardian.com/world/rss",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
    ],
    "eco": [
        "https://www.theguardian.com/business/economics/rss",
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
    ],
    "nateco": [
        "https://feeds.feedburner.com/observador",
        "https://www.jornaldenegocios.pt/rss",
        "https://www.publico.pt/rss/economia",
    ],
    "politics": [
        "https://www.publico.pt/rss/politica",
        "https://feeds.feedburner.com/observador",
    ],
    "climate": [
        "https://www.theguardian.com/environment/rss",
        "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    ],
    "market": [
        "https://feeds.reuters.com/reuters/companyNews",
        "https://hbr.org/rss/topic/finance",
    ],
    "work": [
        "https://hbr.org/rss/topic/leadership",
        "https://feeds.feedburner.com/mitsloanmanagementreview",
    ],
    "biz": [
        "https://hbr.org/rss/hbreditors",
        "https://www.weforum.org/rss/agenda",
    ],
    "ai": [
        "https://techcrunch.com/feed/",
        "https://feeds.feedburner.com/mit-technology-review",
        "https://www.theverge.com/rss/index.xml",
        "https://venturebeat.com/category/ai/feed/",
    ],
    "gadgets": [
        "https://www.theverge.com/rss/index.xml",
        "https://feeds.arstechnica.com/arstechnica/technology-lab",
    ],
    "science": [
        "https://www.nasa.gov/rss/dyn/breaking_news.rss",
        "https://feeds.nature.com/nature/rss/current",
        "https://www.sciencedaily.com/rss/top/technology.xml",
    ],
    "soul": [
        "https://hbr.org/rss/hbreditors",
        "https://feeds.feedburner.com/TedTalks_video",
    ],
}

CAT_NAMES = {
    "breaking": "Breaking News",
    "geo":      "Geopolitica",
    "eco":      "Economia Global",
    "nateco":   "Economia Nacional (Portugal)",
    "politics": "Politica Nacional (Portugal)",
    "climate":  "Clima Global",
    "market":   "Mercado Financeiro",
    "work":     "Future of Work",
    "biz":      "Dica de Negocios",
    "ai":       "IA e Inovacao",
    "gadgets":  "Gadgets e Hardware",
    "science":  "Ciencia e Espaco",
    "soul":     "Recomendacao do Dia",
}

STOCKS   = ["AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOGL"]
FOREX    = ["EURUSD=X","BTC-USD","GC=F"]
LABELS   = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","TSLA":"Tesla",
    "AMZN":"Amazon","META":"Meta","GOOGL":"Google",
    "EURUSD=X":"EUR/USD","BTC-USD":"Bitcoin","GC=F":"Gold",
}


# ─── RSS ──────────────────────────────────────────────────────────────
def fetch_feed(url, n=6):
    try:
        f = feedparser.parse(url)
        items = []
        for e in f.entries[:n]:
            s = re.sub(r"<[^>]+>", "", e.get("summary",""))[:400]
            items.append({
                "title":   e.get("title","").strip(),
                "summary": s.strip(),
                "url":     e.get("link",""),
                "source":  f.feed.get("title", url),
            })
        return items
    except Exception as ex:
        print(f"  aviso feed ({url}): {ex}")
        return []

def collect(cat):
    all_items = []
    for url in RSS.get(cat,[]):
        items = fetch_feed(url)
        all_items.extend(items)
        if items: time.sleep(0.4)
    seen, unique = set(), []
    for it in all_items:
        k = it["title"].lower()[:60]
        if k and k not in seen:
            seen.add(k)
            unique.append(it)
    return unique[:12]


# ─── GEMINI — curadoria de artigos ────────────────────────────────────
def curate(cat, articles):
    if not articles: return []
    name = CAT_NAMES.get(cat, cat)
    body = "\n\n".join([
        f"[{i+1}] TITULO: {a['title']}\nFONTE: {a['source']}\nURL: {a['url']}\nRESUMO: {a['summary']}"
        for i,a in enumerate(articles)
    ])
    prompt = f"""Es um curador de noticias para um executivo portugues. Categoria: {name}.

Selecciona os 3 artigos mais relevantes de hoje:

{body}

Responde APENAS com JSON valido (sem markdown, sem backticks):
[
  {{
    "title": "titulo original do artigo",
    "url": "url original",
    "source": "nome da fonte",
    "summary": "2-3 frases em portugues de Portugal, factual e directo",
    "why": "porque e relevante hoje, 1 frase",
    "impact": "impacto potencial concreto, 1 frase",
    "related": "ligacao com outros temas, 1 frase",
    "impact_level": "alto ou medio ou baixo"
  }}
]

Nao inventes URLs. Em portugues de Portugal. Se nao ha nada relevante devolve [].
"""
    try:
        r = MODEL.generate_content(prompt,
            generation_config={"temperature":0.2,"max_output_tokens":1200})
        text = re.sub(r"^```(?:json)?|```$","",r.text.strip(),flags=re.MULTILINE).strip()
        data = json.loads(text)
        for item in data:
            item["date"] = NOW_ISO
        return data[:3]
    except json.JSONDecodeError as e:
        print(f"  aviso JSON invalido ({cat}): {e}\n  Resposta: {r.text[:200]}")
        return []
    except Exception as e:
        print(f"  aviso Gemini ({cat}): {e}")
        return []


# ─── GEMINI — resumo de 3 minutos ─────────────────────────────────────
def gen_summary(categories):
    top = []
    for cat in ["breaking","geo","eco","nateco","ai","market"]:
        for a in categories.get(cat,[])[:1]:
            top.append(f"- [{CAT_NAMES.get(cat,cat)}] {a.get('title','')} | {a.get('url','')}")
        if len(top) >= 8: break
    if not top: return {"headline": f"Briefing {TODAY}", "items": []}

    hoje = datetime.now(timezone.utc)
    dia  = hoje.strftime("%-d de %B de %Y")
    prompt = f"""Com base nestas noticias de {dia}:
{chr(10).join(top)}

Cria exactamente 4 pontos para um resumo de 3 minutos para um executivo portugues ocupado.
Cada ponto = 1 frase curta e directa com o essencial.

Responde APENAS com JSON valido (sem markdown):
{{
  "headline": "Resumo de {dia}",
  "items": [
    {{"text": "frase 1", "color": "#EF4444", "source": "fonte", "time": "{hoje.strftime('%H:%M')}", "url": "url"}},
    {{"text": "frase 2", "color": "#F59E0B", "source": "fonte", "time": "{hoje.strftime('%H:%M')}", "url": "url"}},
    {{"text": "frase 3", "color": "#3B82F6", "source": "fonte", "time": "{hoje.strftime('%H:%M')}", "url": "url"}},
    {{"text": "frase 4", "color": "#10B981", "source": "fonte", "time": "{hoje.strftime('%H:%M')}", "url": "url"}}
  ]
}}

Em portugues de Portugal. Sem markdown."""
    try:
        r = MODEL.generate_content(prompt,
            generation_config={"temperature":0.1,"max_output_tokens":700})
        text = re.sub(r"^```(?:json)?|```$","",r.text.strip(),flags=re.MULTILINE).strip()
        return json.loads(text)
    except Exception as e:
        print(f"  aviso resumo: {e}")
        return {"headline": f"Briefing {TODAY}", "items": []}


# ─── GEMINI — analise de portfolio ────────────────────────────────────
def gen_portfolio_analysis(stocks_list, categories):
    top_news = []
    for cat in ["eco","market","geo"]:
        for a in categories.get(cat,[])[:1]:
            top_news.append(f"- {a.get('title','')}")

    stocks_txt = "\n".join([
        f"- {s['label']} ({s['symbol']}): {s['price']} ({'+' if s['up'] else ''}{s['change_pct']:.2f}%)"
        for s in stocks_list[:6]
    ])

    prompt = f"""Analisa este resumo de mercado para um investidor particular portugues:

COTACOES DE HOJE:
{stocks_txt}

NOTICIAS RELEVANTES:
{chr(10).join(top_news) if top_news else "Sem noticias disponiveis."}

Responde APENAS com JSON valido (sem markdown):
{{
  "total_change_pct": numero_medio_das_variacoes,
  "sentiment": "positivo ou neutro ou negativo",
  "what_happened": "1 frase sobre o que moveu os mercados hoje",
  "what_to_do": "1 accao concreta recomendada para hoje",
  "risks": "1 risco principal a vigiar",
  "tips": [
    {{"risk": "baixo",  "suggestion": "sugestao pratica de baixo risco"}},
    {{"risk": "medio",  "suggestion": "sugestao pratica de medio risco"}},
    {{"risk": "alto",   "suggestion": "sugestao pratica de alto risco"}}
  ]
}}

Em portugues de Portugal. Conservador e realista."""
    try:
        r = MODEL.generate_content(prompt,
            generation_config={"temperature":0.15,"max_output_tokens":600})
        text = re.sub(r"^```(?:json)?|```$","",r.text.strip(),flags=re.MULTILINE).strip()
        return json.loads(text)
    except Exception as e:
        print(f"  aviso portfolio analysis: {e}")
        return {
            "total_change_pct": 0,
            "sentiment": "neutro",
            "what_happened": "Dados insuficientes para analise.",
            "what_to_do": "Manter posicoes actuais.",
            "risks": "Volatilidade de mercado.",
            "tips": [
                {"risk":"baixo",  "suggestion":"ETF de indice diversificado"},
                {"risk":"medio",  "suggestion":"Accoes de qualidade com dividendo"},
                {"risk":"alto",   "suggestion":"Sem recomendacao especulativa hoje"},
            ]
        }


# ─── STOCKS — yfinance ────────────────────────────────────────────────
def fetch_stocks():
    result = []
    tickers = STOCKS + FOREX
    try:
        data = yf.download(tickers, period="5d", interval="1d",
                           progress=False, auto_adjust=True)
        close = data["Close"]
        for tk in tickers:
            try:
                if tk not in close.columns: continue
                prices = close[tk].dropna()
                if len(prices) < 1: continue
                p_now  = float(prices.iloc[-1])
                p_prev = float(prices.iloc[-2]) if len(prices) >= 2 else p_now
                chg    = p_now - p_prev
                pct    = (chg / p_prev * 100) if p_prev else 0
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


# ─── MAIN ─────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*50}")
    print(f"  Prisma — briefing {TODAY}")
    print(f"{'='*50}\n")

    # 1. Artigos por categoria
    categories = {}
    for cat in RSS:
        print(f"[{cat}] recolher...")
        raw = collect(cat)
        print(f"  {len(raw)} artigos → Gemini...")
        curated = curate(cat, raw)
        categories[cat] = curated
        print(f"  {len(curated)} seleccionados")
        time.sleep(2.5)  # respeita rate limit 15 req/min

    # 2. Resumo
    print("\n[resumo] a gerar...")
    summary = gen_summary(categories)
    print(f"  {len(summary.get('items',[]))} pontos")
    time.sleep(2)

    # 3. Stocks
    print("\n[stocks] a buscar...")
    stocks_list = fetch_stocks()
    print(f"  {len(stocks_list)} instrumentos")

    # 4. Portfolio analysis
    print("\n[portfolio] a analisar...")
    pa = gen_portfolio_analysis(stocks_list, categories)
    print("  analise gerada")

    # 5. Monta JSON no formato exacto que o HTML espera
    output = {
        "updated_at":        NOW_ISO,
        "categories":        categories,
        "summary":           summary,
        "stocks":            stocks_list,
        "portfolio_analysis": pa,
        "connections":       [],
    }

    # 6. Guarda ficheiros
    dated  = DATA / f"{TODAY}.json"
    latest = DATA / "latest.json"
    dated.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    latest.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n Guardado: {dated}")
    print(f" Guardado: {latest}")

    # 7. Actualiza index.json
    idx_path = DATA / "index.json"
    existing = {"dates": []}
    if idx_path.exists():
        try: existing = json.loads(idx_path.read_text())
        except: existing = {"dates": []}
    if TODAY not in existing.get("dates", []):
        existing["dates"].insert(0, TODAY)
    existing["dates"] = existing["dates"][:30]
    idx_path.write_text(json.dumps(existing, ensure_ascii=False))
    print(f" Index: {len(existing['dates'])} entradas")

    print(f"\n{'='*50}")
    print(f"  Briefing pronto! Prisma esta ao vivo.")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
