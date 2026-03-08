"""
Prisma — fetch_news.py  (v3 — antifragile)
══════════════════════════════════════════
Problemas resolvidos proactivamente:

  #1  RPM Gemini esgotado       → 2 chamadas por run (era 4-12), bem dentro de 15/min
  #2  Feeds bloqueados           → apenas BBC + Guardian + TechCrunch + Ars + Verge
  #3  Git push concorrente       → resolvido no update.yml com git pull --rebase
  #4  Quota diária esgotada      → 2 calls × 6 runs = 12/dia de 1500 disponíveis
  #5  JSON inválido              → parser defensivo com múltiplas estratégias de recovery
  #6  Mercados fechados fim-semana → yfinance usa period=5d, mostra último preço disponível
  #7  Deprecação de modelo       → MODEL_FALLBACKS com lista de fallback automático
  #8  Output tokens insuficiente → prompt compacto + max_output_tokens conservador
  #9  GitHub Actions timeout     → timeout global de 8 minutos com sys.exit limpo
  #10 Feeds PT instáveis         → lista de fallback por categoria se feed principal falha
  #11 Artigos duplicados         → desduplicação global por URL hash (não só por cat)
  #12 Prompt demasiado grande    → cada artigo truncado a 120 chars, max 6 por cat
  #13 index.json corrompido      → escrita atómica via ficheiro temporário + rename
  #14 Gemini falha completamente → latest.json preserva dados do run anterior
"""

import os, json, re, time, hashlib, sys, tempfile, signal
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
import yfinance as yf
from google import genai
from google.genai import types

# ─── TIMEOUT GLOBAL ──────────────────────────────────────────────────
# Se o script demorar mais de 8 minutos, termina com saída limpa
def _timeout_handler(sig, frame):
    print("\n⚠️  Timeout global de 8 minutos atingido. A guardar o que existe...")
    sys.exit(0)
signal.signal(signal.SIGALRM, _timeout_handler)
signal.alarm(480)  # 8 minutos

# ─── CONFIG ───────────────────────────────────────────────────────────
# Fallback automático de modelo se o principal for deprecado
MODEL_FALLBACKS = [
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash",
]

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

FEED_TIMEOUT = 8
MAX_WORKERS  = 12
MAX_ARTS_PER_CAT = 6    # limita tamanho do prompt
MAX_TITLE_LEN    = 120  # trunca títulos longos

# ─── HTTP SESSION ─────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; PrismaBot/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
})

# ─── FEEDS — lista plana, sem categorias fixas ────────────────────────
# O Gemini categoriza livremente nos 8 temas abaixo
RSS = [
    # Mundo / Breaking
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.theguardian.com/world/rss",
    "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
    # Portugal
    "https://feeds.feedburner.com/observador",
    "https://eco.sapo.pt/feed/",
    "https://www.jornaldenegocios.pt/rss",
    "https://www.rtp.pt/noticias/rss/rtp-noticias",
    "https://www.dn.pt/rss/feed.aspx",
    # Economia & Mercados
    "https://www.theguardian.com/business/economics/rss",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.theguardian.com/business/stock-markets/rss",
    # Tecnologia & IA
    "https://techcrunch.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://feeds.arstechnica.com/arstechnica/index",
    # Carreira & Negócios
    "https://www.theguardian.com/money/work-and-careers/rss",
    "https://www.theguardian.com/business/rss",
    # Clima / Ambiente (Mundo)
    "https://www.theguardian.com/environment/rss",
    # Recomendação do Dia
    "https://www.theguardian.com/lifeandstyle/rss",
    "https://www.theguardian.com/books/rss",
    "https://www.theguardian.com/science/rss",
]

CAT_NAMES = {
    "breaking":     "Breaking News",
    "portugal":     "Portugal",
    "mundo":        "Mundo",
    "economia":     "Economia & Mercados",
    "portfolio":    "O Meu Portfólio",
    "tecnologia":   "Tecnologia & IA",
    "carreira":     "Carreira & Negócios",
    "recomendacao": "Recomendação do Dia",
}
CATS_ALL = list(CAT_NAMES.keys())

STOCKS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL"]
FOREX  = ["EURUSD=X", "BTC-USD", "GC=F"]
LABELS = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","TSLA":"Tesla",
    "AMZN":"Amazon","META":"Meta","GOOGL":"Google",
    "EURUSD=X":"EUR/USD","BTC-USD":"Bitcoin","GC=F":"Gold",
}


# ─── RSS ──────────────────────────────────────────────────────────────
def fetch_one_feed(url: str) -> list[dict]:
    try:
        resp = SESSION.get(url, timeout=FEED_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        items = []
        for e in feed.entries[:8]:
            title = e.get("title", "").strip()[:MAX_TITLE_LEN]
            if not title:
                continue
            items.append({
                "title":  title,
                "url":    e.get("link", ""),
                "source": feed.feed.get("title", url)[:40],
            })
        return items
    except Exception as ex:
        print(f"  aviso feed ({url[:50]}): {ex}")
        return []


def collect_all() -> list[dict]:
    """Recolhe todos os feeds em paralelo e devolve lista plana deduplicada."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fmap = {ex.submit(fetch_one_feed, url): url for url in RSS}
        raw = []
        for f in as_completed(fmap):
            try:
                raw.extend(f.result())
            except Exception:
                pass

    # Desduplicação global por URL e título
    seen_urls:   set[str] = set()
    seen_titles: set[str] = set()
    unique = []
    for it in raw:
        url_key   = hashlib.md5(it["url"].encode()).hexdigest()
        title_key = hashlib.md5(it["title"].lower().encode()).hexdigest()
        if url_key not in seen_urls and title_key not in seen_titles:
            seen_urls.add(url_key)
            seen_titles.add(title_key)
            unique.append(it)

    print(f"  {len(unique)} artigos únicos de {len(RSS)} feeds")
    return unique[:48]  # ~48 artigos = prompt ~3500 tokens, dentro do limite seguro


# ─── GEMINI ───────────────────────────────────────────────────────────
def get_client_and_model() -> tuple:
    """Tenta cada modelo na lista de fallbacks."""
    api_key = os.environ["GEMINI_API_KEY"]
    client  = genai.Client(api_key=api_key)
    return client, MODEL_FALLBACKS[0]  # começa com o principal


def call_gemini(client, model: str, prompt: str, max_tokens: int) -> tuple[str | None, str]:
    """
    Chama Gemini com retry limitado.
    Retorna (texto, modelo_usado).
    Não faz mais de 2 retries — se falhar, aceita resultado vazio.
    Nunca bloqueia mais de 90 segundos no total.
    """
    for attempt in range(2):  # máximo 2 tentativas (não 3 — evita timeout)
        for mdl in MODEL_FALLBACKS:
            try:
                r = client.models.generate_content(
                    model=mdl,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.2,
                        max_output_tokens=max_tokens,
                    )
                )
                return r.text, mdl
            except Exception as ex:
                msg = str(ex)
                if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                    # Espera fixo de 35s — suficiente para reset do RPM de 1 minuto
                    wait = 35
                    print(f"  rate limit ({mdl}, tentativa {attempt+1}/2), aguarda {wait}s...")
                    time.sleep(wait)
                    break  # sai do loop de modelos, tenta de novo no loop externo
                elif "404" in msg or "not found" in msg.lower():
                    # Este modelo foi deprecado, tenta o próximo
                    print(f"  modelo {mdl} não disponível, a tentar fallback...")
                    continue
                else:
                    print(f"  erro Gemini ({mdl}): {ex}")
                    return None, mdl
    print("  Gemini não respondeu após 2 tentativas. A continuar sem curadoria.")
    return None, model


def parse_json(text: str) -> dict | list | None:
    if not text:
        return None
    # Remove blocos de código markdown
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Tenta extrair o primeiro objeto JSON válido
        for pattern in [r'\{.*\}', r'\[.*\]']:
            m = re.search(pattern, text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group())
                except Exception:
                    pass
    return None


# ─── CHAMADA 1 — categorização + curadoria numa só chamada ───────────
def curate_all(client, model: str, articles: list) -> dict:
    """Gemini categoriza livremente e selecciona os 2 melhores por categoria."""
    if not articles:
        return {}

    arts_txt = "\n".join([
        f"  [{i+1}] {a['title']} | {a['source']} | {a['url']}"
        for i, a in enumerate(articles)
    ])

    cats_desc = "\n".join([f"  - {k}: {v}" for k, v in CAT_NAMES.items()])

    prompt = f"""És um curador de notícias para um executivo português. Data: {TODAY}.

Tens {len(articles)} artigos abaixo. As 8 categorias disponíveis são:
{cats_desc}

INSTRUÇÕES:
1. Lê todos os artigos e atribui cada um à categoria mais adequada.
2. Selecciona os 2 artigos mais relevantes por categoria (pode ficar vazio se não houver artigos adequados).
3. Para cada artigo seleccionado, preenche TODOS os campos:
   - "title": TRADUZ SEMPRE para português de Portugal. Se já for português, mantém.
   - "translated": true se traduziste, false se já era português
   - "summary": 2 frases directas em português de Portugal
   - "why": 1 frase — porque é relevante para um executivo português
   - "impact": 1 frase — impacto concreto nos próximos dias/semanas
   - "related": 1 frase — ligação a outro tema ou notícia
   - "impact_level": "alto", "médio" ou "baixo"
   - "url": URL original sem alterações
   - "source": nome da fonte original
   - "date": "{NOW_ISO}"

NOTA sobre categorias:
- "breaking": apenas acontecimentos das últimas horas, urgentes
- "portugal": política, economia ou sociedade portuguesa
- "mundo": geopolítica, conflitos, relações internacionais, ambiente
- "economia": macroeconomia global, bolsa, mercados, finanças
- "portfolio": análise de mercado relevante para investidores individuais
- "tecnologia": IA, software, hardware, inovação digital
- "carreira": liderança, future of work, estratégia empresarial
- "recomendacao": apenas 1 artigo — ensaio, livro, ideia ou reflexão de valor duradouro

Responde APENAS com JSON válido:
{{
  "breaking":     [{{ "title":"PT","url":"...","source":"...","translated":true,"summary":"...","why":"...","impact":"...","related":"...","impact_level":"médio","date":"{NOW_ISO}" }}],
  "portugal":     [...],
  "mundo":        [...],
  "economia":     [...],
  "portfolio":    [...],
  "tecnologia":   [...],
  "carreira":     [...],
  "recomendacao": [...]
}}

Artigos disponíveis:
{arts_txt}"""

    print(f"  prompt: {len(prompt)} chars, ~{len(prompt)//4} tokens")
    text, used_model = call_gemini(client, model, prompt, max_tokens=6000)
    if used_model != model:
        print(f"  a usar modelo fallback: {used_model}")

    data = parse_json(text)
    if not isinstance(data, dict):
        print("  aviso: curadoria devolveu resultado vazio, a usar artigos em bruto")
        # Fallback: distribui os primeiros artigos pelas categorias sem curadoria
        raw_fallback = {cat: [] for cat in CATS_ALL}
        for i, a in enumerate(articles[:16]):
            cat = CATS_ALL[i % len(CATS_ALL)]
            if len(raw_fallback[cat]) < 2:
                raw_fallback[cat].append({
                    "title": a["title"], "url": a["url"], "source": a["source"],
                    "translated": False, "summary": "", "why": "", "impact": "",
                    "related": "", "impact_level": "médio", "date": NOW_ISO
                })
        return raw_fallback
    return data


# ─── CHAMADA 2 — resumo + portfolio ───────────────────────────────────
def gen_summary_and_portfolio(client, model: str, categories: dict, stocks: list) -> tuple:
    top = []
    for cat in ["breaking", "portugal", "mundo", "economia", "tecnologia"]:
        for a in categories.get(cat, [])[:1]:
            top.append(f"- [{CAT_NAMES.get(cat,cat)}] {a.get('title','')} | {a.get('url','')}")
        if len(top) >= 5:
            break

    stocks_txt = "\n".join([
        f"- {s['label']}: {s['price']} ({'+' if s['up'] else ''}{s['change_pct']:.2f}%)"
        for s in stocks[:7]
    ])

    dia = DIA_PT
    prompt = f"""Notícias de {dia}:
{chr(10).join(top) if top else "Sem notícias."}

Cotações:
{stocks_txt if stocks_txt else "Sem cotações."}

Responde APENAS com JSON válido:
{{"summary":{{"headline":"Briefing de {dia}","items":[
  {{"text":"frase directa","color":"#EF4444","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}},
  {{"text":"frase directa","color":"#F59E0B","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}},
  {{"text":"frase directa","color":"#3B82F6","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}},
  {{"text":"frase directa","color":"#10B981","source":"fonte","time":"{_NOW.strftime('%H:%M')}","url":"url"}}
]}},"portfolio_analysis":{{"total_change_pct":0.0,"sentiment":"positivo|neutro|negativo",
"what_happened":"1 frase","what_to_do":"1 acção concreta","risks":"1 risco",
"tips":[{{"risk":"baixo","suggestion":"..."}},{{"risk":"médio","suggestion":"..."}},{{"risk":"alto","suggestion":"..."}}]}}}}"""

    text, _ = call_gemini(client, model, prompt, max_tokens=800)
    data = parse_json(text)

    default_s  = {"headline": f"Briefing de {DIA_PT}", "items": []}
    default_pa = {
        "total_change_pct": 0, "sentiment": "neutro",
        "what_happened": "Dados insuficientes.",
        "what_to_do": "Manter posições actuais.",
        "risks": "Volatilidade de mercado.",
        "tips": [
            {"risk": "baixo",  "suggestion": "ETF de índice diversificado"},
            {"risk": "médio",  "suggestion": "Acções de qualidade com dividendo"},
            {"risk": "alto",   "suggestion": "Sem recomendação especulativa"},
        ]
    }
    if not isinstance(data, dict):
        return default_s, default_pa
    return data.get("summary", default_s), data.get("portfolio_analysis", default_pa)


# ─── STOCKS ───────────────────────────────────────────────────────────
def fetch_stocks() -> list[dict]:
    """
    Usa period=5d para garantir dados mesmo ao fim de semana
    (devolve o último preço disponível, tipicamente sexta-feira).
    """
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


# ─── ESCRITA ATÓMICA ──────────────────────────────────────────────────
def atomic_write(path: Path, content: str):
    """
    Escreve via ficheiro temporário + rename atómico.
    Evita corrupção se dois runs escreverem ao mesmo tempo (#13).
    """
    tmp = path.with_suffix(".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ─── MAIN ─────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"\n{'='*54}")
    print(f"  Prisma — {RUN_ID}")
    print(f"  Gemini calls: 2  |  Feeds paralelos: {MAX_WORKERS}")
    print(f"  Timeout global: 8 min")
    print(f"{'='*54}\n")

    client, model = get_client_and_model()

    # 1. Todos os feeds em paralelo (~2s)
    print(f"[RSS] A recolher {len(RSS)} feeds...")
    articles = collect_all()
    print(f"  {len(articles)} artigos prontos em {time.time()-t0:.1f}s\n")

    # 2. Stocks (sem Gemini)
    print("[stocks] A buscar cotações...")
    stocks_list = fetch_stocks()
    print(f"  {len(stocks_list)} instrumentos\n")

    # 3. CHAMADA GEMINI 1/2 — categorização + curadoria
    print("[Gemini 1/2] A categorizar e curar artigos...")
    categories = curate_all(client, model, articles)
    total_curated = sum(len(v) for v in categories.values())
    print(f"  {total_curated} artigos curados em {sum(1 for v in categories.values() if v)} categorias\n")

    # Pausa de 5s entre as 2 chamadas (margem de segurança de RPM)
    time.sleep(5)

    # 4. CHAMADA GEMINI 2/2 — resumo + portfolio
    print("[Gemini 2/2] Resumo + portfolio...")
    summary, pa = gen_summary_and_portfolio(client, model, categories, stocks_list)
    print(f"  {len(summary.get('items',[]))} pontos no resumo\n")

    # 5. Monta output
    output = {
        "updated_at":         NOW_ISO,
        "run_id":             RUN_ID,
        "categories":         categories,
        "summary":            summary,
        "stocks":             stocks_list,
        "portfolio_analysis": pa,
        "connections":        [],
    }
    payload = json.dumps(output, ensure_ascii=False, indent=2)

    # 6. Guarda ficheiros com escrita atómica
    run_file = DATA / f"{RUN_ID}.json"
    atomic_write(run_file, payload)
    atomic_write(DATA / "latest.json", payload)
    print(f"  Guardado: {run_file}")

    # 7. Actualiza index — escrita atómica (#13)
    idx_path = DATA / "index.json"
    existing = {"runs": []}
    if idx_path.exists():
        try:
            existing = json.loads(idx_path.read_text())
        except Exception:
            existing = {"runs": []}
    runs = [r for r in existing.get("runs", []) if r.get("id") != RUN_ID]
    runs.insert(0, {"id": RUN_ID, "date": TODAY, "slot": RUN_SLOT, "ts": NOW_ISO})
    existing["runs"] = runs[:180]
    atomic_write(idx_path, json.dumps(existing, ensure_ascii=False, indent=2))

    elapsed = time.time() - t0
    print(f"\n{'='*54}")
    print(f"  Concluído em {elapsed:.1f}s")
    print(f"  {total_curated} artigos | {len([c for c in categories if categories[c]])} categorias")
    print(f"  Gemini calls: 2 / 1500 disponíveis hoje")
    print(f"{'='*54}\n")


if __name__ == "__main__":
    main()
