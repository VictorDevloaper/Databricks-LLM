from __future__ import annotations

from fastapi import FastAPI, HTTPException  # type: ignore[import-untyped]
from fastapi.middleware.cors import CORSMiddleware  # type: ignore[import-untyped]
from pydantic import BaseModel, Field  # type: ignore[import-untyped]
from typing import TYPE_CHECKING, Any, List, Optional, Dict
import os
import json
import uvicorn  # type: ignore[import-untyped]

app = FastAPI(title="Databricks ERP RAG API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ──────────────────────────────────────────────────────────────
CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze_ecommerce"
SILVER_SCHEMA = "silver_ecommerce"
GOLD_SCHEMA = "gold_ecommerce"
VS_ENDPOINT_NAME = "ecommerce_rag_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_vs_index"
LLM_ENDPOINT = os.getenv("DATABRICKS_LLM_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct")
SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "0bfe3395d7f01318")

# Tabelas Gold
GOLD_REVENUE_SUMMARY = f"{CATALOG_NAME}.{GOLD_SCHEMA}.revenue_summary"
GOLD_TOP_PRODUCTS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_products"
GOLD_TOP_CUSTOMERS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.top_customers"
GOLD_KPIS = f"{CATALOG_NAME}.{GOLD_SCHEMA}.kpis"

# Tabelas por camada (para pipeline status)
PIPELINE_TABLES = [
    {
        "name": "01_bronze_ingestao",
        "desc": "Ingestão de dados brutos (JSON → Delta)",
        "tables": [
            f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers",
            f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products",
            f"{CATALOG_NAME}.{BRONZE_SCHEMA}.orders",
        ],
    },
    {
        "name": "02_silver_tratamento",
        "desc": "Limpeza e padronização dos dados",
        "tables": [
            f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers",
            f"{CATALOG_NAME}.{SILVER_SCHEMA}.products",
            f"{CATALOG_NAME}.{SILVER_SCHEMA}.orders",
        ],
    },
    {
        "name": "03_gold_kpis",
        "desc": "Geração de KPIs agregados",
        "tables": [GOLD_REVENUE_SUMMARY, GOLD_TOP_PRODUCTS, GOLD_TOP_CUSTOMERS, GOLD_KPIS],
    },
    {
        "name": "04_embeddings",
        "desc": "Geração de embeddings vetoriais",
        "tables": [f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_documents"],
    },
    {
        "name": "05_rag_chat",
        "desc": "Endpoint de Vector Search + LLM",
        "tables": [],
    },
]

# ── Pydantic Models ─────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    query: str
    history: Optional[List[Dict[str, str]]] = []

class InfoSource(BaseModel):
    id: str
    text: str
    score: Optional[float] = None

class ChartData(BaseModel):
    model_config = {"populate_by_name": True}

    chart_type: str = Field(alias="type")
    x: List[str]
    y: List[float]
    title: str
    xaxis_title: str = ""
    yaxis_title: str = ""

class ChatResponse(BaseModel):
    answer: str
    sources: List[InfoSource] = []
    chart: Optional[ChartData] = None

# ── Load .env (se existir) ───────────────────────────────────────────────
from pathlib import Path as _Path
_env_path = _Path(__file__).resolve().parent / ".env"
try:
    from dotenv import load_dotenv  # type: ignore[import-untyped]
    if _env_path.exists():
        load_dotenv(dotenv_path=str(_env_path), override=True)
        print(f"📄 .env carregado de {_env_path}")
    else:
        print(f"⚠️ Arquivo .env não encontrado em {_env_path}")
except ImportError:
    print("⚠️ python-dotenv não instalado, usando variáveis de ambiente do sistema")

# ── Databricks clients ──────────────────────────────────────────────────
vsc: Any = None
deploy_client: Any = None
w: Any = None
databricks_sql_available: bool = False
databricks_rag_available: bool = False


def try_init_clients():
    """Inicializa clientes Databricks em duas etapas independentes:
    1) WorkspaceClient (para SQL queries) — obrigatório para KPIs e Pipeline
    2) VectorSearch + MLflow (para RAG chat) — opcional
    """
    global vsc, deploy_client, w, databricks_sql_available, databricks_rag_available

    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    print(f"🔧 DATABRICKS_HOST={'definido' if host else 'NÃO DEFINIDO'}")
    print(f"🔧 DATABRICKS_TOKEN={'definido' if token else 'NÃO DEFINIDO'}")

    # --- Etapa 1: WorkspaceClient (SQL) ---
    if not databricks_sql_available:
        try:
            from databricks.sdk import WorkspaceClient  # type: ignore[import-untyped]

            w = WorkspaceClient(host=host, token=token) if host and token else WorkspaceClient()
            # Testa a conexão
            current_user = w.current_user.me()
            databricks_sql_available = True
            print(f"✅ Databricks SQL conectado! (user: {current_user.user_name})")
        except Exception as e:
            print(f"❌ Falha ao conectar WorkspaceClient: {e}")
            print("   Verifique DATABRICKS_HOST e DATABRICKS_TOKEN")

    # --- Etapa 2: VectorSearch + Gemini (RAG) ---
    if not databricks_rag_available:
        try:
            from databricks.vector_search.client import VectorSearchClient  # type: ignore[import-untyped]
            import google.generativeai as genai  # type: ignore[import-untyped]

            vsc = VectorSearchClient()
            
            gemini_key = os.getenv("GEMINI_API_KEY")
            if not gemini_key:
                raise ValueError("GEMINI_API_KEY não configurada no .env")
            genai.configure(api_key=gemini_key)
            deploy_client = True  # Mantém compatibilidade com a flag do roteador
            
            databricks_rag_available = True
            print("✅ Databricks RAG (VectorSearch + Gemini 1.5) conectado!")
        except Exception as e:
            print(f"⚠️ RAG não disponível (VectorSearch/Gemini): {e}")
            print("   Endpoints /api/kpis e /api/pipeline funcionam normalmente.")


@app.on_event("startup")
async def startup_event():
    """Inicializa clientes Databricks quando o worker do Uvicorn inicia."""
    try_init_clients()


# ── SQL Helper ──────────────────────────────────────────────────────────
def _execute_sql(query: str) -> list[dict[str, Any]]:
    """Executa uma query SQL no Databricks via Statement Execution API."""
    assert w is not None, "WorkspaceClient não inicializado"

    from databricks.sdk.service.sql import StatementState  # type: ignore[import-untyped]

    response = w.statement_execution.execute_statement(
        warehouse_id=SQL_WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )

    if response.status and response.status.state != StatementState.SUCCEEDED:
        error_msg = response.status.error.message if response.status.error else "Erro desconhecido"
        raise Exception(f"SQL error: {error_msg}")

    if not response.result or not response.result.data_array:
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows = []
    for row in response.result.data_array:
        rows.append(dict(zip(columns, row)))
    return rows


# ── Endpoints ───────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "status": "ok",
        "app": "Databricks ERP RAG API",
        "version": "1.0.0",
        "databricks_sql": databricks_sql_available,
        "databricks_rag": databricks_rag_available,
    }


@app.get("/api/kpis")
def get_kpis():
    """Retorna KPIs reais das tabelas Gold do Databricks."""
    if not databricks_sql_available:
        raise HTTPException(status_code=503, detail="Databricks SQL não está disponível. Verifique DATABRICKS_HOST e DATABRICKS_TOKEN.")

    try:
        # Receita total, pedidos, ticket médio
        revenue_rows = _execute_sql(f"""
            SELECT 
                COALESCE(SUM(total_revenue), 0) as total_revenue,
                COALESCE(SUM(total_orders), 0) as total_orders,
                COALESCE(AVG(avg_ticket), 0) as avg_ticket
            FROM {GOLD_REVENUE_SUMMARY}
        """)

        total_revenue = float(revenue_rows[0]["total_revenue"]) if revenue_rows else 0
        total_orders = int(float(revenue_rows[0]["total_orders"])) if revenue_rows else 0
        avg_ticket = float(revenue_rows[0]["avg_ticket"]) if revenue_rows else 0

        # Clientes ativos (distintos na tabela top_customers)
        customer_rows = _execute_sql(f"""
            SELECT COUNT(*) as active_customers
            FROM {GOLD_TOP_CUSTOMERS}
        """)
        active_customers = int(float(customer_rows[0]["active_customers"])) if customer_rows else 0

        # Top 5 produtos por quantidade
        product_rows = _execute_sql(f"""
            SELECT 
                product_name as name,
                total_quantity as sold,
                total_revenue as revenue
            FROM {GOLD_TOP_PRODUCTS}
            ORDER BY rank_quantity ASC
            LIMIT 5
        """)
        top_products = [
            {
                "name": row["name"],
                "sold": int(float(row["sold"])),
                "revenue": float(row["revenue"]),
            }
            for row in product_rows
        ]

        # Receita mensal
        monthly_rows = _execute_sql(f"""
            SELECT 
                order_year_month as month,
                total_revenue as revenue
            FROM {GOLD_REVENUE_SUMMARY}
            ORDER BY order_year_month ASC
        """)
        monthly_revenue = [
            {
                "month": row["month"],
                "revenue": float(row["revenue"]),
            }
            for row in monthly_rows
        ]

        return {
            "totalRevenue": total_revenue,
            "totalOrders": total_orders,
            "avgTicket": round(float(avg_ticket), 2),  # type: ignore[call-overload]
            "activeCustomers": active_customers,
            "topProducts": top_products,
            "monthlyRevenue": monthly_revenue,
        }

    except Exception as e:
        print(f"❌ Erro ao consultar KPIs: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar KPIs do Databricks: {str(e)}")


@app.get("/api/pipeline")
def get_pipeline():
    """Retorna status real do pipeline consultando cada tabela no Databricks."""
    if not databricks_sql_available:
        raise HTTPException(status_code=503, detail="Databricks SQL não está disponível")

    pipeline_status: list[dict[str, Any]] = []

    for step in PIPELINE_TABLES:
        total_records: int = 0
        last_run = "—"
        status = "success"

        if not step["tables"]:
            # Etapa sem tabela (ex: rag_chat endpoint)
            # Verifica se o Vector Search endpoint existe
            try:
                assert vsc is not None
                vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
                status = "success"
                last_run = "Endpoint ativo"
            except Exception:
                status = "warning"
                last_run = "Endpoint não verificado"

            pipeline_status.append({
                "name": step["name"],
                "desc": step["desc"],
                "status": status,
                "records": None,
                "lastRun": last_run,
            })
            continue

        for table in step["tables"]:
            try:
                rows = _execute_sql(f"SELECT COUNT(*) as cnt FROM {table}")
                count = int(float(rows[0]["cnt"])) if rows else 0
                total_records += count  # type: ignore[operator]

                # Tenta pegar o timestamp de processamento mais recente
                try:
                    ts_rows = _execute_sql(f"""
                        SELECT MAX(_processed_timestamp) as last_ts 
                        FROM {table}
                    """)
                    if ts_rows and ts_rows[0].get("last_ts"):
                        last_ts_val = str(ts_rows[0]["last_ts"])
                        last_run = last_ts_val[:19]  # type: ignore[index]
                except Exception:
                    pass  # Nem todas as tabelas têm _processed_timestamp

            except Exception as e:
                print(f"⚠️ Erro ao consultar {table}: {e}")
                status = "error"

        pipeline_status.append({
            "name": step["name"],
            "desc": step["desc"],
            "status": status,
            "records": total_records if total_records > 0 else None,
            "lastRun": last_run,
        })

    return pipeline_status


@app.post("/api/chat", response_model=ChatResponse)
def chat_endpoint(req: ChatRequest):
    """Chat RAG — consulta Vector Search + LLM no Databricks."""
    if not databricks_rag_available or not deploy_client:
        raise HTTPException(
            status_code=503,
            detail="Databricks não está disponível. Configure DATABRICKS_HOST e DATABRICKS_TOKEN.",
        )

    try:
        docs = _search_docs(req.query)
        is_analytical = any(kw in req.query.lower() for kw in ["top", "receita", "venda", "cliente", "produto", "kpi", "quantidade", "faturamento"])
        
        sources = []
        if databricks_sql_available:
            sources.append(InfoSource.model_validate({
                "id": "Databricks SQL Warehouse (Gold)",
                "text": "Fonte exata: Tabelas agregadas de KPIs, Top Produtos e Top Clientes com valores atualizados e matematicamente precisos.",
                "score": 1.0
            }))
            
        # Remove os resultados de texto do Vector Search da interface se for uma pergunta puramente analítica
        # para não confundir o usuário mostrando produtos aleatórios que não tem a ver com a resposta.
        if not is_analytical:
            for d in docs:
                sources.append(InfoSource.model_validate({"id": d["id"], "text": d["text"], "score": d.get("score")}))
                
        prompt = _build_prompt(req.query, docs if not is_analytical else [])
        import google.generativeai as genai  # type: ignore[import-untyped]
        
        # O Gemini 3.0 suporta intruções do sistema
        model = genai.GenerativeModel("gemini-3-flash-preview", system_instruction=SYSTEM_PROMPT)
        
        # Constrói o histórico no formato esperado pelo Gemini
        gemini_history = []
        for msg in req.history or []:
            role = "user" if msg.get("role") == "user" else "model"
            gemini_history.append({"role": role, "parts": [msg.get("content", "")]})
            
        chat = model.start_chat(history=gemini_history)
        response = chat.send_message(prompt)
        answer_raw = response.text
        answer_text, chart_data = _extract_chart(answer_raw)
        return ChatResponse(answer=answer_text, sources=sources, chart=chart_data)  # type: ignore[call-arg]

    except Exception as e:
        print(f"❌ Erro no chat RAG: {e}")
        raise HTTPException(status_code=500, detail=f"Erro no processamento RAG: {str(e)}")


# ── RAG Helpers ─────────────────────────────────────────────────────────
SYSTEM_PROMPT = """Voce e um assistente ERP de analise de dados de e-commerce.
Responda APENAS com base nos dados fornecidos. Seja conciso e profissional.
Responda em Portugues Brasileiro (PT-BR). Cite as fontes no final.
Se a analise contiver ranking/top/serie-temporal, gere um JSON entre *** delimitadores:
***
{"chart_type": "bar", "x": ["A","B"], "y": [10,20], "title": "T", "xaxis": "X", "yaxis": "Y"}
***"""


def _search_docs(query: str) -> list[dict[str, Any]]:
    try:
        assert vsc is not None
        vs_index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
        results = vs_index.similarity_search(
            query_text=query, columns=["doc_id", "doc_type", "text"], num_results=5
        )
        docs = []
        if results and results.get("result", {}).get("data_array"):
            for row in results["result"]["data_array"]:  # type: ignore[index]
                docs.append({
                    "id": row[0],
                    "type": row[1],
                    "text": row[2],
                    "score": row[3] if len(row) > 3 else None,
                })
        return docs
    except Exception as e:
        print(f"⚠️ Erro no Vector Search: {e}")
        return []


def _get_exact_kpis_context() -> str:
    try:
        if not databricks_sql_available:
            return ""
        
        rows_kpis = _execute_sql(f"SELECT total_revenue, total_orders, avg_ticket FROM {GOLD_KPIS} LIMIT 1")
        kpi_text = ""
        if rows_kpis:
            k = rows_kpis[0]
            kpi_text = f"Métricas Gerais (Exatas): {k['total_orders']} pedidos, Receita R$ {k['total_revenue']}, Ticket Médio R$ {k['avg_ticket']}."
            
        rows_prod_rev = _execute_sql(f"SELECT product_name, total_quantity, total_revenue FROM {GOLD_TOP_PRODUCTS} ORDER BY total_revenue DESC LIMIT 5")
        rows_prod_qtd = _execute_sql(f"SELECT product_name, total_quantity, total_revenue FROM {GOLD_TOP_PRODUCTS} ORDER BY total_quantity DESC LIMIT 5")
        
        prod_text = "Top Produtos por Receita (Exatos):\n" + "\n".join([f"- {r['product_name']}: {r['total_quantity']} unidades, Receita R$ {r['total_revenue']}" for r in rows_prod_rev])
        prod_text += "\n\nTop Produtos por Volume de Vendas (Exatos):\n" + "\n".join([f"- {r['product_name']}: {r['total_quantity']} unidades, Receita R$ {r['total_revenue']}" for r in rows_prod_qtd])
        
        rows_cust = _execute_sql(f"SELECT customer_name, total_revenue, total_orders FROM {GOLD_TOP_CUSTOMERS} ORDER BY total_revenue DESC LIMIT 5")
        cust_text = "Top Clientes (Exatos):\n" + "\n".join([f"- {r['customer_name']}: {r['total_orders']} pedidos, Receita R$ {r['total_revenue']}" for r in rows_cust])
        
        return f"[DADOS EXATOS DO BANCO (USE ESTES CASO HAJA CONFLITO COM O VECTOR SEARCH)]\n{kpi_text}\n\n{prod_text}\n\n{cust_text}\n"
    except Exception as e:
        print(f"Erro ao buscar contexto exato SQL: {e}")
        return ""

def _build_prompt(query: str, docs: list) -> str:
    exact_data = _get_exact_kpis_context()
    context = "\n\n".join([f"[Doc {i+1} - {d['type']}]\n{d['text']}" for i, d in enumerate(docs)])
    return f"CONTEXTO DO VECTOR SEARCH:\n{context}\n\n{exact_data}\n\nPERGUNTA:\n{query}"


def _extract_chart(text: str):
    if "***" not in text:
        return text, None
    parts = text.split("***")
    answer = parts[0].strip()
    try:
        chart_json = json.loads(parts[1].strip())
        return answer, ChartData.model_validate({
            "type": chart_json.get("chart_type", "bar"),
            "x": chart_json.get("x", []),
            "y": chart_json.get("y", []),
            "title": chart_json.get("title", ""),
            "xaxis_title": chart_json.get("xaxis", ""),
            "yaxis_title": chart_json.get("yaxis", ""),
        })
    except Exception:
        return answer, None


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
