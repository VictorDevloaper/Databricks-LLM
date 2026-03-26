# Databricks notebook source

# ============================================================================
# ETAPA 5 — RAG CHAT (Retrieval-Augmented Generation)
# ============================================================================
# Objetivo: Permitir consultas em linguagem natural sobre os dados de
#           e-commerce usando RAG com Databricks Foundation Model APIs.
#
# Fluxo:
#   1. Usuário faz pergunta em linguagem natural
#   2. Sistema busca documentos relevantes via Vector Search
#   3. LLM gera resposta contextualizada com os dados encontrados
#
# Tecnologias:
#   - Databricks Vector Search (busca semântica)
#   - Databricks Foundation Model APIs (LLM para geração de respostas)
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 🤖 RAG Chat — Consulta Inteligente com IA
# MAGIC
# MAGIC Faça perguntas em linguagem natural sobre os dados de e-commerce
# MAGIC e receba respostas baseadas nos dados processados pelo pipeline.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import mlflow.deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Configurações

# COMMAND ----------

CATALOG_NAME = "workspace"
GOLD_SCHEMA = "gold_ecommerce"

# Vector Search
VS_ENDPOINT_NAME = "ecommerce_rag_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG_NAME}.{GOLD_SCHEMA}.embeddings_vs_index"

# LLM Endpoint (Databricks Foundation Model)
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"

# Configurações do RAG
TOP_K = 5  # Número de documentos retornados na busca vetorial
MAX_TOKENS = 1024
TEMPERATURE = 0.1

# System Prompt
SYSTEM_PROMPT = """Você é um assistente de análise de dados de e-commerce altamente especializado.
Seu objetivo é responder perguntas sobre vendas, clientes, produtos e métricas de negócio.

REGRAS IMPORTANTES:
1. Responda EXCLUSIVAMENTE com base nos dados fornecidos no CONTEXTO abaixo.
2. Se a informação não estiver disponível no contexto, diga claramente: "Não possuo dados suficientes para responder essa pergunta."
3. Sempre forneça números, valores e métricas quando disponíveis.
4. Use formato de moeda brasileira (R$) para valores monetários.
5. Seja conciso mas completo nas respostas.
6. Responda sempre em português brasileiro.
7. Quando listar itens, use formatação organizada com tópicos."""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Inicialização dos Clientes

# COMMAND ----------

# Inicializa clientes
vsc = VectorSearchClient()
deploy_client = mlflow.deployments.get_deploy_client("databricks")

# Obtém referência ao índice
vs_index = vsc.get_index(
    endpoint_name=VS_ENDPOINT_NAME,
    index_name=VS_INDEX_NAME
)

print("✅ Clientes inicializados com sucesso!")
print(f"  🔍 Vector Search: {VS_INDEX_NAME}")
print(f"  🤖 LLM: {LLM_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Funções do RAG

# COMMAND ----------

def search_relevant_documents(query: str, top_k: int = TOP_K) -> list:
    """
    Busca documentos relevantes no Vector Search com base na query.

    Args:
        query: Pergunta do usuário em linguagem natural
        top_k: Número de documentos a retornar

    Returns:
        Lista de documentos relevantes com texto e score
    """
    results = vs_index.similarity_search(
        query_text=query,
        columns=["doc_id", "doc_type", "text"],
        num_results=top_k
    )

    documents = []
    if results and results.get("result", {}).get("data_array"):
        for row in results["result"]["data_array"]:
            documents.append({
                "doc_id": row[0],
                "doc_type": row[1],
                "text": row[2],
                "score": row[3] if len(row) > 3 else None
            })

    return documents

# COMMAND ----------

def build_prompt(query: str, documents: list) -> str:
    """
    Constrói o prompt completo para o LLM com contexto dos documentos.

    Args:
        query: Pergunta do usuário
        documents: Lista de documentos relevantes

    Returns:
        String formatada com o prompt completo
    """
    # Monta o contexto com os documentos encontrados
    context_parts = []
    for i, doc in enumerate(documents, 1):
        context_parts.append(f"[Documento {i} - {doc['doc_type']}]\n{doc['text']}")

    context = "\n\n".join(context_parts)

    prompt = f"""CONTEXTO (dados do e-commerce):
{context}

PERGUNTA DO USUÁRIO:
{query}

Responda com base nos dados do contexto acima."""

    return prompt

# COMMAND ----------

def generate_response(query: str, context_prompt: str) -> str:
    """
    Gera resposta usando o LLM do Databricks Foundation Model APIs.

    Args:
        query: Pergunta original do usuário
        context_prompt: Prompt com contexto dos documentos

    Returns:
        Resposta gerada pelo LLM
    """
    response = deploy_client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": context_prompt}
            ],
            "max_tokens": MAX_TOKENS,
            "temperature": TEMPERATURE
        }
    )

    return response["choices"][0]["message"]["content"]

# COMMAND ----------

def rag_query(query: str, verbose: bool = True) -> str:
    """
    Executa o pipeline RAG completo:
    1. Busca documentos relevantes
    2. Monta prompt com contexto
    3. Gera resposta com LLM

    Args:
        query: Pergunta em linguagem natural
        verbose: Se True, mostra detalhes do processo

    Returns:
        Resposta gerada pelo LLM
    """
    if verbose:
        print("=" * 60)
        print(f"  ❓ Pergunta: {query}")
        print("=" * 60)

    # 1. Busca vetorial
    if verbose:
        print("\n🔍 Buscando documentos relevantes...")

    documents = search_relevant_documents(query)

    if verbose:
        print(f"  📄 {len(documents)} documentos encontrados:")
        for doc in documents:
            score_str = f" (score: {doc['score']:.4f})" if doc['score'] else ""
            print(f"    • [{doc['doc_type']}] {doc['doc_id']}{score_str}")

    if not documents:
        return "❌ Nenhum documento relevante encontrado para sua pergunta."

    # 2. Monta prompt
    if verbose:
        print("\n📝 Montando prompt com contexto...")

    context_prompt = build_prompt(query, documents)

    # 3. Gera resposta
    if verbose:
        print("🤖 Gerando resposta com IA...\n")

    response = generate_response(query, context_prompt)

    if verbose:
        print("─" * 60)
        print("  💬 RESPOSTA:")
        print("─" * 60)
        print(response)
        print("─" * 60)

    return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Exemplos de Perguntas
# MAGIC
# MAGIC Teste o sistema RAG com as perguntas abaixo.

# COMMAND ----------

# Pergunta 1: Receita
resposta_1 = rag_query("Qual foi a receita total do e-commerce?")

# COMMAND ----------

# Pergunta 2: Produtos mais vendidos
resposta_2 = rag_query("Quais são os 5 produtos mais vendidos?")

# COMMAND ----------

# Pergunta 3: Top clientes
resposta_3 = rag_query("Quem são os clientes que mais compraram? Liste os top 5.")

# COMMAND ----------

# Pergunta 4: Crescimento
resposta_4 = rag_query("Como foi o crescimento mensal de receita nos últimos meses?")

# COMMAND ----------

# Pergunta 5: Métodos de pagamento
resposta_5 = rag_query("Qual o método de pagamento mais utilizado e qual gera mais receita?")

# COMMAND ----------

# Pergunta 6: Categorias
resposta_6 = rag_query("Qual categoria de produto gera mais receita?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💬 Chat Interativo
# MAGIC
# MAGIC Execute a célula abaixo para iniciar um chat interativo.
# MAGIC Digite `sair` para encerrar.

# COMMAND ----------

def chat_interativo():
    """
    Inicia um loop de chat interativo para consultas RAG.
    Digite 'sair' para encerrar.
    """
    print("=" * 60)
    print("  🤖 CHAT IA — E-Commerce Analytics")
    print("  Faça perguntas sobre vendas, clientes e produtos.")
    print("  Digite 'sair' para encerrar.")
    print("=" * 60)

    while True:
        print()
        query = input("📝 Sua pergunta: ").strip()

        if not query:
            print("  ⚠️ Por favor, digite uma pergunta.")
            continue

        if query.lower() in ["sair", "exit", "quit", "q"]:
            print("\n  👋 Até logo! Obrigado por usar o Chat IA.")
            break

        try:
            rag_query(query)
        except Exception as e:
            print(f"\n  ❌ Erro ao processar pergunta: {e}")
            print("  Tente reformular sua pergunta.")

# Para iniciar o chat interativo, descomente a linha abaixo:
# chat_interativo()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Validações do RAG

# COMMAND ----------

print("=" * 60)
print("  📋 VALIDAÇÕES DO SISTEMA RAG")
print("=" * 60)

# 1. Verificar conexão com Vector Search
try:
    test_results = vs_index.similarity_search(
        query_text="receita total",
        columns=["doc_id", "doc_type", "text"],
        num_results=1
    )
    print(f"  ✅ Vector Search: Conexão OK")
    print(f"     Documentos indexados: acessíveis")
except Exception as e:
    print(f"  ❌ Vector Search: Erro - {e}")

# 2. Verificar conexão com LLM
try:
    test_response = deploy_client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={
            "messages": [{"role": "user", "content": "Responda apenas: OK"}],
            "max_tokens": 10,
            "temperature": 0
        }
    )
    print(f"  ✅ LLM ({LLM_ENDPOINT}): Conexão OK")
except Exception as e:
    print(f"  ❌ LLM ({LLM_ENDPOINT}): Erro - {e}")

# 3. Teste end-to-end
try:
    test_answer = rag_query("Qual a receita total?", verbose=False)
    has_answer = len(test_answer) > 20
    status = "✅" if has_answer else "⚠️"
    print(f"  {status} RAG End-to-End: {'Resposta gerada com sucesso' if has_answer else 'Resposta muito curta'}")
except Exception as e:
    print(f"  ❌ RAG End-to-End: Erro - {e}")

print("\n" + "=" * 60)
print("  🎉 VALIDAÇÕES DO RAG CONCLUÍDAS!")
print("=" * 60)
