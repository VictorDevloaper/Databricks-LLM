# ============================================================================
# Deploy Script - Upload do Projeto para o Databricks
# ============================================================================
# Este script automatiza o upload de dados e notebooks para o workspace
# Databricks usando o Databricks SDK.
#
# Pre-requisitos:
#   1. pip install databricks-sdk
#   2. Configurar autenticacao (uma das opcoes):
#      a) Variavel de ambiente: DATABRICKS_HOST e DATABRICKS_TOKEN
#      b) Databricks CLI: databricks configure
#      c) O script pedira interativamente
#
# Uso:
#   python deploy.py
# ============================================================================

import os
import sys
import json
from pathlib import Path

# Diretorio raiz do projeto
PROJECT_DIR = Path(__file__).parent
DATA_RAW_DIR = PROJECT_DIR / "data" / "raw"
NOTEBOOKS_DIR = PROJECT_DIR / "notebooks"

# Configuracoes do Databricks (ajuste conforme seu workspace)
CATALOG_NAME = "workspace"
BRONZE_SCHEMA = "bronze_ecommerce"
SILVER_SCHEMA = "silver_ecommerce"
GOLD_SCHEMA = "gold_ecommerce"

# Caminhos no Databricks
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/raw_data"
WORKSPACE_NOTEBOOKS_PATH = "/Users/{user_email}/project_databricks_rag/notebooks"


def get_workspace_client():
    """Inicializa o cliente Databricks com autenticacao."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("ERRO: databricks-sdk nao instalado.")
        print("Execute: pip install databricks-sdk")
        sys.exit(1)

    # Tenta criar o cliente (usa config existente ou variaveis de ambiente)
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if not host:
        print("\n" + "=" * 60)
        print("  CONFIGURACAO DO DATABRICKS")
        print("=" * 60)
        host = input("\n  URL do workspace (ex: https://adb-xxx.azuredatabricks.net): ").strip()
        if not host:
            print("  ERRO: URL do workspace e obrigatoria.")
            sys.exit(1)

    if not token:
        token = input("  Personal Access Token: ").strip()
        if not token:
            print("  ERRO: Token e obrigatorio.")
            print("  Gere um token em: Settings > Developer > Access Tokens")
            sys.exit(1)

    try:
        w = WorkspaceClient(host=host, token=token)
        # Testa a conexao
        current_user = w.current_user.me()
        print(f"\n  Conectado como: {current_user.user_name}")
        return w, current_user.user_name
    except Exception as e:
        print(f"\n  ERRO ao conectar: {e}")
        print("  Verifique a URL e o token.")
        sys.exit(1)


def create_volume(w):
    """Cria o Unity Catalog Volume para armazenar dados raw."""
    print("\n" + "-" * 60)
    print("  1/4 - Criando Catalogo, Schema e Volume...")
    print("-" * 60)

    try:
        # Cria catalogo (ignora se ja existe)
        try:
            w.catalogs.create(name=CATALOG_NAME)
            print(f"  + Catalogo '{CATALOG_NAME}' criado")
        except Exception:
            print(f"  = Catalogo '{CATALOG_NAME}' ja existe")

        # Cria schemas (ignora se ja existe)
        for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
            try:
                w.schemas.create(name=schema, catalog_name=CATALOG_NAME)
                print(f"  + Schema '{schema}' criado")
            except Exception:
                print(f"  = Schema '{schema}' ja existe")

        # Cria volume na camada Bronze (ignora se ja existe)
        try:
            from databricks.sdk.service.catalog import VolumeType
            w.volumes.create(
                catalog_name=CATALOG_NAME,
                schema_name=BRONZE_SCHEMA,
                name="raw_data",
                volume_type=VolumeType.MANAGED
            )
            print(f"  + Volume 'raw_data' criado em {BRONZE_SCHEMA}")
        except Exception:
            print(f"  = Volume 'raw_data' ja existe em {BRONZE_SCHEMA}")

        return True
    except Exception as e:
        print(f"  ERRO: {e}")
        return False


def upload_data_files(w):
    """Faz upload dos arquivos JSON para o Volume."""
    print("\n" + "-" * 60)
    print("  2/4 - Fazendo upload dos dados para o Volume...")
    print("-" * 60)

    json_files = list(DATA_RAW_DIR.glob("*.json"))

    if not json_files:
        print("  AVISO: Nenhum arquivo JSON encontrado em data/raw/")
        print("  Execute primeiro: python data/generate_data.py")
        return False

    for json_file in json_files:
        remote_path = f"{VOLUME_PATH}/{json_file.name}"
        try:
            with open(json_file, "rb") as f:
                w.files.upload(remote_path, f, overwrite=True)
            size_kb = json_file.stat().st_size / 1024
            print(f"  + {json_file.name} ({size_kb:.1f} KB) -> {remote_path}")
        except Exception as e:
            print(f"  ERRO ao enviar {json_file.name}: {e}")
            return False

    print(f"\n  {len(json_files)} arquivos enviados com sucesso!")
    return True


def upload_notebooks(w, user_email):
    """Importa os notebooks no workspace Databricks."""
    print("\n" + "-" * 60)
    print("  3/4 - Importando notebooks no workspace...")
    print("-" * 60)

    notebooks_path = WORKSPACE_NOTEBOOKS_PATH.format(user_email=user_email)

    # Cria o diretorio parente se nao existir
    try:
        w.workspace.mkdirs(notebooks_path)
    except Exception as e:
        print(f"  AVISO ao criar diretorio {notebooks_path}: {e}")

    notebook_files = sorted(NOTEBOOKS_DIR.glob("*.py"))

    if not notebook_files:
        print("  ERRO: Nenhum notebook encontrado em notebooks/")
        return False

    for nb_file in notebook_files:
        remote_path = f"{notebooks_path}/{nb_file.stem}"
        try:
            with open(nb_file, "r", encoding="utf-8") as f:
                content = f.read()

            import base64
            from databricks.sdk.service.workspace import ImportFormat, Language

            w.workspace.import_(
                path=remote_path,
                content=base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True
            )
            print(f"  + {nb_file.name} -> {remote_path}")
        except Exception as e:
            print(f"  ERRO ao importar {nb_file.name}: {e}")
            return False

    print(f"\n  {len(notebook_files)} notebooks importados com sucesso!")
    print(f"  Caminho: {notebooks_path}")
    return True


def update_data_path_info():
    """Exibe instrucoes sobre o DATA_RAW_PATH."""
    print("\n" + "-" * 60)
    print("  4/4 - Configuracao do caminho de dados...")
    print("-" * 60)
    print(f"""
  Os notebooks ja estao configurados para usar:
  DATA_RAW_PATH = "{VOLUME_PATH}"

  Se necessario, atualize o DATA_RAW_PATH no notebook
  01_bronze_ingestao.py para o caminho correto do seu Volume.
""")


def main():
    """Executa o deploy completo."""
    print("=" * 60)
    print("  DEPLOY - Data Pipeline + RAG com Databricks")
    print("=" * 60)

    # 1. Conecta ao Databricks
    w, user_email = get_workspace_client()

    # 2. Cria Volume
    create_volume(w)

    # 3. Upload dos dados
    upload_data_files(w)

    # 4. Importa notebooks
    upload_notebooks(w, user_email)

    # 5. Instrucoes finais
    update_data_path_info()

    print("=" * 60)
    print("  DEPLOY CONCLUIDO COM SUCESSO!")
    print("=" * 60)
    print(f"""
  Proximos passos:
  1. Acesse seu workspace Databricks
  2. Navegue ate os notebooks importados
  3. Execute na ordem: 01 -> 02 -> 03 -> 04 -> 05
  4. Teste o chat RAG no notebook 05!

  Workspace: {w.config.host}
""")


if __name__ == "__main__":
    main()
