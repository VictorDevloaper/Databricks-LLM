# ============================================================================
# Gerador de Dados Simulados - E-Commerce
# ============================================================================
# Gera dados fictícios de clientes, produtos e pedidos para
# simular um cenário real de e-commerce.
#
# Uso: Execute este script localmente ou no Databricks para gerar
#      os arquivos JSON na pasta data/raw/
# ============================================================================

import json
import random
import os
from datetime import datetime, timedelta

# Seed para reprodutibilidade
random.seed(42)

# ----------------------------------------------------------------------------
# CONFIGURAÇÕES
# ----------------------------------------------------------------------------
NUM_CUSTOMERS = 50
NUM_PRODUCTS = 30
NUM_ORDERS = 500
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw")

# ----------------------------------------------------------------------------
# DADOS BASE
# ----------------------------------------------------------------------------
FIRST_NAMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduardo", "Fernanda", "Gabriel",
    "Helena", "Igor", "Julia", "Karen", "Lucas", "Mariana", "Nelson",
    "Olivia", "Pedro", "Quezia", "Rafael", "Sofia", "Thiago", "Ursula",
    "Vinicius", "Wanda", "Xavier", "Yasmin", "Zeca", "Alice", "Bernardo",
    "Camila", "Diego", "Elisa", "Felipe", "Giovana", "Henrique", "Isabela",
    "João", "Karina", "Leonardo", "Monica", "Nicolas", "Patricia", "Ricardo",
    "Sandra", "Tiago", "Valentina", "Wagner", "Ximena", "Yuri", "Bianca", "Caio"
]

LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Lima", "Pereira", "Costa",
    "Ferreira", "Rodrigues", "Almeida", "Nascimento", "Araújo", "Melo",
    "Barbosa", "Ribeiro", "Martins", "Carvalho", "Gomes", "Rocha", "Dias"
]

CITIES_STATES = [
    ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"),
    ("Brasília", "DF"), ("Fortaleza", "CE"), ("Recife", "PE"),
    ("Manaus", "AM"), ("Goiânia", "GO"), ("Florianópolis", "SC"),
    ("Campinas", "SP"), ("Vitória", "ES"), ("Belém", "PA")
]

CATEGORIES = {
    "Eletrônicos": [
        ("Smartphone Galaxy S24", 3499.90), ("Notebook Lenovo IdeaPad", 4299.00),
        ("Tablet Samsung Tab S9", 2899.00), ("Fone Bluetooth JBL", 299.90),
        ("Smart TV 55\" LG", 3199.00), ("Smartwatch Apple Watch", 2499.00),
        ("Câmera GoPro Hero 12", 2799.00)
    ],
    "Moda": [
        ("Camiseta Polo Ralph Lauren", 249.90), ("Calça Jeans Levi's", 399.90),
        ("Tênis Nike Air Max", 699.90), ("Bolsa Michael Kors", 1299.00),
        ("Jaqueta North Face", 899.90), ("Óculos Ray-Ban Aviador", 599.90)
    ],
    "Casa & Decoração": [
        ("Sofá Retrátil 3 Lugares", 2499.00), ("Mesa de Jantar 6 Lugares", 1899.00),
        ("Luminária de Piso Design", 449.90), ("Jogo de Cama Queen 400 Fios", 349.90),
        ("Panela Elétrica de Arroz", 199.90), ("Aspirador Robô iRobot", 2199.00)
    ],
    "Livros": [
        ("Clean Code - Robert C. Martin", 89.90), ("O Poder do Hábito", 49.90),
        ("Sapiens - Yuval Harari", 59.90), ("Python para Data Science", 79.90),
        ("Pai Rico Pai Pobre", 54.90)
    ],
    "Esportes": [
        ("Bicicleta Mountain Bike Caloi", 2199.00), ("Esteira Elétrica Movement", 3499.00),
        ("Kit Halteres 20kg", 299.90), ("Barraca Camping 4 Pessoas", 449.90),
        ("Raquete de Tênis Wilson", 599.90), ("Bola de Futebol Adidas", 149.90)
    ]
}

DOMAINS = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "uol.com.br"]


# ----------------------------------------------------------------------------
# GERAÇÃO DE CLIENTES
# ----------------------------------------------------------------------------
def generate_customers(n: int) -> list:
    """Gera lista de clientes fictícios."""
    customers = []
    used_emails = set()

    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"

        # Gera email único
        email_base = f"{first.lower()}.{last.lower()}"
        email = f"{email_base}@{random.choice(DOMAINS)}"
        while email in used_emails:
            email = f"{email_base}{random.randint(1, 99)}@{random.choice(DOMAINS)}"
        used_emails.add(email)

        city, state = random.choice(CITIES_STATES)

        # Data de cadastro entre 2023-01-01 e 2025-12-31
        start_date = datetime(2023, 1, 1)
        random_days = random.randint(0, 1094)
        registration_date = (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "customer_name": name,
            "email": email,
            "city": city,
            "state": state,
            "registration_date": registration_date
        })

    return customers


# ----------------------------------------------------------------------------
# GERAÇÃO DE PRODUTOS
# ----------------------------------------------------------------------------
def generate_products(n: int) -> list:
    """Gera lista de produtos fictícios a partir das categorias."""
    products = []
    product_id = 1

    all_products = []
    for category, items in CATEGORIES.items():
        for name, price in items:
            all_products.append((name, category, price))

    # Seleciona N produtos (ou todos se N >= total)
    selected = all_products[:n] if n <= len(all_products) else all_products

    for name, category, price in selected:
        # Variação de preço ±10%
        price_variation = price * random.uniform(0.9, 1.1)

        products.append({
            "product_id": f"PROD-{product_id:04d}",
            "product_name": name,
            "category": category,
            "price": round(price_variation, 2),
            "stock_quantity": random.randint(10, 500),
            "created_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
        })
        product_id += 1

    return products


# ----------------------------------------------------------------------------
# GERAÇÃO DE PEDIDOS
# ----------------------------------------------------------------------------
def generate_orders(n: int, customers: list, products: list) -> list:
    """Gera lista de pedidos fictícios."""
    orders = []

    for i in range(1, n + 1):
        customer = random.choice(customers)

        # Cada pedido tem entre 1 e 5 itens
        num_items = random.randint(1, 5)
        order_products = random.sample(products, min(num_items, len(products)))

        items = []
        total_amount = 0.0

        for prod in order_products:
            quantity = random.randint(1, 3)
            item_total = prod["price"] * quantity
            total_amount += item_total

            items.append({
                "product_id": prod["product_id"],
                "product_name": prod["product_name"],
                "quantity": quantity,
                "unit_price": prod["price"],
                "item_total": round(item_total, 2)
            })

        # Data do pedido entre 2024-01-01 e 2025-12-31
        order_date = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 729)))

        # Status do pedido
        status = random.choices(
            ["completed", "processing", "shipped", "cancelled"],
            weights=[60, 15, 20, 5],
            k=1
        )[0]

        # Método de pagamento
        payment_method = random.choice(["credit_card", "debit_card", "pix", "boleto"])

        orders.append({
            "order_id": f"ORD-{i:05d}",
            "customer_id": customer["customer_id"],
            "customer_name": customer["customer_name"],
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": status,
            "payment_method": payment_method,
            "items": items,
            "total_amount": round(total_amount, 2)
        })

    return orders


# ----------------------------------------------------------------------------
# SALVAR DADOS
# ----------------------------------------------------------------------------
def save_json(data: list, filepath: str) -> None:
    """Salva dados em arquivo JSON com formatação."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Salvo: {filepath} ({len(data)} registros)")


# ----------------------------------------------------------------------------
# EXECUÇÃO PRINCIPAL
# ----------------------------------------------------------------------------
def main():
    """Gera todos os dados de exemplo."""
    print("=" * 60)
    print("  🏪 Gerando dados de E-Commerce simulados")
    print("=" * 60)

    # Gera dados
    customers = generate_customers(NUM_CUSTOMERS)
    products = generate_products(NUM_PRODUCTS)
    orders = generate_orders(NUM_ORDERS, customers, products)

    # Salva arquivos
    save_json(customers, os.path.join(OUTPUT_DIR, "customers.json"))
    save_json(products, os.path.join(OUTPUT_DIR, "products.json"))
    save_json(orders, os.path.join(OUTPUT_DIR, "orders.json"))

    print("\n" + "=" * 60)
    print("  📊 Resumo dos dados gerados:")
    print(f"  • Clientes: {len(customers)}")
    print(f"  • Produtos: {len(products)}")
    print(f"  • Pedidos:  {len(orders)}")
    total_revenue = sum(o["total_amount"] for o in orders)
    print(f"  • Receita Total: R$ {total_revenue:,.2f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
