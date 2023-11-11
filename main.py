from io import StringIO

import psycopg2


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    sql_create_table_acoounts = "CREATE TABLE accounts(" \
               "customer_id INT," \
               "first_name VARCHAR(60)," \
               "last_name VARCHAR(60)," \
               "address_1 VARCHAR(60)," \
               "address_2 VARCHAR(60)," \
               "city VARCHAR(60)," \
               "state VARCHAR(60)," \
               "zip_code INT," \
               "join_date DATE," \
               "PRIMARY KEY(customer_id)" \
               ")"

    sql_create_table_products = "CREATE TABLE products(" \
               "product_id INT," \
               "product_code INT," \
               "product_description VARCHAR(255)," \
               "PRIMARY KEY(product_id)" \
               ")"

    sql_create_table_transactions = "CREATE TABLE transactions(" \
               "transaction_id VARCHAR(60)," \
               "transaction_date DATE," \
               "product_id INT," \
               "product_code INT," \
               "product_description VARCHAR(255)," \
               "quantity INT," \
               "account_id INT," \
               "PRIMARY KEY(transaction_id)," \
               "CONSTRAINT fk_accounts" \
               "    FOREIGN KEY(account_id)" \
               "        REFERENCES accounts(customer_id)," \
               "CONSTRAINT fk_products" \
               "    FOREIGN KEY(product_id)" \
               "        REFERENCES products(product_id)" \
               ")"

    cursor = conn.cursor()

    cursor.execute(sql_create_table_acoounts)
    cursor.execute(sql_create_table_products)
    cursor.execute(sql_create_table_transactions)

    sql_create_index_accounts = "CREATE INDEX accounts_idx ON accounts(zip_code)"
    sql_create_index_products = "CREATE INDEX product_code_idx ON products(product_code)"
    sql_create_index_transactions = "CREATE INDEX transactions_idx ON transactions(transaction_date)"

    cursor.execute(sql_create_index_accounts)
    cursor.execute(sql_create_index_products)
    cursor.execute(sql_create_index_transactions)

    with open('/app/data/accounts.csv', 'r') as f:
        next(f)
        data = f.read().replace(", ", ",")
        cursor.copy_from(StringIO(data), 'accounts', sep=',')

    with open('/app/data/products.csv', 'r') as f:
        next(f)
        data = f.read().replace(", ", ",")
        cursor.copy_from(StringIO(data), 'products', sep=',')

    with open('/app/data/transactions.csv', 'r') as f:
        next(f)
        data = f.read().replace(", ", ",")
        cursor.copy_from(StringIO(data), 'transactions', sep=',')

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
