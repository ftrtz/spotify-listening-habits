from prefect_sqlalchemy import SqlAlchemyConnector
import getpass


def create_pg_block(block_name: str = "spotify-postgresql"):
    # Check if block exists
    try:
        SqlAlchemyConnector.load(block_name)
        print(f"⚠️  A Prefect SqlAlchemyConnector block named '{block_name}' already exists.")
        overwrite = input("Do you want to overwrite it? (y/N): ").strip().lower()
        if overwrite != "y":
            print("Aborted. No changes made.")
            return
    except Exception:
        # Block does not exist, continue
        pass

    print("Enter your PostgreSQL connection details:")
    host = input("Host [localhost]: ").strip() or "localhost"
    port = input("Port [5432]: ").strip() or "5432"
    db = input("Database name: ").strip()
    user = input("User: ").strip()
    password = getpass.getpass("Password: ").strip()

    if not (user and password and db):
        print("⚠️  Skipped spotify-postgresql block (missing required fields)")
        return

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    SqlAlchemyConnector(connection_info=db_url).save(block_name, overwrite=True)
    print(f"✅ Created Prefect SqlAlchemyConnector block: {block_name}")
