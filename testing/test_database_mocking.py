from sqlalchemy import create_engine

# Validate that yield here does what i think it does
def create_db_engine(db_info):
    creds = f"{db_info['username']}:{db_info['password']}"
    host = f"{db_info['host']}:{db_info['port']}"
    yield create_engine(f"postgresql://{creds}@{host}/{db_info['db_name']}")
    # "postgresql://postgres:postgres@host.docker.internal:5432/postgres" - docker
    # postgresql://postgres:postgres@localhost:5432/postgres - local


def get_data():
    db_info = {
        "username":"postgres",
        "password": "psotgres",
        "host": "localhost",
        "port":"5432",
        "db_name":"postgres"
    }
    with create_db_engine(db_info) as engine:
        engine.execute("SELECT * from postgres")

def test_create_engine_fixture(treatment_conn):
    for row in treatment_conn.execute("SELECT * FROM treatment"):
        print(row)

def func():
    with create_engine(...) as engine:
        engine.execute(...)
        

