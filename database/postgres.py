from sqlalchemy.engine import URL, create_engine



def create_pg_engine():

    """ Create the Postgres engine """

    # create connection to database 
    connection_url = URL.create(
        drivername = "postgresql+pg8000",
        username = "postgres",
        password = "",
        host = "localhost",
        port = "5432",
        database = "postgres"
    )

    engine = create_engine(connection_url)

    return engine
