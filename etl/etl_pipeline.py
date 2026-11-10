import os
import pandas as pd
from sqlalchemy import create_engine, text
from neo4j import GraphDatabase

# Configurazione percorsi e connessioni
DATA_DIR = "../data"
FLIGHTS_CSV = os.path.join(DATA_DIR, "flights.csv")
AIRPORTS_CSV = os.path.join(DATA_DIR, "airports.csv")
AIRLINES_CSV = os.path.join(DATA_DIR, "airlines.csv")

PG_URI = os.getenv("PG_URI", "postgresql://postgres:database@localhost:5432/flights_db")
pg_engine = create_engine(PG_URI)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
NEO4J_AUTH = ("neo4j", NEO4J_PASSWORD)

# Funzione principale per la pipeline ETL
def run_etl():
    print("=== INIZIO PIPELINE ETL ===")
    
    # Pulizia iniziale del database relazionale per la riproducibilità dei test
    print("Pulizia iniziale del database relazionale...")
    with pg_engine.connect() as conn:
        conn.execute(text("TRUNCATE flights, airports, airlines CASCADE;"))
        conn.commit()

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    # Caricamento delle anagrafiche
    print("Caricamento anagrafiche in corso...")
    
    if os.path.exists(AIRLINES_CSV):
        df_airlines = pd.read_csv(AIRLINES_CSV)
        df_airlines.columns = ['iata_code', 'airline_name']
        df_airlines.to_sql("airlines", pg_engine, if_exists="append", index=False)
        load_airlines_neo4j(driver, df_airlines)
        print(f"Caricate {len(df_airlines)} compagnie su DB Relazionale e Grafo.")
        
    if os.path.exists(AIRPORTS_CSV):
        df_airports = pd.read_csv(AIRPORTS_CSV)
        df_airports = df_airports[['IATA_CODE', 'AIRPORT', 'CITY', 'STATE', 'LATITUDE', 'LONGITUDE']].dropna(subset=['IATA_CODE'])
        df_airports.columns = ['iata_code', 'airport_name', 'city', 'state', 'latitude', 'longitude']
        df_airports.to_sql("airports", pg_engine, if_exists="append", index=False)
        load_airports_neo4j(driver, df_airports)
        print(f"Caricati {len(df_airports)} aeroporti su DB Relazionale e Grafo.")

    # Caricamento dei voli in chunk e pulizia dei dati
    print("Elaborazione dei voli in corso...")
    
    if os.path.exists(FLIGHTS_CSV):
        chunk_size = 25000
        
        for chunk_idx, chunk in enumerate(pd.read_csv(FLIGHTS_CSV, chunksize=chunk_size, low_memory=False)):
            chunk = chunk[chunk['ORIGIN_AIRPORT'].astype(str).str.len() == 3]
            chunk = chunk[chunk['DESTINATION_AIRPORT'].astype(str).str.len() == 3]
            
            df_cleaned = pd.DataFrame({
                'flight_id': chunk.index, 
                'year': chunk['YEAR'],
                'month': chunk['MONTH'],
                'day': chunk['DAY'],
                'day_of_week': chunk['DAY_OF_WEEK'],
                'airline_code': chunk['AIRLINE'],
                'flight_number': chunk['FLIGHT_NUMBER'],
                'tail_number': chunk['TAIL_NUMBER'],
                'origin_airport': chunk['ORIGIN_AIRPORT'],
                'destination_airport': chunk['DESTINATION_AIRPORT'],
                'departure_delay': chunk['DEPARTURE_DELAY'].astype('Int64').replace({pd.NA: None}),
                'arrival_delay': chunk['ARRIVAL_DELAY'].astype('Int64').replace({pd.NA: None}),
                'distance': chunk['DISTANCE'].fillna(0).astype(int),
                'cancelled': chunk['CANCELLED'],
                'cancellation_reason': chunk['CANCELLATION_REASON']
            })
            
            df_cleaned.to_sql("flights", pg_engine, if_exists="append", index=False)
            load_flights_to_neo4j(driver, df_cleaned)
            
            print(f"Elaborato e caricato chunk {chunk_idx + 1} (~{len(df_cleaned)} record validi).")

        # Creazione del Modello Topologico e della Vista SQL
        print("Creazione del Modello Topologico e della Vista SQL in corso...")
        enrich_graph_topology(driver)
        
        enrich_sql_topology(pg_engine)

        # Controllo finale di coerenza
        print("\nVerifica di coerenza strutturale...")
        with pg_engine.connect() as conn:
            n_pg = conn.execute(text("SELECT COUNT(*) FROM routes;")).scalar()
        with driver.session() as session:
            n_neo = session.run("MATCH ()-[r:ROUTE]->() RETURN COUNT(r) AS n").single()["n"]

        print(f"Rotte aggregate in Postgres (vista routes): {n_pg}")
        print(f"Rotte aggregate in Neo4j (arco :ROUTE)    : {n_neo}")
        
        if n_pg == n_neo:
            print("OK: parità informativa confermata")
        else:
            print("ATTENZIONE: differenza infirmativa")

        print("=== PIPELINE ETL COMPLETATA CON SUCCESSO ===")
    else:
        print(f"Errore: File {FLIGHTS_CSV} non trovato.")

    driver.close()
    pg_engine.dispose()

# Funzioni di supporto per il caricamento dei dati in Neo4j
def load_airlines_neo4j(driver, df):
    query = """
    UNWIND $batch AS row
    MERGE (a:Airline {iata_code: row.iata_code})
    SET a.airline_name = row.airline_name
    """
    with driver.session() as session:
        session.run(query, batch=df.to_dict(orient="records"))

def load_airports_neo4j(driver, df):
    query = """
    UNWIND $batch AS row
    MERGE (a:Airport {iata_code: row.iata_code})
    SET a.airport_name = row.airport_name, 
        a.city = row.city, 
        a.state = row.state, 
        a.latitude = row.latitude, 
        a.longitude = row.longitude
    """
    with driver.session() as session:
        session.run(query, batch=df.to_dict(orient="records"))

def load_flights_to_neo4j(driver, df):
    query = """
    UNWIND $batch AS row
    MATCH (origin:Airport {iata_code: row.origin_airport})
    MATCH (dest:Airport {iata_code: row.destination_airport})
    MATCH (airline:Airline {iata_code: row.airline_code})
    
    CREATE (f:Flight {
        flight_id: row.flight_id,
        year: row.year,
        month: row.month,
        day: row.day,
        flight_number: row.flight_number,
        tail_number: row.tail_number,
        departure_delay: row.departure_delay,
        distance: row.distance
    })
    
    CREATE (f)-[:ORIGIN]->(origin)
    CREATE (f)-[:DESTINATION]->(dest)
    CREATE (f)-[:OPERATED_BY]->(airline)
    """
    with driver.session() as session:
        session.run(query, batch=df.to_dict(orient="records"))

# Funzioni per l'arricchimento topologico e la creazione della vista SQL
def enrich_graph_topology(driver):
    print("Avvio Arricchimento Topologico: Creazione delle relazioni dirette [:ROUTE]...")
    query = """
    MATCH (origin:Airport)<-[:ORIGIN]-(f:Flight)-[:DESTINATION]->(dest:Airport)
    WITH origin, dest, COUNT(f) AS num_voli, AVG(f.departure_delay) AS ritardo_medio
    MERGE (origin)-[r:ROUTE]->(dest)
    SET r.num_voli = num_voli, r.ritardo_medio = ritardo_medio
    """
    with driver.session() as session:
        session.run(query)
    print("Arricchimento completato")

def enrich_sql_topology(pg_engine):
    print("Avvio Arricchimento SQL: Creazione della vista materializzata 'routes'...")
    with pg_engine.begin() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS routes;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW routes AS
            SELECT origin_airport,
                   destination_airport,
                   COUNT(*) AS num_voli,
                   ROUND(AVG(departure_delay), 2) AS ritardo_medio
            FROM flights
            GROUP BY origin_airport, destination_airport;
        """))
        conn.execute(text("CREATE INDEX idx_routes_origin ON routes(origin_airport);"))
        conn.execute(text("ANALYZE routes;"))
    print("Arricchimento SQL completato")

if __name__ == "__main__":
    run_etl()