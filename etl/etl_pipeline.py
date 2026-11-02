import os
import pandas as pd
from sqlalchemy import create_engine, text
from neo4j import GraphDatabase

# --- CONFIGURAZIONE PERCORSI E CONNESSIONI ---
DATA_DIR = "../data"
FLIGHTS_CSV = os.path.join(DATA_DIR, "flights.csv")
AIRPORTS_CSV = os.path.join(DATA_DIR, "airports.csv")
AIRLINES_CSV = os.path.join(DATA_DIR, "airlines.csv")

# Connessioni configurate tramite Variabili d'Ambiente (con fallback di default per test locali)
PG_URI = os.getenv("PG_URI", "postgresql://postgres:database@localhost:5432/flights_db")
pg_engine = create_engine(PG_URI)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")
NEO4J_AUTH = ("neo4j", NEO4J_PASSWORD)

def run_etl():
    print("=== INIZIO PIPELINE ETL ===")
    
    # 0. IDEMPOTENZA E CONSISTENZA INIZIALE
    # Svuotiamo il database PostgreSQL prima di partire. 
    # Neo4j è gestito in modo sicuro tramite i MERGE e i vincoli IS UNIQUE.
    print("Pulizia iniziale del database relazionale (TRUNCATE) per garantire l'idempotenza...")
    with pg_engine.connect() as conn:
        conn.execute(text("TRUNCATE flights, airports, airlines CASCADE;"))
        conn.commit()

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    # ==========================================
    # 1. CARICAMENTO DELLE ANAGRAFICHE (SQL & Neo4j)
    # ==========================================
    print("Caricamento anagrafiche in corso...")
    
    if os.path.exists(AIRLINES_CSV):
        df_airlines = pd.read_csv(AIRLINES_CSV)
        df_airlines.columns = ['iata_code', 'airline_name']
        # Load SQL
        df_airlines.to_sql("airlines", pg_engine, if_exists="append", index=False)
        # Load Neo4j
        load_airlines_neo4j(driver, df_airlines)
        print(f"Caricate {len(df_airlines)} compagnie su DB Relazionale e Grafo.")
        
    if os.path.exists(AIRPORTS_CSV):
        df_airports = pd.read_csv(AIRPORTS_CSV)
        df_airports = df_airports[['IATA_CODE', 'AIRPORT', 'CITY', 'STATE', 'LATITUDE', 'LONGITUDE']].dropna(subset=['IATA_CODE'])
        df_airports.columns = ['iata_code', 'airport_name', 'city', 'state', 'latitude', 'longitude']
        # Load SQL
        df_airports.to_sql("airports", pg_engine, if_exists="append", index=False)
        # Load Neo4j
        load_airports_neo4j(driver, df_airports)
        print(f"Caricati {len(df_airports)} aeroporti su DB Relazionale e Grafo.")

    # ==========================================
    # 2. ESTRAZIONE E PULIZIA DEI VOLI (Chunking)
    # ==========================================
    print("Elaborazione dei voli in corso...")
    
    if os.path.exists(FLIGHTS_CSV):
        chunk_size = 25000  # Ridotto a 25k per evitare pressione sulla Garbage Collection di Neo4j
        
        for chunk_idx, chunk in enumerate(pd.read_csv(FLIGHTS_CSV, chunksize=chunk_size, low_memory=False)):
            
            # Filtro Data Quality (Lunghezza == 3)
            chunk = chunk[chunk['ORIGIN_AIRPORT'].astype(str).str.len() == 3]
            chunk = chunk[chunk['DESTINATION_AIRPORT'].astype(str).str.len() == 3]
            
            # --- NOTA ARCHITETTURALE SULL'INDICE (flight_id) ---
            # L'utilizzo di `chunk.index` è sicuro perché `pd.read_csv` con `chunksize`
            # mantiene un contatore continuo dell'indice tra un chunk e l'altro.
            # In architetture con lettura parallela, andrebbe sostituito con un UUID.
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
                'departure_delay': chunk['DEPARTURE_DELAY'].fillna(0).astype(int),
                'arrival_delay': chunk['ARRIVAL_DELAY'].fillna(0).astype(int),
                'distance': chunk['DISTANCE'].fillna(0).astype(int),
                'cancelled': chunk['CANCELLED'],
                'cancellation_reason': chunk['CANCELLATION_REASON']
            })
            
            # --- LOAD SU POSTGRESQL ---
            # Rimosso method='multi' per prevenire crolli di memoria in SQLAlchemy con grandi chunk
            df_cleaned.to_sql("flights", pg_engine, if_exists="append", index=False)
            
            # --- LOAD SU NEO4J ---
            load_flights_to_neo4j(driver, df_cleaned)
            
            print(f"Elaborato e caricato chunk {chunk_idx + 1} (~{len(df_cleaned)} record validi).")

        # ---> INSERISCI QUI L'ARRICCHIMENTO <---
        # Lo lanciamo solo se abbiamo effettivamente trovato e caricato i voli
        print("Creazione del Modello Topologico in corso...")
        enrich_graph_topology(driver)

        print("=== PIPELINE ETL COMPLETATA CON SUCCESSO ===")
    else:
        print(f"Errore: File {FLIGHTS_CSV} non trovato.")

    driver.close()
    pg_engine.dispose()

# ==========================================
# FUNZIONI CYPHER BATCH
# ==========================================

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
    
    // Usiamo MATCH invece di MERGE perché le anagrafiche sono già caricate (Estrema velocità)
    MATCH (origin:Airport {iata_code: row.origin_airport})
    MATCH (dest:Airport {iata_code: row.destination_airport})
    MATCH (airline:Airline {iata_code: row.airline_code})
    
    // Creazione Nodo Volo con il flight_id (Risolve il problema del Constraint)
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
    print("Arricchimento completato: Grafo Topologico pronto per il pathfinding!")

if __name__ == "__main__":
    run_etl()