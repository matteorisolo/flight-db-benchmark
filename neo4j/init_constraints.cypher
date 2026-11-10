// 1. Vincolo di unicità per le Compagnie Aeree (funge da Primary Key)
CREATE CONSTRAINT airline_iata IF NOT EXISTS FOR (a:Airline) REQUIRE a.iata_code IS UNIQUE;

// 2. Vincolo di unicità per gli Aeroporti (funge da Primary Key)
CREATE CONSTRAINT airport_iata IF NOT EXISTS FOR (a:Airport) REQUIRE a.iata_code IS UNIQUE;

// 3. Vincolo di unicità per i Voli (evita duplicati in ETL)
CREATE CONSTRAINT flight_id IF NOT EXISTS FOR (f:Flight) REQUIRE f.flight_id IS UNIQUE;

// 4. Indice composito temporale (per query storiche)
CREATE INDEX flight_date IF NOT EXISTS FOR (f:Flight) ON (f.year, f.month, f.day);

// 5. Indice per velocizzare le ricerche e i calcoli sui minuti di ritardo
CREATE INDEX flight_delay IF NOT EXISTS FOR (f:Flight) ON (f.departure_delay);