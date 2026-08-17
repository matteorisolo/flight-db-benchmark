-- 1. Creazione anagrafica compagnie
CREATE TABLE airlines (
    iata_code VARCHAR(2) PRIMARY KEY,
    airline_name VARCHAR(100) NOT NULL
);

-- 2. Creazione anagrafica aeroporti
CREATE TABLE airports (
    iata_code VARCHAR(3) PRIMARY KEY,
    airport_name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(2),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    -- Vincolo di dominio derivato dall'EDA
    CONSTRAINT check_iata_length CHECK (char_length(iata_code) = 3) 
);

-- 3. Creazione tabella voli (PARTIZIONATA PER MESE)
CREATE TABLE flights (
    flight_id BIGINT NOT NULL, -- Modificato da BIGSERIAL a BIGINT NOT NULL
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    airline_code VARCHAR(2) NOT NULL REFERENCES airlines(iata_code),
    flight_number INTEGER NOT NULL,
    tail_number VARCHAR(10),
    origin_airport VARCHAR(3) NOT NULL REFERENCES airports(iata_code),
    destination_airport VARCHAR(3) NOT NULL REFERENCES airports(iata_code),
    departure_delay SMALLINT,
    arrival_delay SMALLINT,
    distance SMALLINT,
    cancelled SMALLINT DEFAULT 0,
    cancellation_reason VARCHAR(1),
    
    -- Vincoli logici di integrità
    CONSTRAINT chk_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_day CHECK (day BETWEEN 1 AND 31), -- Aggiunto vincolo sul giorno
    CONSTRAINT chk_day_of_week CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_cancelled CHECK (cancelled IN (0, 1)),
    CONSTRAINT chk_cancellation_reason CHECK (cancellation_reason IN ('A', 'B', 'C', 'D')), -- Aggiunto vincolo sulle motivazioni DOT
    
    -- Chiave primaria composita necessaria per le tabelle partizionate
    PRIMARY KEY (flight_id, month)
) PARTITION BY LIST (month);

-- 3.1 Creazione delle Partizioni Fisiche
CREATE TABLE flights_m01 PARTITION OF flights FOR VALUES IN (1);
CREATE TABLE flights_m02 PARTITION OF flights FOR VALUES IN (2);
CREATE TABLE flights_m03 PARTITION OF flights FOR VALUES IN (3);
CREATE TABLE flights_m04 PARTITION OF flights FOR VALUES IN (4);
CREATE TABLE flights_m05 PARTITION OF flights FOR VALUES IN (5);
CREATE TABLE flights_m06 PARTITION OF flights FOR VALUES IN (6);
CREATE TABLE flights_m07 PARTITION OF flights FOR VALUES IN (7);
CREATE TABLE flights_m08 PARTITION OF flights FOR VALUES IN (8);
CREATE TABLE flights_m09 PARTITION OF flights FOR VALUES IN (9);
CREATE TABLE flights_m10 PARTITION OF flights FOR VALUES IN (10);
CREATE TABLE flights_m11 PARTITION OF flights FOR VALUES IN (11);
CREATE TABLE flights_m12 PARTITION OF flights FOR VALUES IN (12);

-- 4. OTTIMIZZAZIONE PRESTAZIONALE (Indici B-Tree)

-- Indice singolo per le aggregazioni statistiche sulle compagnie
CREATE INDEX idx_flights_airline ON flights(airline_code);

-- Indici Composti per accelerare al massimo le JOIN e il Pathfinding in RAM
CREATE INDEX idx_flights_route ON flights(origin_airport, destination_airport);
CREATE INDEX idx_flights_route_reverse ON flights(destination_airport, origin_airport);

-- Nuovi indici per garantire parità architetturale con Neo4j
CREATE INDEX idx_flights_date ON flights(year, month, day);
CREATE INDEX idx_flights_delay ON flights(departure_delay);