CREATE TABLE IF NOT EXISTS bufr_file_index(
   bufr_file_index_id SERIAL,
   path TEXT NOT NULL,
   created_at TIMESTAMP NOT NULL,
   parsing_nb SMALLINT NOT NULL DEFAULT 0,
   station_id VARCHAR(50)  NOT NULL,
   PRIMARY KEY(bufr_file_index_id)
);

