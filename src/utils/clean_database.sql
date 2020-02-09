DROP TABLE raw_LBC;
CREATE TABLE IF NOT EXISTS raw_LBC (
   id SERIAL PRIMARY KEY,
   date_scrap TIMESTAMP WITHOUT TIME ZONE,
   id_annonce TEXT,
   url_annonce TEXT,
   titre TEXT,
   prix TEXT,
   date_annonce TEXT,
   auteur TEXT,
   ville TEXT,
   code_postal TEXT,
   is_msg BOOLEAN,
   is_num BOOLEAN,
   categorie TEXT,
   critere JSON,
   nb_pict INT,
   descr TEXT,
   processed INT --should be boolean
);


DROP TABLE raw_PV;
CREATE TABLE IF NOT EXISTS raw_PV (
   id SERIAL PRIMARY KEY,
   date_scrap TIMESTAMP WITHOUT TIME ZONE,
   id_annonce TEXT,
   url_annonce TEXT,
   titre TEXT,
   prix TEXT,
   surface TEXT,
   date_annonce TEXT,
   auteur TEXT,
   ville TEXT,
   code_postal TEXT,
   nb_pieces int,
   nb_pict TEXT,
   agence TEXT,
   processed INT --should be boolean
);


DROP TABLE tmp_cleaned;
CREATE TABLE IF NOT EXISTS tmp_cleaned (
   id SERIAL PRIMARY KEY,
   id_annonce TEXT,
   prix TEXT,
   surface TEXT,
   prix_m2 TEXT,
   ville TEXT,
   code_postal TEXT,
   origine TEXT, -- Should be an int related to project
   dept TEXT
);


DROP TABLE processed_annonce;
CREATE TABLE IF NOT EXISTS processed_annonce (
   id SERIAL PRIMARY KEY,
   id_annonce TEXT,
   prix TEXT,
   surface TEXT,
   prix_m2 TEXT,
   ville TEXT,
   code_postal TEXT,
   origine TEXT, -- Should be an int related to project
   dept TEXT,
   alert INT -- Should be an int related to project
);


DROP TABLE users;
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    pseudo TEXT UNIQUE,
    password TEXT,
    date_naissance DATE,
    date_inscription DATE
);
INSERT INTO users ("pseudo", "password", "date_naissance", "date_inscription")
VALUES ('Thib', 'password', '1995-11-10', '2020-02-09');


DROP TABLE slack;
CREATE TABLE IF NOT EXISTS slack (
    id SERIAL PRIMARY KEY,
    id_user BIGINT,
    slack_token TEXT,
    channel_name TEXT,
    alert_type INT
);
INSERT INTO slack ("id_user", "slack_token", "channel_name", "alert_type")
VALUES (1, 'mon_token_blablab_azeazeaze', 'immo_scrap', 0);


DROP TABLE alerts;
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    id_user BIGINT,
    emoji TEXT,
    channel_name TEXT,
    date_alerte TIMESTAMP WITHOUT TIME ZONE,
    alerte TEXT
);
INSERT INTO alerts ("id_user", "emoji", "channel_name", "date_alerte", "alerte")
VALUES (1, ':female-firefighter:', 'immo_scrap', '2020-02-09 18:40:19', 'contenu de alerte');


DROP TABLE requests;
CREATE TABLE IF NOT EXISTS requests (
    id SERIAL PRIMARY KEY,
    id_user BIGINT,
    date_creation_requete DATE,
    is_active BOOLEAN,
    ville TEXT, -- could multiple cities ??
    surface_min INT,
    surface_max INT,
    prix_min INT,
    prix_max INT,
    ascenceur BOOLEAN,
    jardin BOOLEAN,
    etage INT,
    automatic_reply BOOLEAN
);


DROP TABLE time_information;
CREATE TABLE IF NOT EXISTS time_information (
    id SERIAL PRIMARY KEY,
    last_analyse TIMESTAMP WITHOUT TIME ZONE,
    last_alert TIMESTAMP WITHOUT TIME ZONE
);
INSERT INTO time_information ("last_analyse", "last_alert")
VALUES ('2020-01-19 19:19:19','2020-01-19 19:19:19');



-- INSERT INTO raw_scrapped ("date_scrap", "origine", "id_annonce", "annonce")
-- VALUES ('2020-01-19', 3, 34567, '{"content":"mon_annonce"}');
