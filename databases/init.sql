SET threads = 4;
SET preserve_insertion_order = false;
SET temp_directory = 'data/DVF/';

DROP TABLE IF EXISTS Classes;
DROP TABLE IF EXISTS Mutations;

CREATE TABLE Mutations (
	idg INTEGER PRIMARY KEY,
	idpar VARCHAR NOT NULL,
	idmutation INTEGER NOT NULL,
	vefa BOOLEAN NOT NULL,
	typologie VARCHAR,
	datemut DATE NOT NULL,
	nature VARCHAR,
	btq VARCHAR,
	voie VARCHAR,
	novoie INTEGER,
	codvoie VARCHAR,
	commune VARCHAR,
	typvoie VARCHAR,
	codepostal VARCHAR,
	valeur_fonciere DECIMAL(15,2) NOT NULL,
	vendu BOOLEAN
);

CREATE TABLE Classes (
	idg INTEGER NOT NULL,
	libelle VARCHAR NOT NULL,
	surface DECIMAL(12,2) NOT NULL,
	FOREIGN KEY (idg) REFERENCES Mutations(idg)
);
