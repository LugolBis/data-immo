DROP IF EXISTS TABLE Mutation;
DROP IF EXISTS TABLE Classe;

CREATE TABLE Mutation (
    id BIGINT primary key,
    idpar VARCHAR(15) not null,
    datemut date not null,
    idmutation number(10) not null,
    vefa boolean not null,
    typologie varchar(50),
    valeur_fonciere number(15) not null,
    btq varchar(1),
    voie varchar(70),
    novoie integer,
    codvoie varchar(4),
    commune varchar(70),
    typvoie varchar(5),
    codepostal varchar(5),
    vendu boolean,
    nature varchar(50)
);

CREATE TABLE Classe (
    id integer not null,
    name varchar(30),
    surface float,
    foreign key (id) references Mutation(id) on delete cascade
);