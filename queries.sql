create database dados_brutos;

create table alunos(
	id serial primary key,
	codigo_municipio text,
	nome_municipio text,
	ano text,
	variavel text,
	ensino_rede text,
	ensino_tipo text,
	valor text
);

create table saude(
	id serial primary key,
	geocodigo text,
	municipio text,
	variavel text,
	ano text,
	quantidade_profissionais_saude text,
	und text,
	tags text,
	fonte text
);

create table dados_refinados (
	id serial primary key,
	codigo_municipio text,
	nome_municipio_aluno text,
	ano text,
	ensino_rede text,
	ensino_tipo text,
	tipo_profissional_saude text,
	ano_profissional_saude text,
	quantidade_profissionais_saude text
);