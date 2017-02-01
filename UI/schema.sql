drop table if exists tasks;
create table tasks (
	id integer primary key autoincrement,
	priority text not null,
	title text not null, 
	date text not null
);	