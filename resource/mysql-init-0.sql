create table dummy (
   id int not null auto_increment,
   data varchar(255) not null default 'test',
   createAt timestamp not null default current_timestamp,
   updateAt timestamp null default null,
   deleteAt timestamp null default null,
   primary key id (id),
   key createAt (createAt),
   key updateAt (updateAt),
   key deleteAt (deleteAt)
) charset utf8mb4 collate utf8mb4_bin comment 'dummy data';

create table cache (
   agent char(20) not null,
   position bigint not null default 0,
   primary key agent (agent)
) charset utf8mb4 collate utf8mb4_bin comment 'cache';
