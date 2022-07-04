create table dummy (
   id int not null auto_increment,
   data varchar(255) not null default 'test',
   createAt timestamp(6) not null default current_timestamp(6),
   updateAt timestamp(6) null default null,
   deleteAt timestamp(6) null default null,
   primary key id (id),
   key createAt (createAt),
   key updateAt (updateAt),
   key deleteAt (deleteAt)
) charset utf8mb4 collate utf8mb4_bin comment 'dummy data';
