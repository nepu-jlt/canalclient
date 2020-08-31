create table ext_word(
word_id varchar(36),
word_text varchar(200) DEFAULT NULL,
create_time datetime DEFAULT NULL,
creator varchar(36) DEFAULT NULL,
update_time datetime DEFAULT NULL,
updator varchar(36) DEFAULT NULL,
PRIMARY KEY (`word_id`)
)DEFAULT CHARSET=utf8;