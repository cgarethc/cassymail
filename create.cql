USE myemail;
DROP TABLE mail;
CREATE TABLE mail (
  messageid varchar PRIMARY KEY,
  datesent timestamp,
  mailfrom varchar,
  mailto varchar,
  subject varchar
);
