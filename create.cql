USE myemail;
DROP TABLE mail;
CREATE TABLE mail (
  messageid varchar,
  datesent timestamp,
  yearsent int,
  monthsent int,
  dayofmonthsent int,
  mailfrom varchar,
  mailto varchar,
  subject varchar,
  PRIMARY KEY(yearsent, monthsent, dayofmonthsent)
);
