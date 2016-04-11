USE myemail;
DROP TABLE mail;
CREATE TABLE mail (
  messageid varchar,
  datesent timestamp,
  yearsent int,
  monthsent int,
  dayofmonthsent int,
  mailfrom list<varchar>,
  mailto list<varchar>,
  cc list<varchar>,
  bcc list<varchar>,
  subject varchar,
  PRIMARY KEY(yearsent, monthsent, dayofmonthsent, messageid)
);
CREATE INDEX mailto on mail(mailto);
