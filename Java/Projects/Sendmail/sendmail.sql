CREATE OR REPLACE PROCEDURE sendmail
(
  toIn varchar2,
  ccIn varchar2,
  bccIn varchar2,
  hostIn varchar2,
  subjIn varchar2,
  msgTextIn varchar2,
  debugIn varchar2
) AS LANGUAGE JAVA
NAME 'sendmail.mailme(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)';
/
