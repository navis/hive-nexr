DESCRIBE FUNCTION =;
DESCRIBE FUNCTION EXTENDED =;

DESCRIBE FUNCTION ==;
DESCRIBE FUNCTION EXTENDED ==;

SELECT true=false, false=true, false=false, true=true FROM src LIMIT 1;

DESCRIBE FUNCTION <=>;
DESCRIBE FUNCTION EXTENDED <=>;

SELECT NULL<=>true, true<=>NULL, NULL<=>NULL FROM src LIMIT 1;
