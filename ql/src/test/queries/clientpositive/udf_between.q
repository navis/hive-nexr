DESCRIBE FUNCTION btw;
DESCRIBE FUNCTION EXTENDED btw;

SELECT btw(false,  50, 100, 200), btw(false, 100, 100, 200), btw(false, 150, 100, 200),
       btw(false, 200, 100, 200), btw(false, 250, 100, 200),
       btw(true,  50, 100, 200), btw(true, 100, 100, 200), btw(true, 150, 100, 200),
       btw(true, 200, 100, 200), btw(true, 250, 100, 200) FROM src LIMIT 1;

SELECT btw(false, 'b', 'a', 'c') FROM src LIMIT 1;
SELECT btw(false, 2, 2, '3') FROM src LIMIT 1;

SELECT * FROM src where 100 between (50 + 30) AND 200 LIMIT 1;
SELECT * FROM src where 100 not between -50 AND 200 LIMIT 1;
