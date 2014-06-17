D := SELECT t_testkey AS key, t_name AS name FROM test;
RES := ORDER D BY t_value ASC;
DISPLAY D USING ('|');
