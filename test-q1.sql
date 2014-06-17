D := SELECT t_testkey AS key, t_name AS name, t_value AS value 
	 FROM test; 
RES := ORDER D by name ASC;
DISPLAY RES USING ('|');
