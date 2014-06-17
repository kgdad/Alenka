C := LOAD 'test.tbl' USING ('|') AS (t_testkey{1}:int, t_name{2}:varchar(25), t_value{3}:int);
STORE C INTO 'test' BINARY;
