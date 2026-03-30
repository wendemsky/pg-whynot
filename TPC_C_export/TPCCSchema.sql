CREATE TABLE items (
	i_id INTEGER PRIMARY KEY,
	i_im_id CHAR(8) UNIQUE NOT NULL,
	i_name VARCHAR(64)  NOT NULL,
	i_price NUMERIC NOT NULL CHECK(i_price >0));

CREATE TABLE warehouses (
 	w_id INTEGER PRIMARY KEY,
 	w_name VARCHAR(16) NOT NULL,
 	w_street VARCHAR(32) NOT NULL,
 	w_city VARCHAR(32) NOT NULL,
 	w_country VARCHAR(16) NOT NULL);

CREATE TABLE stocks (
	w_id INTEGER REFERENCES warehouses(w_id),
	i_id INTEGER REFERENCES items(i_id),
	s_qty SMALLINT CHECK(s_qty > 0),
	PRIMARY KEY (w_id, i_id));
