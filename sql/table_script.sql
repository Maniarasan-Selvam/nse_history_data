
CREATE TABLE public.ind_stock_dly_price (
	stock_date date NOT NULL,
	stock_id VARCHAR(10) NOT NULL,
	open_price float8 NULL,
	high_price float8 NULL,
	low_price float8 NULL,
	close_price float8 NULL,
	stock_volume int8 NULL,
	create_date TIMESTAMP NOT NULL DEFAULT NOW(),
	update_date TIMESTAMP NULL,
	PRIMARY KEY(stock_date, stock_id)
);


CREATE TABLE public.dim_stock_list (
	stock_id varchar(50) NOT NULL,
	stock_name varchar(200) NULL,
	date_of_listing date NULL,
	series varchar(10) NULL,
	face_value int8 NULL,
	create_date TIMESTAMP NOT NULL DEFAULT NOW(),
	update_date TIMESTAMP NULL,
	PRIMARY KEY(stock_id)
);

CREATE TABLE public.dim_sector_stock_raw(
	sector_id varchar(50) NOT NULL,
	sub_sector varchar(200),
	stock_name varchar(200),
	create_date TIMESTAMP NOT NULL DEFAULT NOW(),
	update_date TIMESTAMP NULL
);