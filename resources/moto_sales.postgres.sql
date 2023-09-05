do $$
  begin
		create type gps as (
			latitude  numeric(16),
			longitude numeric(16),
			elevation numeric(16)
		);
	exception when others then
    raise notice 'type gps already exists';
end;
$$;

drop schema if exists moto_sales cascade;
create schema moto_sales;

create table if not exists moto_sales.addresses
(
    address_number numeric(16) not null constraint moto_addresses_pk primary key,
    street_name varchar(50),
    street_number numeric(16),
    postal_code varchar(6),
    city varchar(50),
    coordinates gps,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.customers
(
    customer_number numeric(16) not null constraint moto_customers_p primary key,
    national_person_id varchar(20),
    first_name varchar(50),
    last_name varchar(50),
    birthdate date,
    gender char(8),
    customer_invoice_address_id numeric(16) constraint moto_cust_inv_adr_fk references addresses,
    customer_ship_to_address_id numeric(16) constraint moto_cust_shto_adr_fk references addresses,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.invoices
(
    invoice_number numeric(16) not null constraint moto_invoices_pk primary key,
    invoice_date date,
    invoice_customer_id numeric(16),
    amount numeric(14, 2),
    discount integer,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.moto_parts
(
    part_id numeric(16) not null constraint parts_pk primary key,
    part_number varchar(50) not null,
    part_language_code varchar(10),
    update_user varchar(30),
    update_timestamp timestamp not null, constraint moto_parts_uk unique (part_number, part_language_code)
);

create table if not exists moto_sales.moto_products
(
    product_id numeric(16) not null constraint moto_products_pk primary key,
    replacement_product_id numeric(16) constraint moto_prod_repl_prod_fk references moto_products,
    product_cc numeric(16),
    product_et_code char(10),
    product_part_code varchar(50),
    product_intro_date date,
    product_name varchar(100),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create unique index if not exists moto_products_uk_idx
on moto_sales.moto_products (product_cc, product_et_code, product_part_code);

create table if not exists moto_sales.invoice_lines
(
    invoice_line_number numeric(16) not null,
    invoice_number numeric(16) not null constraint moto_inv_lin_inv_fk references invoices,
    product_id numeric(16) constraint moto_invln_prod_fk references moto_products,
    part_id numeric(16) constraint invoice_lines_parts_fk references moto_parts,
    amount numeric(14, 2),
    quantity numeric(12),
    unit_price numeric(14, 2),
    update_user varchar(30),
    update_timestamp timestamp not null, constraint moto_invoice_lines_pk primary key (invoice_number, invoice_line_number)
);

create table if not exists moto_sales.product_feature_class
(
    product_feature_class_id numeric(16) not null constraint product_feature_class_pk primary key,
    product_feature_class_code varchar(20) constraint product_feature_class_uk unique,
    product_feature_class_desc varchar(50),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.product_feature_cat
(
    product_feature_category_id integer not null constraint product_feature_cat_pk primary key,
    product_feature_category_code varchar(50),
    prod_feat_cat_language_code varchar(10),
    prod_feat_cat_description varchar(60),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.product_features
(
    product_feature_id integer not null constraint moto_product_features_pk primary key,
    product_feature_cat_id integer constraint product_features_fk references product_feature_cat,
    product_feature_code varchar(20),
    product_feature_language_code varchar(10),
    product_feature_description varchar(60),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.product_feat_class_rel
(
    product_feature_id integer,
    product_id numeric(16) constraint product_feat_class_rel_prod_fk references moto_products,
    product_feature_class_id numeric(16),
    update_user varchar(30),
    update_timestamp timestamp not null, constraint product_feat_class_rel_uk unique (product_feature_id, product_id, product_feature_class_id)
);

create table if not exists moto_sales.codes_to_language
(
    code varchar(50) not null,
    language_code varchar(10) not null,
    description varchar(512),
    update_user varchar(30),
    update_timestamp timestamp not null, constraint codes_to_language_pk primary key (code, language_code)
);

create table if not exists moto_sales.payments
(
    transaction_id varchar(32) not null constraint payments_pk primary key,
    date_time timestamp not null,
    invoice_number numeric(16) not null,
    amount numeric(14, 2) not null,
    customer_number numeric(16) not null,
    update_timestamp timestamp default clock_timestamp() not null
);

create table if not exists moto_sales.cust_addresses
(
    customer_number numeric(16),
    address_number numeric(16),
    address_type varchar(3),
    update_user varchar(30),
    update_timestamp timestamp not null, constraint priv_cust_addresses_rel_uk unique (customer_number, address_number, address_type)
);

create table if not exists moto_sales.product_sensors
(
    product_number numeric(16) constraint product_sensors_products_fk references moto_products,
    vehicle_number varchar(30) not null,
    sensor varchar(20),
    sensor_value numeric(14, 2),
    unit_of_measurement varchar(30)
);

create table if not exists moto_sales.jrn_invoice_lines
(
    operation char,
    trans_timestamp timestamp,
    invoice_line_number numeric(16) not null,
    invoice_number numeric(16) not null,
    product_id numeric(16),
    part_id numeric(16),
    amount numeric(14, 2),
    quantity numeric(12),
    unit_price numeric(14, 2),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_product_feature_class
(
    operation char,
    trans_timestamp timestamp,
    product_feature_class_id numeric(16) not null,
    product_feature_class_code varchar(20),
    product_feature_class_desc varchar(50),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_codes_to_language
(
    operation char,
    trans_timestamp timestamp,
    code varchar(50) not null,
    language_code varchar(10) not null,
    description varchar(512),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_addresses
(
    operation char,
    trans_timestamp timestamp,
    address_number numeric(16) not null,
    street_name varchar(50),
    street_number numeric(16),
    postal_code varchar(6),
    city varchar(50),
    coordinates gps,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_customers
(
    operation char,
    trans_timestamp timestamp,
    customer_number numeric(16) not null,
    national_person_id varchar(20),
    first_name varchar(50),
    last_name varchar(50),
    birthdate date,
    gender char(8),
    customer_invoice_address_id numeric(16),
    customer_ship_to_address_id numeric(16),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_moto_parts
(
    operation char,
    trans_timestamp timestamp,
    part_id numeric(16) not null,
    part_number varchar(50) not null,
    part_language_code varchar(10),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_moto_products
(
    operation char,
    trans_timestamp timestamp,
    product_id numeric(16) not null,
    replacement_product_id numeric(16),
    product_cc numeric(16),
    product_et_code char(10),
    product_part_code varchar(50),
    product_intro_date date,
    product_name varchar(100),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_product_feature_cat
(
    operation char,
    trans_timestamp timestamp,
    product_feature_category_id integer not null,
    product_feature_category_code varchar(50),
    prod_feat_cat_language_code varchar(10),
    prod_feat_cat_description varchar(60),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_product_features
(
    operation char,
    trans_timestamp timestamp,
    product_feature_id integer not null,
    product_feature_cat_id integer,
    product_feature_code varchar(20),
    product_feature_language_code varchar(10),
    product_feature_description varchar(60),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_product_feat_class_rel
(
    operation char,
    trans_timestamp timestamp,
    product_feature_id integer,
    product_id numeric(16),
    product_feature_class_id numeric(16),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_invoices
(
    operation char,
    trans_timestamp timestamp,
    invoice_number numeric(16) not null,
    invoice_date date,
    invoice_customer_id numeric(16),
    amount numeric(14, 2),
    discount integer,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_payments
(
    operation char,
    trans_timestamp timestamp,
    transaction_id varchar(32) not null,
    date_time timestamp not null,
    invoice_number numeric(16) not null,
    amount numeric(14, 2) not null,
    customer_number numeric(16) not null,
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_cust_addresses
(
    operation char,
    trans_timestamp timestamp,
    customer_number numeric(16),
    address_number numeric(16),
    address_type varchar(3),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_sales.jrn_product_sensors
(
    product_number numeric(16),
    vehicle_number varchar(30) not null,
    sensor varchar(20),
    sensor_value numeric(14, 2),
    unit_of_measurement varchar(30)
);
