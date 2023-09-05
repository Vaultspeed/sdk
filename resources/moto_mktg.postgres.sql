drop schema if exists moto_mktg cascade;
create schema moto_mktg;

create table if not exists moto_mktg.addresses
(
    address_number numeric(16) not null constraint addresses_pk primary key,
    street_name varchar(50),
    street_number numeric(16),
    postal_code varchar(6),
    city varchar(50),
    province varchar(50),
    update_user varchar(30),
    update_timestamp timestamp not null,
    constraint addresses_uk unique (street_name, street_number, postal_code, city)
);

create table if not exists moto_mktg.party
(
    party_number numeric(16) not null constraint party_pk primary key,
    parent_party_number numeric(16) constraint party_parent_fk references moto_mktg.party,
    name varchar(50),
    birthdate date,
    gender char(8),
    party_type_code char(2),
    comments text,
    address_number numeric(16) constraint party_addresses_fk
    references moto_mktg.addresses constraint party_addresses_address_number_fk
    references moto_sales.addresses,
    update_user varchar(30),
    update_timestamp timestamp not null,
    constraint party_uk unique (name, birthdate, gender, party_type_code)
);

create table if not exists moto_mktg.motorcycles
(
    motorcycle_id numeric(16) not null constraint motorcycles_pk primary key,
    motorcycle_cc numeric(16),
    motorcycle_et_code char(10),
    motorcycle_part_code varchar(50),
    motorcycle_name varchar(100),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create unique index if not exists motorcycles_uk_idx
on moto_mktg.motorcycles (motorcycle_cc, motorcycle_et_code, motorcycle_part_code);

create table if not exists moto_mktg.campaigns
(
    campaign_code varchar(20) not null,
    campaign_start_date date not null,
    campaign_name varchar(100),
    update_user varchar(30),
    update_timestamp timestamp not null,
    constraint campaigns_pk primary key (campaign_code, campaign_start_date)
);

create table if not exists moto_mktg.contacts
(
    contact_id numeric(16) not null constraint contacts_pk primary key,
    contact_type varchar(10),
    contact_type_desc varchar(100),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.camp_part_cont
(
    party_number numeric(16) not null constraint camp_part_cont_party_fk references moto_mktg.party,
    campaign_code varchar(20) not null,
    campaign_start_date date,
    contact_id numeric(16) constraint camp_part_cont_cont_fk references moto_mktg.contacts,
    marketing_program_code char(10),
    marketing_program_name char(100),
    update_user varchar(30),
    update_timestamp timestamp not null,
    constraint camp_part_cont_un unique (party_number, campaign_code, campaign_start_date, contact_id)
);

create table if not exists moto_mktg.e_mails
(
    contact_id numeric(16) not null constraint e_mails_pk primary key constraint e_mails_contacts_fk references moto_mktg.contacts,
    name varchar(100),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.phones
(
    contact_id numeric(16) not null constraint phones_pk primary key constraint phone_contacts_fk references moto_mktg.contacts,
    phone_number varchar(20),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.party_contacts
(
    party_number numeric(16) constraint party_contact_party_fk references moto_mktg.party,
    contact_id numeric(16) constraint party_contacts_contact_fk references moto_mktg.contacts,
    update_timestamp timestamp not null,
    constraint party_contacts_uk unique (party_number, contact_id)
);

create table if not exists moto_mktg.campaign_motorcycles
(
    campaign_code varchar(60),
    campaign_start_date date,
    motorcycle_id numeric(16) constraint party_motorcycles_campaign_motorcycle_fk references moto_mktg.motorcycles,
    motorcycle_class_desc varchar(40),
    motorcycle_subclass_desc varchar(256),
    motorcycle_emotion_desc varchar(256),
    motorcycle_comment varchar(1000),
    update_timestamp timestamp not null,
    constraint campaign_motorcycles_uk unique (campaign_code, campaign_start_date, motorcycle_id),
    constraint campaign_motorcycles_campaign_fk foreign key (campaign_code, campaign_start_date) references moto_mktg.campaigns
);

create table if not exists moto_mktg.channels
(
    channel_id numeric(16) not null constraint channels_pk primary key,
    channel_code varchar(50) constraint channels_uk unique,
    channel_description varchar(250),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.camp_moto_channel
(
    channel_id numeric(16) not null,
    campaign_code varchar(20),
    campaign_start_date date,
    motorcycle_name varchar,
    from_date timestamp,
    to_date timestamp,
    valid_from_date timestamp,
    valid_to_date timestamp,
    update_user varchar(30),
    update_timestamp timestamp not null,
    constraint camp_moto_channel_uk unique (channel_id, campaign_code, campaign_start_date, motorcycle_name, from_date, valid_from_date)
);

create table if not exists moto_mktg.camp_moto_chan_region
(
    channel_id numeric(16) not null,
    campaign_code varchar(20),
    campaign_start_date date,
    motorcycle_id numeric(16),
    region varchar,
    update_user varchar(30),
    update_timestamp timestamp not null,
    constraint camp_moto_chan_region_uk unique (channel_id, campaign_code, campaign_start_date, region)
);

create table if not exists moto_mktg.jrn_addresses
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    address_number numeric(16) not null,
    street_name varchar(50),
    street_number numeric(16),
    postal_code varchar(6),
    city varchar(50),
    province varchar(50),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_motorcycles
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    motorcycle_id numeric(16) not null,
    motorcycle_cc numeric(16),
    motorcycle_et_code char(10),
    motorcycle_part_code varchar(50),
    motorcycle_name varchar(100),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_channels
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    channel_id numeric(16) not null,
    channel_code varchar(50),
    channel_description varchar(250),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_camp_moto_channel
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    channel_id numeric(16) not null,
    campaign_code varchar(20),
    campaign_start_date date,
    motorcycle_name varchar,
    from_date timestamp,
    to_date timestamp,
    valid_from_date timestamp,
    valid_to_date timestamp,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_camp_moto_chan_region
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    channel_id numeric(16) not null,
    campaign_code varchar(20),
    campaign_start_date date,
    motorcycle_id numeric(16),
    region varchar,
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_party
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    party_number numeric(16) not null,
    parent_party_number numeric(16),
    name varchar(50),
    birthdate date,
    gender char(8),
    party_type_code char(2),
    comments text,
    address_number numeric(16),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_camp_part_cont
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    party_number numeric(16) not null,
    campaign_code varchar(20) not null,
    campaign_start_date date,
    contact_id numeric(16),
    marketing_program_code char(10),
    marketing_program_name char(100),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_contacts
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    contact_id numeric(16) not null,
    contact_type varchar(10),
    contact_type_desc varchar(100),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_e_mails
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    contact_id numeric(16) not null,
    name varchar(100),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_phones
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    contact_id numeric(16) not null,
    phone_number varchar(20),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_party_contacts
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    party_number numeric(16),
    contact_id numeric(16),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_campaigns
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    campaign_code varchar(20) not null,
    campaign_start_date date not null,
    campaign_name varchar(100),
    update_user varchar(30),
    update_timestamp timestamp not null
);

create table if not exists moto_mktg.jrn_campaign_motorcycles
(
    trans_id numeric(16),
    image_type varchar(20),
    operation char,
    trans_timestamp timestamp,
    campaign_code varchar(60),
    campaign_start_date date,
    motorcycle_id numeric(16),
    motorcycle_class_desc varchar(40),
    motorcycle_subclass_desc varchar(256),
    motorcycle_emotion_desc varchar(256),
    motorcycle_comment varchar(1000),
    update_timestamp timestamp not null
);
