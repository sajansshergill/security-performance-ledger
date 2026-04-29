create database if not exists SECURITY_MASTER;
create schema if not exists SECURITY_MASTER.RAW;

use database SECURITY_MASTER;
use schema RAW;

create table if not exists RAW_POSITIONS (
    _ROW_ID varchar primary key,
    BATCH_ID varchar,
    AS_OF_DATE varchar,
    PORTFOLIO_ID varchar,
    CUSIP varchar,
    ISIN varchar,
    QUANTITY varchar,
    MARKET_PRICE varchar,
    MARKET_VALUE_USD varchar,
    COST_BASIS_USD varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);

create table if not exists RAW_NAV_EVENTS (
    _ROW_ID varchar primary key,
    BATCH_ID varchar,
    AS_OF_DATE varchar,
    PORTFOLIO_ID varchar,
    NAV_BEGIN_USD varchar,
    NAV_END_USD varchar,
    EXTERNAL_FLOW_USD varchar,
    BENCHMARK_RETURN varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);
