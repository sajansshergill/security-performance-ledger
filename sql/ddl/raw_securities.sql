create database if not exists SECURITY_MASTER;
create schema if not exists SECURITY_MASTER.RAW;

use database SECURITY_MASTER;
use schema RAW;

create table if not exists RAW_SECURITIES_OPENFIGI (
    _ROW_ID varchar primary key,
    BATCH_ID varchar,
    CUSIP varchar,
    ISIN varchar,
    FIGI varchar,
    COMPOSITE_FIGI varchar,
    TICKER varchar,
    SECURITY_NAME varchar,
    SECURITY_TYPE varchar,
    MARKET_SECTOR varchar,
    CURRENCY varchar,
    EXCHANGE_CODE varchar,
    SOURCE varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);

create table if not exists RAW_SECURITIES_REFINITIV (
    _ROW_ID varchar primary key,
    BATCH_ID varchar,
    CUSIP varchar,
    ISIN varchar,
    TICKER varchar,
    SECURITY_NAME varchar,
    SECURITY_TYPE varchar,
    MARKET_SECTOR varchar,
    CURRENCY varchar,
    EXCHANGE_CODE varchar,
    GICS_SECTOR varchar,
    GICS_INDUSTRY varchar,
    COUNTRY varchar,
    ASSET_CATEGORY varchar,
    SOURCE varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);

create table if not exists RAW_SECURITIES_BLOOMBERG (
    _ROW_ID varchar primary key,
    BATCH_ID varchar,
    CUSIP varchar,
    ISIN varchar,
    FIGI varchar,
    COMPOSITE_FIGI varchar,
    TICKER varchar,
    SECURITY_NAME varchar,
    SECURITY_TYPE varchar,
    MARKET_SECTOR varchar,
    CURRENCY varchar,
    EXCHANGE_CODE varchar,
    GICS_SECTOR varchar,
    GICS_INDUSTRY varchar,
    COUNTRY varchar,
    ASSET_CATEGORY varchar,
    SOURCE varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);

create table if not exists INT_SECURITY_MASTER_STAGING (
    MASTER_ID varchar primary key,
    CUSIP varchar,
    ISIN varchar,
    FIGI varchar,
    COMPOSITE_FIGI varchar,
    TICKER varchar,
    SECURITY_NAME varchar,
    SECURITY_TYPE varchar,
    MARKET_SECTOR varchar,
    CURRENCY varchar,
    EXCHANGE_CODE varchar,
    GICS_SECTOR varchar,
    GICS_INDUSTRY varchar,
    COUNTRY varchar,
    ASSET_CATEGORY varchar,
    SOURCE_OF_TRUTH varchar,
    SOURCES_SEEN varchar,
    CONFLICT_FLAGS varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);

create table if not exists RAW_IDENTIFIER_CONFLICTS (
    CONFLICT_ID varchar primary key,
    BATCH_ID varchar,
    MASTER_ID varchar,
    CUSIP varchar,
    ISIN varchar,
    FIELD_NAME varchar,
    VALUE_WINNER varchar,
    SOURCE_WINNER varchar,
    VALUE_LOSER varchar,
    SOURCE_LOSER varchar,
    _LOADED_AT timestamp_ntz default current_timestamp()
);
