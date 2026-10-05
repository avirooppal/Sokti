-- Create Airflow database
CREATE DATABASE airflow;

-- Connect to sokti database
\c sokti;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Plans table
CREATE TABLE IF NOT EXISTS plans (
    plan_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plan_name TEXT NOT NULL,
    price_cents INT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    max_streams INT NOT NULL DEFAULT 2,
    resolution VARCHAR(10) NOT NULL DEFAULT '1080p',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Users table (PII protected at analytics layer)
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    country_code VARCHAR(5) NOT NULL DEFAULT 'US',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Profiles table
CREATE TABLE IF NOT EXISTS profiles (
    profile_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    profile_name TEXT NOT NULL,
    avatar_id TEXT,
    is_kids BOOLEAN NOT NULL DEFAULT FALSE,
    language_preference VARCHAR(10) NOT NULL DEFAULT 'en',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    plan_id UUID NOT NULL REFERENCES plans(plan_id),
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- active, past_due, canceled, expired
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    payment_method TEXT NOT NULL DEFAULT 'credit_card',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Devices table
CREATE TABLE IF NOT EXISTS devices (
    device_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    device_type VARCHAR(20) NOT NULL, -- smart_tv, mobile, tablet, web, streaming_stick
    device_name TEXT NOT NULL,
    os VARCHAR(50),
    app_version VARCHAR(20),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Watchlists table
CREATE TABLE IF NOT EXISTS watchlists (
    watchlist_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    content_id TEXT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, content_id)
);

-- Enable replica identity for Debezium CDC
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE subscriptions REPLICA IDENTITY FULL;
ALTER TABLE plans REPLICA IDENTITY FULL;
