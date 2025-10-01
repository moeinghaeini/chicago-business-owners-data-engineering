-- Chicago Business Demographics Data Warehouse Schema
-- PostgreSQL Database Schema for Big Data ETL Pipeline

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- =============================================
-- STAGING LAYER TABLES
-- =============================================

-- Raw data staging table
CREATE TABLE IF NOT EXISTS staging_business_owners (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_number INTEGER,
    legal_name VARCHAR(500),
    owner_first_name VARCHAR(100),
    owner_middle_initial VARCHAR(10),
    owner_last_name VARCHAR(100),
    suffix VARCHAR(20),
    legal_entity_owner VARCHAR(500),
    title VARCHAR(100),
    raw_data JSONB,
    file_name VARCHAR(255),
    batch_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
);

-- Data quality staging
CREATE TABLE IF NOT EXISTS staging_data_quality (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id VARCHAR(100),
    table_name VARCHAR(100),
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    completeness_score DECIMAL(5,2),
    quality_metrics JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- Business dimension
CREATE TABLE IF NOT EXISTS dim_business (
    business_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_number INTEGER UNIQUE NOT NULL,
    legal_name VARCHAR(500) NOT NULL,
    business_type VARCHAR(50),
    business_size_category VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Owner dimension
CREATE TABLE IF NOT EXISTS dim_owner (
    owner_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    full_name VARCHAR(255),
    first_name VARCHAR(100),
    middle_initial VARCHAR(10),
    last_name VARCHAR(100),
    suffix VARCHAR(20),
    is_individual BOOLEAN,
    legal_entity_name VARCHAR(500),
    owner_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Role dimension
CREATE TABLE IF NOT EXISTS dim_role (
    role_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(100) NOT NULL,
    role_category VARCHAR(50),
    is_leadership BOOLEAN DEFAULT FALSE,
    is_ownership BOOLEAN DEFAULT FALSE,
    hierarchy_level INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Geographic dimension (for future expansion)
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    business_id UUID REFERENCES dim_business(business_id),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- FACT TABLES
-- =============================================

-- Main business ownership fact table
CREATE TABLE IF NOT EXISTS fact_business_ownership (
    ownership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    business_id UUID REFERENCES dim_business(business_id),
    owner_id UUID REFERENCES dim_owner(owner_id),
    role_id UUID REFERENCES dim_role(role_id),
    date_id DATE REFERENCES dim_date(date_id),
    ownership_percentage DECIMAL(5,2),
    is_primary_owner BOOLEAN DEFAULT FALSE,
    ownership_start_date DATE,
    ownership_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Business metrics fact table
CREATE TABLE IF NOT EXISTS fact_business_metrics (
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    business_id UUID REFERENCES dim_business(business_id),
    date_id DATE REFERENCES dim_date(date_id),
    total_owners INTEGER,
    individual_owners INTEGER,
    corporate_owners INTEGER,
    leadership_roles INTEGER,
    ownership_roles INTEGER,
    diversity_score DECIMAL(5,2),
    complexity_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Owner demographics fact table
CREATE TABLE IF NOT EXISTS fact_owner_demographics (
    demo_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_id UUID REFERENCES dim_owner(owner_id),
    date_id DATE REFERENCES dim_date(date_id),
    name_length INTEGER,
    name_complexity_score DECIMAL(5,2),
    is_unique_name BOOLEAN,
    name_frequency_rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- AGGREGATED TABLES
-- =============================================

-- Daily business aggregations
CREATE TABLE IF NOT EXISTS agg_daily_business (
    aggregation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date_id DATE REFERENCES dim_date(date_id),
    total_businesses INTEGER,
    new_businesses INTEGER,
    multi_owner_businesses INTEGER,
    single_owner_businesses INTEGER,
    avg_owners_per_business DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily owner aggregations
CREATE TABLE IF NOT EXISTS agg_daily_owners (
    aggregation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date_id DATE REFERENCES dim_date(date_id),
    total_owners INTEGER,
    individual_owners INTEGER,
    corporate_owners INTEGER,
    unique_owners INTEGER,
    most_common_role VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Role distribution aggregations
CREATE TABLE IF NOT EXISTS agg_role_distribution (
    aggregation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date_id DATE REFERENCES dim_date(date_id),
    role_id UUID REFERENCES dim_role(role_id),
    role_count INTEGER,
    percentage_of_total DECIMAL(5,2),
    growth_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- INDEXES FOR PERFORMANCE
-- =============================================

-- Staging table indexes
CREATE INDEX IF NOT EXISTS idx_staging_business_owners_batch_id ON staging_business_owners(batch_id);
CREATE INDEX IF NOT EXISTS idx_staging_business_owners_status ON staging_business_owners(status);
CREATE INDEX IF NOT EXISTS idx_staging_business_owners_account_number ON staging_business_owners(account_number);

-- Dimension table indexes
CREATE INDEX IF NOT EXISTS idx_dim_business_account_number ON dim_business(account_number);
CREATE INDEX IF NOT EXISTS idx_dim_owner_full_name ON dim_owner(full_name);
CREATE INDEX IF NOT EXISTS idx_dim_role_title ON dim_role(title);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_ownership_business_id ON fact_business_ownership(business_id);
CREATE INDEX IF NOT EXISTS idx_fact_ownership_owner_id ON fact_business_ownership(owner_id);
CREATE INDEX IF NOT EXISTS idx_fact_ownership_date_id ON fact_business_ownership(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_ownership_is_current ON fact_business_ownership(is_current);

CREATE INDEX IF NOT EXISTS idx_fact_metrics_business_id ON fact_business_metrics(business_id);
CREATE INDEX IF NOT EXISTS idx_fact_metrics_date_id ON fact_business_metrics(date_id);

CREATE INDEX IF NOT EXISTS idx_fact_demo_owner_id ON fact_owner_demographics(owner_id);
CREATE INDEX IF NOT EXISTS idx_fact_demo_date_id ON fact_owner_demographics(date_id);

-- =============================================
-- VIEWS FOR ANALYTICS
-- =============================================

-- Business ownership summary view
CREATE OR REPLACE VIEW v_business_ownership_summary AS
SELECT 
    b.account_number,
    b.legal_name,
    b.business_type,
    COUNT(DISTINCT o.owner_id) as total_owners,
    COUNT(DISTINCT CASE WHEN o.is_individual THEN o.owner_id END) as individual_owners,
    COUNT(DISTINCT CASE WHEN NOT o.is_individual THEN o.owner_id END) as corporate_owners,
    COUNT(DISTINCT CASE WHEN r.is_leadership THEN o.owner_id END) as leadership_owners,
    MAX(fbo.created_at) as last_updated
FROM dim_business b
LEFT JOIN fact_business_ownership fbo ON b.business_id = fbo.business_id
LEFT JOIN dim_owner o ON fbo.owner_id = o.owner_id
LEFT JOIN dim_role r ON fbo.role_id = r.role_id
WHERE fbo.is_current = TRUE
GROUP BY b.business_id, b.account_number, b.legal_name, b.business_type;

-- Owner demographics view
CREATE OR REPLACE VIEW v_owner_demographics AS
SELECT 
    o.owner_id,
    o.full_name,
    o.first_name,
    o.last_name,
    o.is_individual,
    o.owner_type,
    COUNT(DISTINCT fbo.business_id) as businesses_owned,
    COUNT(DISTINCT fbo.role_id) as unique_roles,
    MAX(fod.name_length) as name_length,
    MAX(fod.name_complexity_score) as complexity_score
FROM dim_owner o
LEFT JOIN fact_business_ownership fbo ON o.owner_id = fbo.owner_id
LEFT JOIN fact_owner_demographics fod ON o.owner_id = fod.owner_id
WHERE fbo.is_current = TRUE
GROUP BY o.owner_id, o.full_name, o.first_name, o.last_name, o.is_individual, o.owner_type;

-- Role distribution view
CREATE OR REPLACE VIEW v_role_distribution AS
SELECT 
    r.title,
    r.role_category,
    r.is_leadership,
    r.is_ownership,
    COUNT(DISTINCT fbo.owner_id) as total_owners,
    COUNT(DISTINCT fbo.business_id) as total_businesses,
    ROUND(COUNT(DISTINCT fbo.owner_id) * 100.0 / SUM(COUNT(DISTINCT fbo.owner_id)) OVER (), 2) as percentage
FROM dim_role r
LEFT JOIN fact_business_ownership fbo ON r.role_id = fbo.role_id
WHERE fbo.is_current = TRUE
GROUP BY r.role_id, r.title, r.role_category, r.is_leadership, r.is_ownership;

-- =============================================
-- FUNCTIONS AND PROCEDURES
-- =============================================

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_date (
            date_id, year, quarter, month, day, day_of_week,
            day_name, month_name, is_weekend, is_holiday,
            fiscal_year, fiscal_quarter
        ) VALUES (
            current_date,
            EXTRACT(YEAR FROM current_date),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(MONTH FROM current_date),
            EXTRACT(DAY FROM current_date),
            EXTRACT(DOW FROM current_date),
            TO_CHAR(current_date, 'Day'),
            TO_CHAR(current_date, 'Month'),
            EXTRACT(DOW FROM current_date) IN (0, 6),
            FALSE, -- Holiday logic would be implemented here
            EXTRACT(YEAR FROM current_date),
            EXTRACT(QUARTER FROM current_date)
        ) ON CONFLICT (date_id) DO NOTHING;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate business metrics
CREATE OR REPLACE FUNCTION calculate_business_metrics(target_date DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO fact_business_metrics (
        business_id, date_id, total_owners, individual_owners,
        corporate_owners, leadership_roles, ownership_roles,
        diversity_score, complexity_score
    )
    SELECT 
        b.business_id,
        target_date,
        COUNT(DISTINCT fbo.owner_id),
        COUNT(DISTINCT CASE WHEN o.is_individual THEN fbo.owner_id END),
        COUNT(DISTINCT CASE WHEN NOT o.is_individual THEN fbo.owner_id END),
        COUNT(DISTINCT CASE WHEN r.is_leadership THEN fbo.owner_id END),
        COUNT(DISTINCT CASE WHEN r.is_ownership THEN fbo.owner_id END),
        CASE 
            WHEN COUNT(DISTINCT fbo.owner_id) > 1 
            THEN ROUND(COUNT(DISTINCT fbo.owner_id) * 1.0 / COUNT(DISTINCT fbo.owner_id), 2)
            ELSE 0
        END,
        CASE 
            WHEN COUNT(DISTINCT fbo.owner_id) > 1 
            THEN ROUND(LOG(COUNT(DISTINCT fbo.owner_id)), 2)
            ELSE 0
        END
    FROM dim_business b
    LEFT JOIN fact_business_ownership fbo ON b.business_id = fbo.business_id
    LEFT JOIN dim_owner o ON fbo.owner_id = o.owner_id
    LEFT JOIN dim_role r ON fbo.role_id = r.role_id
    WHERE fbo.is_current = TRUE
    GROUP BY b.business_id
    ON CONFLICT (business_id, date_id) DO UPDATE SET
        total_owners = EXCLUDED.total_owners,
        individual_owners = EXCLUDED.individual_owners,
        corporate_owners = EXCLUDED.corporate_owners,
        leadership_roles = EXCLUDED.leadership_roles,
        ownership_roles = EXCLUDED.ownership_roles,
        diversity_score = EXCLUDED.diversity_score,
        complexity_score = EXCLUDED.complexity_score;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- TRIGGERS FOR AUDIT
-- =============================================

-- Audit trigger function
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply audit triggers
CREATE TRIGGER trg_dim_business_audit
    BEFORE UPDATE ON dim_business
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER trg_dim_owner_audit
    BEFORE UPDATE ON dim_owner
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

CREATE TRIGGER trg_fact_ownership_audit
    BEFORE UPDATE ON fact_business_ownership
    FOR EACH ROW EXECUTE FUNCTION audit_trigger_function();

-- =============================================
-- INITIAL DATA POPULATION
-- =============================================

-- Populate date dimension for the next 5 years
SELECT populate_date_dimension(CURRENT_DATE, CURRENT_DATE + INTERVAL '5 years');

-- Insert default roles
INSERT INTO dim_role (title, role_category, is_leadership, is_ownership, hierarchy_level) VALUES
('CEO', 'Executive', TRUE, TRUE, 1),
('PRESIDENT', 'Executive', TRUE, TRUE, 1),
('MANAGING MEMBER', 'Management', TRUE, TRUE, 2),
('MANAGER', 'Management', TRUE, FALSE, 3),
('DIRECTOR', 'Management', TRUE, FALSE, 2),
('OWNER', 'Ownership', FALSE, TRUE, 1),
('SHAREHOLDER', 'Ownership', FALSE, TRUE, 2),
('PARTNER', 'Ownership', FALSE, TRUE, 2),
('MEMBER', 'Ownership', FALSE, TRUE, 3),
('OTHER', 'Other', FALSE, FALSE, 4)
ON CONFLICT DO NOTHING;
