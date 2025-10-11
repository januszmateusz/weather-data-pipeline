-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data from API';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics-ready data';