def get_schema():
    """Returns the SQL schema to create the database tables."""
    return """
    -- Create table for session sources
    CREATE TABLE IF NOT EXISTS session_sources (
        id INTEGER PRIMARY KEY,
        session_id TEXT NOT NULL,
        source TEXT NOT NULL,
        medium TEXT NOT NULL,
        campaign TEXT NOT NULL,
        date DATE NOT NULL
    );

    -- Create table for conversions
    CREATE TABLE IF NOT EXISTS conversions (
        id INTEGER PRIMARY KEY,
        session_id TEXT NOT NULL,
        revenue REAL NOT NULL,
        date DATE NOT NULL
    );

    -- Create table for session costs
    CREATE TABLE IF NOT EXISTS session_costs (
        id INTEGER PRIMARY KEY,
        session_id TEXT NOT NULL,
        cost REAL NOT NULL,
        date DATE NOT NULL
    );

    -- Create table for attribution customer journey
    CREATE TABLE IF NOT EXISTS attribution_customer_journey (
        id INTEGER PRIMARY KEY,
        session_id TEXT NOT NULL,
        attributed_order_count INTEGER NOT NULL,
        attributed_revenue REAL NOT NULL,
        date DATE NOT NULL
    );

    -- Create table for channel reporting
    CREATE TABLE IF NOT EXISTS channel_reporting (
        id INTEGER PRIMARY KEY,
        channel TEXT NOT NULL,
        date DATE NOT NULL,
        total_orders INTEGER NOT NULL,
        total_revenue REAL NOT NULL,
        total_cost REAL NOT NULL,
        cpo REAL NOT NULL,  -- Cost per Order
        roas REAL NOT NULL  -- Return on Ad Spend
    );
    """
