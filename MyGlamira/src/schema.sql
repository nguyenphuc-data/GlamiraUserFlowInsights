CREATE TABLE IF NOT EXISTS currency_totals (
    product_id BIGINT PRIMARY KEY,
    total_MXN_USD DECIMAL(18, 2),
    total_Ft DECIMAL(18, 2),
    total_PLN DECIMAL(18, 2),
    total_CLP DECIMAL(18, 2),
    total_SGD_USD DECIMAL(18, 2),
    total_AU_USD DECIMAL(18, 2),
    total_HKD_USD DECIMAL(18, 2),
    total_NZD_USD DECIMAL(18, 2),
    total_USD DECIMAL(18, 2),
    total_GBP DECIMAL(18, 2),
    total_CAD_USD DECIMAL(18, 2),
    total_JPY DECIMAL(18, 2),
    total_EUR DECIMAL(18, 2)
);