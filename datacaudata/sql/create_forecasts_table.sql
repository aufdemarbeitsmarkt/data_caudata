CREATE TABLE public.forecasts
(
    ds TIMESTAMP,
    unique_id INTEGER,
    auto_arima DOUBLE PRECISION,
    auto_arima_lo_90 DOUBLE PRECISION,
    auto_arima_hi_90 DOUBLE PRECISION,
    forecast_type TEXT,
    _date_forecasted DATE
);

ALTER TABLE IF EXISTS public.forecasts
    OWNER to postgres;
