SELECT GENERATE_SERIES(
    0,
    DATE_PART('day', (CURRENT_DATE - '2008-01-01'::TIMESTAMP))::INTEGER + 30-- iNaturalist launched in 2008
    ) as numbers