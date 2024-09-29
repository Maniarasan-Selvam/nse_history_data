

-- public.stock_rolling_return_vw source

CREATE OR REPLACE VIEW public.stock_rolling_return_vw
AS SELECT stock_id,
    sum(three_year_avg) AS three_year_avg,
    sum(five_year_avg) AS five_year_avg
   FROM ( SELECT unnamed_subquery_1.stock_id,
            avg(round(((unnamed_subquery_1.new_price - unnamed_subquery_1.old_price) / unnamed_subquery_1.old_price * 100::double precision)::numeric, 2)) AS three_year_avg,
            0 AS five_year_avg
           FROM ( SELECT isdp.stock_id,
                    isdp.stock_date,
                    isdp1.stock_date AS thir_date,
                    isdp.close_price AS old_price,
                    isdp1.close_price AS new_price
                   FROM ind_stock_dly_price isdp
                     JOIN ind_stock_dly_price isdp1 ON isdp.stock_id::text = isdp1.stock_id::text AND isdp.stock_date = (isdp1.stock_date - '3 years'::interval year)::date
                  WHERE (isdp1.stock_date - '3 years'::interval year)::date >= '2000-01-01'::date) unnamed_subquery_1
          GROUP BY unnamed_subquery_1.stock_id
        UNION ALL
         SELECT unnamed_subquery_1.stock_id,
            0 AS three_year_avg,
            avg(round(((unnamed_subquery_1.new_price - unnamed_subquery_1.old_price) / unnamed_subquery_1.old_price * 100::double precision)::numeric, 2)) AS five_year_avg
           FROM ( SELECT isdp.stock_id,
                    isdp.stock_date,
                    isdp1.stock_date AS thir_date,
                    isdp.close_price AS old_price,
                    isdp1.close_price AS new_price
                   FROM ind_stock_dly_price isdp
                     JOIN ind_stock_dly_price isdp1 ON isdp.stock_id::text = isdp1.stock_id::text AND isdp.stock_date = (isdp1.stock_date - '5 years'::interval year)::date
                  WHERE (isdp1.stock_date - '5 years'::interval year)::date >= '2000-01-01'::date) unnamed_subquery_1
          GROUP BY unnamed_subquery_1.stock_id) unnamed_subquery
  GROUP BY stock_id;



  -- public.stock_cagr_return_vw source

CREATE OR REPLACE VIEW public.stock_cagr_return_vw
AS SELECT stock_id,
    start_date AS listed_date,
    total_years,
    cagr,
    total_return
   FROM ( SELECT unnamed_subquery_1.stock_id,
            unnamed_subquery_1.start_date,
            unnamed_subquery_1.ipo_price,
            unnamed_subquery_1.today_price,
            EXTRACT(year FROM age(unnamed_subquery_1.end_date::timestamp with time zone, unnamed_subquery_1.start_date::timestamp with time zone)) AS total_years,
            (power(unnamed_subquery_1.today_price / unnamed_subquery_1.ipo_price, (1.0 / EXTRACT(year FROM age(unnamed_subquery_1.end_date::timestamp with time zone, unnamed_subquery_1.start_date::timestamp with time zone)))::double precision) - 1::double precision) * 100::double precision AS cagr,
            round(((unnamed_subquery_1.today_price - unnamed_subquery_1.ipo_price) / unnamed_subquery_1.ipo_price * 100::double precision)::numeric, 2) AS total_return
           FROM ( SELECT sp.stock_id,
                    min(sp.stock_date) AS start_date,
                    max(sp.stock_date) AS end_date,
                    ( SELECT ind_stock_dly_price.close_price
                           FROM ind_stock_dly_price
                          WHERE ind_stock_dly_price.stock_id::text = sp.stock_id::text AND ind_stock_dly_price.stock_date = min(sp.stock_date)) AS ipo_price,
                    ( SELECT ind_stock_dly_price.close_price
                           FROM ind_stock_dly_price
                          WHERE ind_stock_dly_price.stock_id::text = sp.stock_id::text AND ind_stock_dly_price.stock_date = max(sp.stock_date)) AS today_price
                   FROM ind_stock_dly_price sp
                  GROUP BY sp.stock_id) unnamed_subquery_1) unnamed_subquery;


-- public.stock_returns_topn source

CREATE OR REPLACE VIEW public.stock_returns_topn
AS SELECT scrw.stock_id,
    dsl.date_of_listing,
    dsl.face_value,
    dssr.sector_id,
    dssr.sub_sector,
    scrw.total_years,
    round(scrw.cagr::numeric, 2) AS cagr,
    round(scrw.total_return, 2) AS total_return,
    round(srrv.three_year_avg, 2) AS three_rr,
    round(srrv.five_year_avg, 2) AS five_rr
   FROM stock_cagr_return_vw scrw
     JOIN stock_rolling_return_vw srrv ON scrw.stock_id::text = srrv.stock_id::text
     LEFT JOIN dim_sector_stock_raw dssr ON scrw.stock_id::text = dssr.stock_id::text
     LEFT JOIN dim_stock_list dsl ON dsl.stock_id::text = scrw.stock_id::text;
     
                     