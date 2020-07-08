CREATE OR REPLACE FUNCTION staging.process_core_tables() RETURNS void LANGUAGE PLPGSQL AS $function$
DECLARE

      -- Temp variables
      _etl RECORD;
      car_style_processed bool;

      -- Variables for error logging
      msg_txt text;
      detail_txt text;
      hint_txt text;

    BEGIN

      -- Start processing only if 1 instance is in progress -- this very instance.
      IF (SELECT COUNT(pid) = 1 FROM pg_stat_activity WHERE query ILIKE '%staging.process_core_tables%' AND query NOT ILIKE '%pg_stat_activity%') THEN

        FOR _etl IN SELECT * FROM log.etl
                    WHERE status = 'EXPORT_FINISHED'
                    AND upstream_source IN ('CORE_TABLES',
                                            'getaround.core.models.cars.base.Car',
                                            'getaround.core.models.trips.base.Trip',
                                            'getaround.core.models.rentals.base.DeprecatedRental',
                                            'getaround.core.models.users.base.User')
                    ORDER BY export_start
        LOOP

          UPDATE log.etl SET status = 'PROCESSING_STARTED', processing_start = (NOW() AT TIME ZONE 'UTC') WHERE id = _etl.id;

          -- car tables
          IF _etl.upstream_source = 'getaround.core.models.cars.base.Car' THEN
              UPDATE staging.car SET (geom, geom_webmercator) =
                 (ST_SetSRID(ST_MakePoint(parking_address1_location_longitude, parking_address1_location_latitude), 4326),
                  ST_SetSRID(ST_MakePoint(parking_address1_location_longitude, parking_address1_location_latitude), 3857));

              DELETE FROM public.car WHERE id IN (SELECT DISTINCT id FROM staging.car);
              INSERT INTO public.car SELECT DISTINCT ON (id) * FROM staging.car;
              TRUNCATE staging.car;

              SELECT g3_cardb.process_car_style() INTO car_style_processed;
          END IF;

          -- trip table
          IF _etl.upstream_source = 'getaround.core.models.trips.base.Trip' THEN
              UPDATE staging.trip SET (geom, geom_webmercator) =
                (ST_SetSRID(ST_MakePoint(car_parking_address1_location_longitude, car_parking_address1_location_latitude), 4326),
                ST_SetSRID(ST_MakePoint(car_parking_address1_location_longitude, car_parking_address1_location_latitude), 3857));

              DELETE FROM public.receipt_item WHERE id IN (SELECT DISTINCT id FROM staging.receipt_item);
              INSERT INTO public.receipt_item SELECT DISTINCT ON (id) * FROM staging.receipt_item;
              TRUNCATE staging.receipt_item;

              DELETE FROM public.trip WHERE id IN (SELECT DISTINCT id FROM staging.trip);
              INSERT INTO public.trip SELECT DISTINCT ON (id) * FROM staging.trip;
              TRUNCATE staging.trip;

              DELETE FROM public.trip_extension_item WHERE id IN (SELECT DISTINCT id FROM staging.trip_extension_item);
              INSERT INTO public.trip_extension_item SELECT DISTINCT ON (id) * FROM staging.trip_extension_item;
              TRUNCATE staging.trip_extension_item;
          END IF;

          -- user tables
          IF _etl.upstream_source = 'getaround.core.models.users.base.User' THEN
              DELETE FROM public.user_work_history prod
                USING staging.user_work_history staging
                WHERE prod.user_facebook_id = staging.user_facebook_id
                      AND prod.employer_facebook_id = staging.employer_facebook_id
                      AND prod.position = coalesce(staging.position, '')
                      AND prod.start_date = coalesce(staging.start_date, '');
              INSERT INTO public.user_work_history
                (employer_facebook_id, employer_name, end_date, location, position, start_date, user_facebook_id)
                SELECT DISTINCT ON (user_facebook_id, employer_facebook_id, position, start_date)
                   employer_facebook_id,
                    employer_name,
                    coalesce(end_date, ''),
                    location,
                    coalesce(position, ''),
                    coalesce(start_date,''),
                    user_facebook_id FROM staging.user_work_history;
              TRUNCATE staging.user_work_history;

              DELETE FROM public.user_education_history prod
                USING staging.user_education_history staging
                WHERE prod.user_facebook_id = staging.user_facebook_id
                      AND prod.school_facebook_id = staging.school_facebook_id
                      AND prod.year = coalesce(staging.year, '');
              INSERT INTO public.user_education_history
                (school_facebook_id, school_name, type, user_facebook_id, year)
                SELECT DISTINCT ON (user_facebook_id, school_facebook_id, year)
                    school_facebook_id,
                     school_name,
                     type,
                     user_facebook_id,
                     coalesce(year, '')  FROM staging.user_education_history;
              TRUNCATE staging.user_education_history;

              DELETE FROM public.account WHERE id IN (SELECT DISTINCT id FROM staging.account);
              INSERT INTO public.account SELECT DISTINCT ON (id) * FROM staging.account;
              TRUNCATE staging.account;
          END IF;

          -- deprecated rental tables
          IF _etl.upstream_source = 'getaround.core.models.rentals.base.DeprecatedRental' THEN
              DELETE FROM public.deprecated_rental WHERE id IN (SELECT DISTINCT id FROM staging.deprecated_rental);
              INSERT INTO public.deprecated_rental SELECT DISTINCT ON (id) * FROM staging.deprecated_rental;
              TRUNCATE staging.deprecated_rental;
          END IF;


          -- logging
          UPDATE log.etl SET status = 'PROCESSING_FINISHED', processing_end = (NOW() AT TIME ZONE 'UTC') WHERE id = _etl.id;

        END LOOP;

      END IF;

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS msg_txt = MESSAGE_TEXT,
                              detail_txt = PG_EXCEPTION_DETAIL,
                              hint_txt = PG_EXCEPTION_HINT;
      INSERT INTO log.errors (error_code, error_msg, error_detail, error_hint)
                      VALUES (SQLSTATE, msg_txt, detail_txt, hint_txt);

    END;
$function$