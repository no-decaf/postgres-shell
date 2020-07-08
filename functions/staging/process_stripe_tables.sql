CREATE OR REPLACE FUNCTION staging.process_stripe_tables() RETURNS void LANGUAGE PLPGSQL AS $function$
    DECLARE

      _etl RECORD;

      -- Variables for error logging
      msg_txt text;
      detail_txt text;
      hint_txt text;

    BEGIN
      -- Start processing only if 1 instance is in progress -- this very instance.
      IF (SELECT COUNT(pid) = 1 FROM pg_stat_activity WHERE query ILIKE '%staging.process_stripe_tables%' AND query NOT ILIKE '%pg_stat_activity%') THEN

        FOR _etl IN SELECT * FROM log.etl
                    WHERE status = 'EXPORT_FINISHED'
                    AND upstream_source = 'CHARGES'
                    ORDER BY export_start
        LOOP

          UPDATE log.etl SET status = 'PROCESSING_STARTED', processing_start = (NOW() AT TIME ZONE 'UTC') WHERE id = _etl.id;

          -- Merge data from staging table to production table, and truncate staging table afterwards

          DELETE FROM stripe.balance_transaction WHERE id in (SELECT DISTINCT id FROM staging.stripe_balance_transaction);
          INSERT INTO stripe.balance_transaction SELECT DISTINCT ON (id) * FROM staging.stripe_balance_transaction;
          TRUNCATE staging.stripe_balance_transaction;

          DELETE FROM stripe.charge WHERE id in (SELECT DISTINCT id FROM staging.stripe_charge);
          INSERT INTO stripe.charge SELECT DISTINCT ON (id) * FROM staging.stripe_charge;
          TRUNCATE staging.stripe_charge;

          DELETE FROM stripe.charge_item WHERE id IN (SELECT DISTINCT id FROM staging.stripe_charge_item);
          INSERT INTO stripe.charge_item SELECT DISTINCT ON (id) * FROM staging.stripe_charge_item;

          WITH temp AS (
              SELECT id,
                max(charge_id) AS charge_id,
                max(invoice_item_id) AS invoice_item_id
              FROM staging.stripe_charge_item
              GROUP BY id
          )
          UPDATE stripe.charge_item SET charge_id = temp.charge_id, invoice_item_id = temp.invoice_item_id
            FROM temp
            WHERE stripe.charge_item.id = temp.id;

          TRUNCATE staging.stripe_charge_item;

          DELETE FROM stripe.coupon WHERE id in (SELECT DISTINCT id FROM staging.stripe_coupon);
          INSERT INTO stripe.coupon SELECT DISTINCT ON (id) * FROM staging.stripe_coupon;
          TRUNCATE staging.stripe_coupon;

          DELETE FROM stripe.customer WHERE id in (SELECT DISTINCT id FROM staging.stripe_customer);
          INSERT INTO stripe.customer SELECT DISTINCT ON (id) * FROM staging.stripe_customer;
          TRUNCATE staging.stripe_customer;

          DELETE FROM stripe.discount WHERE id in (SELECT DISTINCT id FROM staging.stripe_discount);
          INSERT INTO stripe.discount SELECT DISTINCT ON (id) * FROM staging.stripe_discount;
          TRUNCATE staging.stripe_discount;

          DELETE FROM stripe.fee_detail WHERE id in (SELECT DISTINCT id FROM staging.stripe_fee_detail);
          INSERT INTO stripe.fee_detail SELECT DISTINCT ON (id) * FROM staging.stripe_fee_detail;
          TRUNCATE staging.stripe_fee_detail;

          DELETE FROM stripe.invoice WHERE id in (SELECT DISTINCT id FROM staging.stripe_invoice);
          INSERT INTO stripe.invoice SELECT DISTINCT ON (id) * FROM staging.stripe_invoice;
          TRUNCATE staging.stripe_invoice;

          DELETE FROM stripe.invoice_item WHERE id in (SELECT DISTINCT id FROM staging.stripe_invoice_item);
          INSERT INTO stripe.invoice_item SELECT DISTINCT ON (id) * FROM staging.stripe_invoice_item;
          TRUNCATE staging.stripe_invoice_item;

          DELETE FROM stripe.payment_card WHERE id in (SELECT DISTINCT id FROM staging.stripe_payment_card);
          INSERT INTO stripe.payment_card SELECT DISTINCT ON (id) * FROM staging.stripe_payment_card;
          TRUNCATE staging.stripe_payment_card;

          DELETE FROM stripe.plan WHERE id in (SELECT DISTINCT id FROM staging.stripe_plan);
          INSERT INTO stripe.plan SELECT DISTINCT ON (id) * FROM staging.stripe_plan;
          TRUNCATE staging.stripe_plan;

          DELETE FROM stripe.refs WHERE charge_item_id IN (SELECT DISTINCT charge_item_id FROM staging.stripe_refs);
          INSERT INTO stripe.refs SELECT DISTINCT ON (charge_item_id) * FROM staging.stripe_refs;
          TRUNCATE staging.stripe_refs;

          DELETE FROM stripe.subscription WHERE id in (SELECT DISTINCT id FROM staging.stripe_subscription);
          INSERT INTO stripe.subscription SELECT DISTINCT ON (id) * FROM staging.stripe_subscription;
          TRUNCATE staging.stripe_subscription;

          DELETE FROM stripe.subscription_invoice_line_item prod
              USING staging.stripe_subscription_invoice_line_item staging
              WHERE prod.id = staging.id AND prod.invoice_id = staging.invoice_id;
          INSERT INTO stripe.subscription_invoice_line_item SELECT DISTINCT ON (id, invoice_id) * FROM staging.stripe_subscription_invoice_line_item;
          TRUNCATE staging.stripe_subscription_invoice_line_item;

          DELETE FROM stripe.transfer WHERE id in (SELECT DISTINCT id FROM staging.stripe_transfer);
          INSERT INTO stripe.transfer SELECT DISTINCT ON (id) * FROM staging.stripe_transfer;
          TRUNCATE staging.stripe_transfer;

          DELETE FROM stripe.transfer_balance_transaction t1
              USING staging.stripe_transfer_balance_transaction t2
              WHERE t1.transfer_id = t2.transfer_id AND t1.transaction_id = t2.transaction_id;
          INSERT INTO stripe.transfer_balance_transaction SELECT DISTINCT ON (transfer_id, transaction_id) * FROM staging.stripe_transfer_balance_transaction;
          TRUNCATE staging.stripe_transfer_balance_transaction;

          UPDATE stripe.balance_transaction
          SET transfer_id = stripe.transfer_balance_transaction.transfer_id
              FROM stripe.transfer_balance_transaction
              WHERE stripe.balance_transaction.id = stripe.transfer_balance_transaction.transaction_id
              AND stripe.balance_transaction.transfer_id IS NULL;

          DELETE FROM public.receipt_item WHERE id IN (SELECT DISTINCT id FROM staging.receipt_item);
          INSERT INTO public.receipt_item SELECT DISTINCT ON (id) * FROM staging.receipt_item;
          TRUNCATE staging.receipt_item;

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