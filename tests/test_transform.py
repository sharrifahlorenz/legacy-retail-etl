import pandas as pd

from legacy import transform


def test_parse_date_formats():
    assert str(transform._parse_date("2024-03-05")) == "2024-03-05"
    assert str(transform._parse_date("03/05/2024")) == "2024-03-05"
    assert transform._parse_date("not-a-date") is None


def test_clean_orders_drops_duplicates_and_bad_dates():
    raw = pd.DataFrame([
        ["1", "1", "1", "1", "2", "10.00", "", "2024-01-01", "WEB"],
        ["1", "1", "1", "1", "2", "10.00", "", "2024-01-01", "WEB"],
        ["2", "1", "1", "1", "1", "10.00", "0.1", "not-a-date", "store"],
    ], columns=["order_id", "line_no", "customer_id", "product_id", "quantity",
                "unit_price", "discount_pct", "order_date", "channel"])
    out = transform.clean_orders(raw)
    assert len(out) == 1
    assert out.loc[0, "channel"] == "web"
    assert out.loc[0, "net_amount"] == 20.0
