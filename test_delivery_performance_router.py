"""Tests for DataDirect-safe OTD extraction and shipment attribution."""
from datetime import date
from fastapi import HTTPException
import pytest

from delivery_performance_router import (
    DeliveryRequest, attribute_delivery, build_release_query,
    build_shipper_query, make_delivery_performance_router,
)


def release(key, shipped, scheduled=10, promise="2026-02-10", line=1):
    return dict(PCN=306713, PO_Line_Key=line, Release_Key=key,
                Release_No="1", Due_Date=promise, Planned_Ship_Date=promise,
                Scheduled=scheduled, Quantity_Shipped=shipped,
                Customer_No=1, Customer_Name="Example", PO_No="PO1",
                Part_No="123", Revision="1", Part_Name="Test")


def ship(key, qty, day, shipper=1):
    return dict(Release_Key=key, Physical_Quantity=qty,
                Actual_Ship_Date=day, Shipper_Key=shipper)


def test_queries_do_not_use_ctes():
    for sql in [build_release_query("2026-01-01", "2026-04-01"),
                build_shipper_query("2026-01-01", "2026-04-01")]:
        assert "WITH ReleaseBalances" not in sql
        assert "SELECT " in sql
        assert "r.Ship_Date >=" in sql


def test_release_mean_source_rows():
    rows = [release(1, 10), release(2, 10)]
    lines = [ship(1, 10, "2026-02-10 20:00:00"),
             ship(2, 10, "2026-02-12 20:00:00")]
    report = attribute_delivery(rows, lines, date(2026, 2, 1), date(2026, 2, 28))
    assert len(report) == 2
    assert report[0]["Delivery_Performance_Raw"] == 100
    assert report[1]["Delivery_Performance_Raw"] == 0


def test_unique_edi_pair():
    rows = [release(1, 5), release(2, 5)]
    lines = [ship(1, 10, "2026-02-10 20:00:00")]
    report = attribute_delivery(rows, lines, date(2026, 2, 1), date(2026, 2, 28))
    assert [r["On_Time"] for r in report] == [5, 5]


def test_ambiguous_balance_does_not_guess():
    rows = [release(1, 10), release(2, 5), release(3, 5)]
    lines = [ship(1, 20, "2026-02-10 20:00:00")]
    report = attribute_delivery(rows, lines, date(2026, 2, 1), date(2026, 2, 28))
    assert report[1]["Unattributed_Quantity"] == 5
    assert report[2]["Unattributed_Quantity"] == 5


def test_router_rejects_invalid_window():
    router = make_delivery_performance_router(lambda: None, lambda _: [], lambda: None)
    route = next(route for route in router.routes if route.path == "/delivery-performance")
    with pytest.raises(HTTPException) as exc:
        route.endpoint(DeliveryRequest(start_date=date(2026, 9, 30),
                                       end_date=date(2026, 9, 1)))
    assert exc.value.status_code == 400
