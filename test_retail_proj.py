import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders

def test_read_customers_df():
    spark=get_spark_session("LOCAL")
    cust_count=read_customers(spark,"LOCAL").count()
    assert cust_count == 12435 

def test_read_orders_df():
    spark=get_spark_session("LOCAL")
    order_count=read_orders(spark,"LOCAL").count()
    assert order_count == 68883

def test_filter_closed_orders_df():
    spark=get_spark_session("LOCAL")
    orders_df=read_orders(spark,"LOCAL")
    filter_count=filter_closed_orders(orders_df).count()
    assert filter_count == 7556
    

