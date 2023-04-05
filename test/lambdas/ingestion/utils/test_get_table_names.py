import sys
sys.path.append('./src/')
from ingestion.utils import get_table_names

def test_get_table_names_queries_database_for_table_names():
    result = get_table_names()
    table_names = ['address',
                      'counterparty',
                      'currency',
                      'department',
                      'design',
                      'payment_type',
                      'payment',
                      'purchase_order',
                      'sales_order',
                      'staff',
                      'transaction']
    for table_name in table_names:
        assert table_name in result
    assert len(result) == 11


