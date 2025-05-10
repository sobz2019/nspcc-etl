import unittest
import os
import sys
from datetime import datetime
import json


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.transform import flatten_data

class TestTransform(unittest.TestCase):
    """Test cases for the transformation module"""
    
    def setUp(self):
        """Set up test data for each test case"""
        self.sample_data = [
            {
                "customer_id": "CUST001",
                "email": "test1@example.com",
                "customer_profile": {
                    "region": "London",
                    "shirt_size": "M",
                    "donates_to_charity": "Monthly",
                    "bikes_to_work": "Never"
                },
                "donations": [
                    {
                        "payment_id": "PAY001",
                        "amount": 100.50,
                        "status": "success",
                        "payment_method": "Credit Card",
                        "payment_date": "2023-01-15"
                    },
                    {
                        "payment_id": "PAY002",
                        "amount": 75.25,
                        "status": "success",
                        "payment_method": "PayPal",
                        "payment_date": "2023-02-15"
                    }
                ]
            },
            {
                "customer_id": "CUST002",
                "email": "test2@example.com",
                "customer_profile": {
                    "region": "Manchester",
                    "shirt_size": "L",
                    "donates_to_charity": "Weekly",
                    "bikes_to_work": "Often"
                },
                "donations": [
                    {
                        "payment_id": "PAY003",
                        "amount": 50.00,
                        "status": "failed",
                        "payment_method": "Debit Card",
                        "payment_date": "2023-01-20"
                    }
                ]
            }
        ]
        
        
    def test_flatten_data(self):
        """Test the flatten_data function correctly transforms raw data"""
        
        # Call the function with sample data
        fact_rows, dim_customers, dim_payments, dim_regions = flatten_data(self.sample_data)
        
        # Test the dimensions of the output data
        self.assertEqual(len(fact_rows), 3, "Should have 3 fact rows (donations)")
        self.assertEqual(len(dim_customers), 2, "Should have 2 customer records")
        self.assertEqual(len(dim_regions), 2, "Should have 2 region records")
        self.assertEqual(len(dim_payments), 3, "Should have 3 payment method records")
        
        # Test content of fact rows
        payment_ids = [row['payment_id'] for row in fact_rows]
        self.assertIn('PAY001', payment_ids)
        self.assertIn('PAY002', payment_ids)
        self.assertIn('PAY003', payment_ids)
        
        # Test customer dimension
        customer_ids = [cust['customer_id'] for cust in dim_customers]
        self.assertIn('CUST001', customer_ids)
        self.assertIn('CUST002', customer_ids)
        
        # Test payment methods
        payment_methods = [p['payment_method'] for p in dim_payments]
        self.assertIn('Credit Card', payment_methods)
        self.assertIn('PayPal', payment_methods)
        self.assertIn('Debit Card', payment_methods)
        

    def test_flatten_data_empty_input(self):
        """Test the function handles empty input correctly"""
        fact_rows, dim_customers, dim_payments, dim_regions = flatten_data([])
        
        self.assertEqual(len(fact_rows), 0)
        self.assertEqual(len(dim_customers), 0)
        self.assertEqual(len(dim_regions), 0)
        self.assertEqual(len(dim_payments), 0)

if __name__ == "__main__":
    unittest.main()