# Transform data for loading into the data warehouse
from utils.logger import setup_logger

# Set up logger for this module
logger = setup_logger('transform')

def flatten_data(raw_data):
    """
    Flatten nested JSON data into dimensional and fact tables
    
    Args:
        raw_data: List of raw data records
    
    Returns:
        Tuple containing (fact_rows, dim_customers, dim_payments, dim_regions)
    """
    logger.info(f"Starting data transformation of {len(raw_data)} records")
    fact_rows, dim_customers, dim_regions, dim_payments = [], [], [], []

    for entry in raw_data:
        cust_id = entry.get("customer_id")
        profile = entry.get("customer_profile", {})
        region = profile.get("region", "Unknown")

        dim_customers.append({
            "customer_id": cust_id,
            "email": entry.get("email"),
            "shirt_size": profile.get("shirt_size"),
            "donates_to_charity": profile.get("donates_to_charity"),
            "bikes_to_work": profile.get("bikes_to_work")
        })
        
        is_london = "london" in region.strip().lower()
        dim_regions.append({
            "region": region,
            "is_london": is_london
        })

        donations = entry.get("donations") or []
        for d in donations:
            payment_date = d.get("payment_date")
            payment_method = d.get("payment_method")
            dim_payments.append({"payment_method": payment_method})

            fact_rows.append({
                "payment_id": d.get("payment_id"),
                "customer_id": cust_id,
                "amount": d.get("amount"),
                "status": d.get("status"),
                "payment_method": payment_method,
                "payment_date": payment_date,
                "region": region
            })

    logger.info(f"Transformation complete: {len(fact_rows)} fact rows, "
               f"{len(dim_customers)} customers, "
               f"{len(set(r['region'] for r in dim_regions))} regions, "
               f"{len(set(p['payment_method'] for p in dim_payments))} payment methods")
    
    # Remove duplicates from dimension tables
    unique_regions = {r['region']: r for r in dim_regions}.values()
    unique_payments = {p['payment_method']: p for p in dim_payments}.values()
    
    return fact_rows, dim_customers, list(unique_payments), list(unique_regions)