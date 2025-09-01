from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveOrdersWithRxNorm(BaseModel):
    """Model for Active Orders with RxNorm data from incoming_*_ActiveORDERSWithRXNorm.csv"""
    facility_id: Optional[str]
    unit: Optional[str]
    npi_number_of_ordering_practitioner: Optional[str]
    name_of_ordering_provider: Optional[str]
    date_time_order_received: Optional[str]
    ndc_code_of_drug_ordered: Optional[str]
    drug_name: Optional[str]
    effective_date: Optional[str]
    dosage: Optional[str]
    drug_strength: Optional[str]
    frequency: Optional[str]
    duration: Optional[str]
    route: Optional[str]
    number_of_refills_issued: Optional[str]
    expiration_date: Optional[str]
    provider_admin_instructions: Optional[str]
    dispensed_quantity: Optional[str]
    formulary_or_non_formulary_drug_flag: Optional[str]
    keep_on_person_indicator: Optional[str]
    medication_time_of_day: Optional[str]
    order_id: Optional[str]
    ordered_by: Optional[str]
    file_name: Optional[str]